# -*- coding: utf-8 -*-

import pickle

from pydal import DAL, Field
from pydal import ast
from pydal.ast_translate import to_ast
from pydal.objects import Expression

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "AST translator targets the SQL DSL surface")
class TestAstTranslate(unittest.TestCase):
    """Layer 2a: verify Expression/Query/Field -> ast.Node mapping.

    These tests don't run any SQL — they only assert the shape of the
    translated AST, so they're cheap and dialect-independent.
    """

    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table(
            "t",
            Field("name"),
            Field("age", "integer"),
            Field("data", "json"),
        )

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    # ----- leaves -----

    def test_field_to_fieldref(self):
        n = to_ast(self.db.t.name)
        # logical identifiers are stable; sqlsafe is pre-formatted for
        # the compiler and not part of the value comparison here.
        self.assertIsInstance(n, ast.FieldRef)
        self.assertEqual((n.table, n.name), ("t", "name"))

    def test_literal_passthrough(self):
        n = to_ast(5, type_hint="integer")
        self.assertEqual(n, ast.Literal(5, "integer"))

    def test_raw_string_op_is_raw(self):
        # The translator bakes the original _expand shape ("(SQL)" with
        # trailing ";" stripped) into the Raw node so the compiler can
        # emit it verbatim.
        e = Expression(self.db, "SOME SQL")
        self.assertEqual(to_ast(e), ast.Raw("(SOME SQL)"))
        e2 = Expression(self.db, "SOME SQL;")
        self.assertEqual(to_ast(e2), ast.Raw("(SOME SQL)"))

    # ----- comparisons + null fold-in -----

    def test_eq_yields_binop(self):
        n = to_ast(self.db.t.name == "x")
        self.assertEqual(n.op, "eq")
        self.assertEqual((n.left.table, n.left.name), ("t", "name"))
        self.assertEqual(n.right, ast.Literal("x", "string"))

    def test_eq_none_collapses_to_is_null(self):
        n = to_ast(self.db.t.name == None)  # noqa: E711
        self.assertEqual(n.op, "is_null")
        self.assertEqual((n.operand.table, n.operand.name), ("t", "name"))

    def test_ne_none_collapses_to_is_not_null(self):
        n = to_ast(self.db.t.name != None)  # noqa: E711
        self.assertEqual(n.op, "is_not_null")
        self.assertEqual((n.operand.table, n.operand.name), ("t", "name"))

    def test_lt_gt(self):
        self.assertEqual(to_ast(self.db.t.age < 1).op, "lt")
        self.assertEqual(to_ast(self.db.t.age > 1).op, "gt")
        self.assertEqual(to_ast(self.db.t.age <= 1).op, "lte")
        self.assertEqual(to_ast(self.db.t.age >= 1).op, "gte")

    # ----- logical -----

    def test_and_or_not(self):
        a = self.db.t.age > 0
        b = self.db.t.age < 100
        self.assertEqual(to_ast(a & b).op, "and")
        self.assertEqual(to_ast(a | b).op, "or")
        self.assertEqual(to_ast(~a).op, "not")

    # ----- string-match opts -----

    def test_like_carries_escape_opt(self):
        n = to_ast(self.db.t.name.like("a%", escape="!"))
        self.assertEqual(n.op, "like")
        self.assertIn(("escape", "!"), n.opts)

    def test_ilike_default_escape_carried_as_none(self):
        # pydal's like/ilike always pass escape=escape; None means
        # "use the dialect default". We faithfully carry that in opts
        # so the compiler can apply its own default.
        n = to_ast(self.db.t.name.ilike("a%"))
        self.assertEqual(dict(n.opts), {"escape": None})

    def test_regexp_carries_match_parameter(self):
        n = to_ast(self.db.t.name.regexp("a+", match_parameter="i"))
        self.assertIn(("match_parameter", "i"), n.opts)

    def test_contains_carries_case_sensitive(self):
        n = to_ast(self.db.t.name.contains("x", case_sensitive=True))
        self.assertIn(("case_sensitive", True), n.opts)

    def test_startswith(self):
        n = to_ast(self.db.t.name.startswith("foo"))
        self.assertEqual(n.op, "startswith")

    # ----- belongs -----

    def test_belongs_list_becomes_inlist(self):
        n = to_ast(self.db.t.age.belongs([1, 2, 3]))
        self.assertIsInstance(n, ast.InList)
        self.assertEqual(len(n.values), 3)
        self.assertTrue(all(isinstance(v, ast.Literal) for v in n.values))
        # type hint propagates from the left field
        self.assertTrue(all(v.type == "integer" for v in n.values))

    # ----- arithmetic + comma -----

    def test_arithmetic_and_comma(self):
        self.assertEqual(to_ast(self.db.t.age + 1).op, "add")
        self.assertEqual(to_ast(self.db.t.age * 2).op, "mul")
        self.assertEqual(to_ast(self.db.t.age | self.db.t.name).op, "comma")

    # ----- unary expression functions -----

    def test_unary_str_funcs(self):
        for builder, expected in [
            (self.db.t.name.lower, "lower"),
            (self.db.t.name.upper, "upper"),
            (self.db.t.name.len, "length"),
        ]:
            n = to_ast(builder())
            self.assertEqual(n.op, expected)
            self.assertEqual((n.operand.table, n.operand.name), ("t", "name"))

    def test_invert_for_desc_order(self):
        n = to_ast(~self.db.t.age)
        self.assertEqual(n.op, "invert")
        self.assertEqual((n.operand.table, n.operand.name), ("t", "age"))

    # ----- multi-arg / structured functions -----

    def test_aggregate_kind_in_opts(self):
        for builder, kind in [
            (self.db.t.age.sum, "SUM"),
            (self.db.t.age.max, "MAX"),
            (self.db.t.age.min, "MIN"),
            (self.db.t.age.avg, "AVG"),
            (self.db.t.age.abs, "ABS"),
        ]:
            n = to_ast(builder())
            self.assertEqual(n.name, "aggregate")
            self.assertEqual(dict(n.opts).get("kind"), kind)

    def test_cast_carries_destination(self):
        n = to_ast(self.db.t.age.cast("double"))
        self.assertEqual(n.name, "cast")
        self.assertIn("to", dict(n.opts))

    def test_extract_unit_in_opts(self):
        n = to_ast(self.db.t.age.year())
        self.assertEqual(n.name, "extract")
        self.assertEqual(dict(n.opts), {"unit": "year"})

    def test_replace_three_args(self):
        n = to_ast(self.db.t.name.replace("a", "b"))
        self.assertEqual(n.name, "replace")
        self.assertEqual(len(n.args), 3)

    def test_coalesce_flattens(self):
        n = to_ast(self.db.t.age.coalesce(0, 1))
        self.assertEqual(n.name, "coalesce")
        self.assertEqual(len(n.args), 3)

    def test_substring_three_args(self):
        n = to_ast(self.db.t.name[1:3])
        self.assertEqual(n.name, "substring")
        self.assertEqual(len(n.args), 3)

    def test_case_three_args(self):
        n = to_ast((self.db.t.age > 0).case(1, 0))
        self.assertEqual(n.name, "case")
        self.assertEqual(len(n.args), 3)

    # ----- alias -----

    def test_with_alias_to_aliased(self):
        n = to_ast(self.db.t.age.with_alias("a"))
        self.assertIsInstance(n, ast.Aliased)
        self.assertEqual(n.alias, "a")

    # ----- json / GIS spot checks -----

    def test_json_accessors(self):
        # SQLite's dialect has no json_key; the JSON accessors live in
        # PostgresDialectJSON. Swap it in to exercise the translator —
        # only possible because the late-bound DialectOp from layer 1
        # makes dialects swappable at runtime.
        from pydal.backends.postgres import PostgresDialectJSON

        original = self.db._adapter.dialect
        self.db._adapter.dialect = PostgresDialectJSON(self.db._adapter)
        try:
            for builder, op in [
                (self.db.t.data.json_key, "json_key"),
                (self.db.t.data.json_key_value, "json_key_value"),
            ]:
                n = to_ast(builder("k"))
                self.assertEqual(n.op, op)
        finally:
            self.db._adapter.dialect = original

    # ----- AST invariants on a representative tree -----

    def test_translated_ast_is_hashable_and_picklable(self):
        q = (
            (self.db.t.age > 0)
            & self.db.t.name.like("a%", escape="!")
            & self.db.t.age.belongs([1, 2, 3])
        )
        n = to_ast(q)
        hash(n)
        n2 = pickle.loads(pickle.dumps(n))
        self.assertEqual(n, n2)


@unittest.skipIf(IS_NOSQL, "AST translator targets the SQL DSL surface")
class TestAstTranslateSubqueries(unittest.TestCase):
    """Subquery surface — nested_select bridges onto the AST cleanly,
    subselect produces the AST natively.
    """

    def test_belongs_with_nested_select_translates(self):
        db = DAL("sqlite:memory")
        db.define_table("t", Field("name"))
        try:
            sub_obj = db._adapter.nested_select(
                db.t.name == "x", [db.t.id], {}
            )
            q = db.t.id.belongs(sub_obj)
            n = to_ast(q)
            # InList with a single Select child.
            self.assertIsInstance(n, ast.InList)
            self.assertEqual(len(n.values), 1)
            self.assertIsInstance(n.values[0], ast.Select)
        finally:
            db.close()
