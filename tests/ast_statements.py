# -*- coding: utf-8 -*-

"""Layer-2c oracle: AST-based statement compilers vs the current path.

For each of the five entry points (_select, _insert, _update, _delete,
_count) we assert that ``compile_*(translator(...))`` produces the same
SQL string as the current pydal pipeline. Scope is intentionally
single-table (no joins, CTE, subqueries) — those land in the next
sub-layer.
"""

from pydal import DAL, Field
from pydal.ast_translate import (
    set_to_count,
    set_to_delete,
    set_to_select,
    set_to_update,
    table_to_insert,
)
from pydal.compilers import SQLiteCompiler

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "AST statement compilers are SQL-only")
class TestAstStatementsMatchesCurrentPath(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table(
            "t",
            Field("name"),
            Field("age", "integer"),
            Field("data", "json"),
        )
        cls.compiler = SQLiteCompiler(
            represent=cls.db._adapter.represent, parameterize=False
        )
        # The wired adapter compiler defaults to parameterize=True on
        # SQLite; force inline so the oracle compares like-for-like.
        cls._restore_param = cls.db._adapter.compiler.parameterize
        cls.db._adapter.compiler.parameterize = False

    @classmethod
    def tearDownClass(cls):
        cls.db._adapter.compiler.parameterize = cls._restore_param
        cls.db.close()

    # ---------- helpers ----------

    def _check_select(self, s, fields=(), **attrs):
        current = s._select(*fields, **attrs)
        node = set_to_select(s, fields, attrs)
        compiled = self.compiler.compile_select(node)
        self.assertEqual(compiled, current)

    def _check_count(self, s, distinct=None):
        current = s._count(distinct=distinct)
        node = set_to_count(s, distinct=distinct)
        compiled = self.compiler.compile_count(node)
        self.assertEqual(compiled, current)

    def _check_update(self, s, **fields):
        current = s._update(**fields)
        row = self.db.t._fields_and_values_for_update(fields)
        node = set_to_update(s, row.op_values())
        compiled = self.compiler.compile_update(node)
        self.assertEqual(compiled, current)

    def _check_delete(self, s):
        current = s._delete()
        node = set_to_delete(s)
        compiled = self.compiler.compile_delete(node)
        self.assertEqual(compiled, current)

    def _check_insert(self, **fields):
        current = self.db.t._insert(**fields)
        row = self.db.t._fields_and_values_for_insert(fields)
        node = table_to_insert(self.db.t, row.op_values())
        compiled = self.compiler.compile_insert(node)
        self.assertEqual(compiled, current)

    # ================================================================== SELECT

    def test_select_default_fields(self):
        # No fields given -> auto-expand to all table columns.
        self._check_select(self.db(self.db.t.id > 0))

    def test_select_specific_fields(self):
        self._check_select(self.db(self.db.t.age > 5), (self.db.t.id, self.db.t.name))

    def test_select_with_orderby(self):
        self._check_select(
            self.db(self.db.t.id > 0),
            (self.db.t.id,),
            orderby=self.db.t.id,
        )

    def test_select_with_orderby_desc(self):
        self._check_select(
            self.db(self.db.t.id > 0),
            (self.db.t.id,),
            orderby=~self.db.t.id,
        )

    def test_select_with_orderby_list(self):
        self._check_select(
            self.db(self.db.t.id > 0),
            (self.db.t.id,),
            orderby=[self.db.t.age, self.db.t.name],
        )

    def test_select_with_groupby(self):
        self._check_select(
            self.db(self.db.t.id > 0),
            (self.db.t.age, self.db.t.age.sum()),
            groupby=self.db.t.age,
        )

    def test_select_with_having(self):
        self._check_select(
            self.db(self.db.t.id > 0),
            (self.db.t.age, self.db.t.age.sum().with_alias("s")),
            groupby=self.db.t.age,
            having=self.db.t.age.sum() > 0,
        )

    def test_select_distinct_true(self):
        self._check_select(self.db(self.db.t.id > 0), (self.db.t.name,), distinct=True)

    def test_select_limit_default_orderby(self):
        # No orderby + limitby triggers default PK ordering.
        self._check_select(self.db(self.db.t.id > 0), (self.db.t.id,), limitby=(0, 10))

    def test_select_limit_explicit_orderby(self):
        self._check_select(
            self.db(self.db.t.id > 0),
            (self.db.t.id,),
            orderby=self.db.t.age,
            limitby=(5, 25),
        )

    def test_select_for_update(self):
        self._check_select(self.db(self.db.t.id > 0), (self.db.t.id,), for_update=True)

    def test_select_no_query(self):
        # db(db.t) becomes id_query: db.t._id != None
        self._check_select(self.db(self.db.t), (self.db.t.id,))

    # ================================================================== COUNT

    def test_count_basic(self):
        self._check_count(self.db(self.db.t.id > 0))

    def test_count_no_query(self):
        self._check_count(self.db(self.db.t))

    def test_count_distinct_field(self):
        self._check_count(self.db(self.db.t.id > 0), distinct=self.db.t.age)

    # ================================================================== UPDATE

    def test_update_single_field(self):
        self._check_update(self.db(self.db.t.id > 0), name="bob")

    def test_update_multi_field(self):
        self._check_update(self.db(self.db.t.age > 5), name="bob", age=99)

    # ================================================================== DELETE

    def test_delete_with_query(self):
        self._check_delete(self.db(self.db.t.age < 0))

    def test_delete_no_query(self):
        self._check_delete(self.db(self.db.t))

    # ================================================================== INSERT

    def test_insert_basic(self):
        self._check_insert(name="alice", age=30)

    def test_insert_default_values_only(self):
        # field with default ("name" has no default; age has no default)
        # so this just inserts with whatever is given
        self._check_insert(name="alice")

    # ================================================================== COMMON FILTERS

    def test_common_filter_applied_select(self):
        # Add a common filter, verify both paths apply it identically.
        try:
            self.db.t._common_filter = lambda q: self.db.t.age > 0
            self._check_select(self.db(self.db.t.id > 0), (self.db.t.id,))
        finally:
            self.db.t._common_filter = None

    def test_common_filter_applied_count(self):
        try:
            self.db.t._common_filter = lambda q: self.db.t.age > 0
            self._check_count(self.db(self.db.t.id > 0))
        finally:
            self.db.t._common_filter = None

    def test_common_filter_applied_update(self):
        try:
            self.db.t._common_filter = lambda q: self.db.t.age > 0
            self._check_update(self.db(self.db.t.id > 0), name="bob")
        finally:
            self.db.t._common_filter = None

    def test_common_filter_applied_delete(self):
        try:
            self.db.t._common_filter = lambda q: self.db.t.age > 0
            self._check_delete(self.db(self.db.t.id > 0))
        finally:
            self.db.t._common_filter = None


@unittest.skipIf(IS_NOSQL, "AST statement compilers are SQL-only")
class TestAstStatementsUnsupported(unittest.TestCase):
    """Explicitly-unsupported features raise so callers can detect them."""

    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table("t1", Field("name"))
        cls.db.define_table("t2", Field("t1_id", "reference t1"))

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def test_bare_table_join_now_supported(self):
        # Bare table in join= is now CROSS JOIN — no longer raises.
        s = self.db(self.db.t1.id == self.db.t2.t1_id)
        node = set_to_select(s, (self.db.t1.id,), {"join": self.db.t2})
        self.assertTrue(any(j.kind == "cross" for j in node.joins))
