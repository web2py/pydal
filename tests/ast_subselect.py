# -*- coding: utf-8 -*-

"""Layer-2e tests: AST-native subqueries via Set.subselect(...).

``subselect`` is the recommended new way to build subqueries. It
returns an ast.Select directly, so parameters flow through the outer
compile step instead of being string-embedded.
"""

from pydal import DAL, Field
from pydal import ast

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestSubselect(unittest.TestCase):

    def setUp(self):
        self.db = DAL("sqlite:memory")
        self.db.define_table("parent", Field("name"))
        self.db.define_table(
            "child",
            Field("parent_id", "reference parent"),
            Field("val"),
        )
        self.db.parent.insert(name="alice")
        self.db.parent.insert(name="bob")
        self.db.parent.insert(name="carol")
        self.db.child.insert(parent_id=1, val="x")
        self.db.child.insert(parent_id=2, val="y")
        self.db.child.insert(parent_id=2, val="x")

    def tearDown(self):
        self.db.close()

    # ---------- shape ----------

    def test_subselect_returns_ast_node(self):
        sub = self.db(self.db.child.val == "x").subselect(self.db.child.parent_id)
        self.assertIsInstance(sub, ast.Select)
        self.assertEqual(len(sub.fields), 1)
        self.assertIsInstance(sub.fields[0], ast.FieldRef)

    def test_subselect_is_value_equal_for_same_inputs(self):
        sub1 = self.db(self.db.child.val == "x").subselect(self.db.child.parent_id)
        sub2 = self.db(self.db.child.val == "x").subselect(self.db.child.parent_id)
        self.assertEqual(sub1, sub2)
        # frozen dataclass: hashable
        self.assertEqual(hash(sub1), hash(sub2))

    # ---------- execution ----------

    def test_belongs_subselect_executes(self):
        sub = self.db(self.db.child.val == "x").subselect(self.db.child.parent_id)
        rows = self.db(self.db.parent.id.belongs(sub)).select(orderby=self.db.parent.id)
        self.assertEqual([r.name for r in rows], ["alice", "bob"])

    def test_subselect_carries_bound_params(self):
        # Trace what SQL flows to the cursor for the parameterized form.
        seen = []
        orig = self.db._adapter.execute
        def tap(*a, **kw):
            seen.append((a[0], getattr(a[0], "params", None)))
            return orig(*a, **kw)
        self.db._adapter.execute = tap
        try:
            sub = self.db(self.db.child.val == "x").subselect(self.db.child.parent_id)
            self.db(self.db.parent.id.belongs(sub)).select()
        finally:
            self.db._adapter.execute = orig
        sqls = [s for s, _ in seen]
        all_params = [p for _, p in seen if p]
        # The inner literal 'x' is bound, not inlined.
        self.assertTrue(any("?" in s for s in sqls))
        self.assertTrue(any("x" in (p or ()) for p in all_params))

    def test_legacy_nested_select_still_works(self):
        # Same query, legacy API: nested_select returns a Select object.
        # Translator bridges it onto the AST automatically.
        sub = self.db(self.db.child.val == "x").nested_select(self.db.child.parent_id)
        rows = self.db(self.db.parent.id.belongs(sub)).select(orderby=self.db.parent.id)
        self.assertEqual([r.name for r in rows], ["alice", "bob"])

    def test_legacy_select_string_still_works(self):
        # And the oldest form (belongs(<SQL string>)) keeps working too.
        sub = self.db(self.db.child.val == "x")._select(self.db.child.parent_id)
        self.assertIsInstance(sub, str)  # not ParamSQL: _select forces inline
        rows = self.db(self.db.parent.id.belongs(sub)).select(orderby=self.db.parent.id)
        self.assertEqual([r.name for r in rows], ["alice", "bob"])

    # ---------- correlated ----------

    def test_correlated_subselect(self):
        # Inner WHERE references the outer parent; the inner FROM must
        # NOT re-declare "parent" or rows multiply via cross-join.
        sub = self.db(self.db.parent.id == self.db.child.parent_id).subselect(
            self.db.child.val
        )
        # Parents with a child whose val == 'x' OR 'y'.
        q = self.db.parent.name.belongs(sub)  # 'val' values used by-name
        # The semantic is contrived but exercises the scope tracking.
        rows = self.db(q).select()
        # parent names are alice/bob/carol; child vals are x/y. No
        # parent name matches any child val, so this returns empty,
        # but the point is that the SQL is well-formed and runs.
        self.assertEqual(len(rows), 0)
