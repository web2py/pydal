# -*- coding: utf-8 -*-

"""Tests for the late-layer subquery features:

* CTE (Common Table Expression) — ``set.cte(name, *fields)``
* Select as a join source — ``nested_select(...).with_alias(name).on(...)``
* DISTINCT ON (list of expressions)

All exercised through the AST pipeline (no legacy fallback).
"""

from pydal import DAL, Field
from pydal import ast
from pydal.ast_translate import set_to_select
from pydal.compilers.sql import SQLCompiler

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestCte(unittest.TestCase):

    def setUp(self):
        self.db = DAL("sqlite:memory")
        self.db.define_table(
            "org", Field("name"), Field("boss", "reference org")
        )

    def tearDown(self):
        self.db.close()

    def test_simple_cte_uses_ast(self):
        alice = self.db.org.insert(name="alice")
        self.db.org.insert(name="bob", boss=alice)
        cte = self.db(self.db.org.boss == None).cte("roots", self.db.org.id, self.db.org.name)  # noqa: E711
        rows = self.db().select(cte.ALL)
        self.assertEqual([r.name for r in rows], ["alice"])

    def test_cte_emits_with_clause(self):
        # Capture SQL.
        seen = []
        orig = self.db._adapter.execute
        def tap(*a, **kw):
            seen.append(a[0])
            return orig(*a, **kw)
        self.db._adapter.execute = tap
        try:
            cte = self.db(self.db.org.name == "alice").cte(
                "topboss", self.db.org.id, self.db.org.name
            )
            self.db().select(cte.ALL)
        finally:
            self.db._adapter.execute = orig
        self.assertTrue(any(s.startswith("WITH ") for s in seen))

    def test_cte_value_binds_through_with_clause(self):
        # The CTE body's literal should be parameterized, riding the
        # outer ParamSQL.
        self.db.org.insert(name="alice")
        seen = []
        orig = self.db._adapter.execute
        def tap(*a, **kw):
            seen.append((a[0], getattr(a[0], "params", None)))
            return orig(*a, **kw)
        self.db._adapter.execute = tap
        try:
            cte = self.db(self.db.org.name == "alice").cte(
                "x", self.db.org.id, self.db.org.name
            )
            self.db().select(cte.ALL)
        finally:
            self.db._adapter.execute = orig
        # The select-from-cte SQL should carry 'alice' as a bound param.
        self.assertTrue(any(p and "alice" in p for _, p in seen if p))


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestSelectAsJoinSource(unittest.TestCase):

    def setUp(self):
        self.db = DAL("sqlite:memory")
        self.db.define_table(
            "t", Field("a", "integer"), Field("b")
        )
        self.db.t.insert(a=1, b="x")
        self.db.t.insert(a=2, b="y")
        self.db.t.insert(a=3, b="x")

    def tearDown(self):
        self.db.close()

    def test_subquery_as_join_source(self):
        # SELECT t1.* FROM t AS t1 JOIN (SELECT a, b FROM t WHERE a != 2) AS sub
        # ON sub.a = t1.a
        t1 = self.db.t.with_alias("t1")
        sub = self.db(self.db.t.a != 2).nested_select(
            self.db.t.a, self.db.t.b
        ).with_alias("sub")
        rows = self.db(t1).select(
            t1.a, t1.b,
            join=sub.on(sub.a == t1.a),
            orderby=t1.a,
        )
        self.assertEqual([(r.a, r.b) for r in rows], [(1, "x"), (3, "x")])


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestDistinctOnList(unittest.TestCase):

    def test_distinct_on_list_compiles(self):
        # Use the base SQLCompiler (not SQLite, which rejects DISTINCT ON).
        db = DAL("sqlite:memory")
        try:
            db.define_table("t", Field("a", "integer"), Field("b"))
            c = SQLCompiler(adapter=db._adapter, parameterize=False)
            node = set_to_select(
                db(db.t.a > 0), (db.t.b,), {"distinct": [db.t.a, db.t.b]}
            )
            sql = c.compile_select(node)
            self.assertIn('DISTINCT ON ("t"."a", "t"."b")', sql)
        finally:
            db.close()

    def test_distinct_on_single_field(self):
        db = DAL("sqlite:memory")
        try:
            db.define_table("t", Field("a", "integer"), Field("b"))
            c = SQLCompiler(adapter=db._adapter, parameterize=False)
            node = set_to_select(
                db(db.t.a > 0), (db.t.b,), {"distinct": db.t.a}
            )
            sql = c.compile_select(node)
            self.assertIn('DISTINCT ON ("t"."a")', sql)
        finally:
            db.close()

    def test_sqlite_rejects_distinct_on(self):
        # SQLiteCompiler mirrors the legacy SQLite dialect's refusal.
        db = DAL("sqlite:memory")
        try:
            db.define_table("t", Field("a", "integer"))
            with self.assertRaises(SyntaxError):
                db(db.t.a > 0).select(db.t.a, distinct=db.t.a)
        finally:
            db.close()
