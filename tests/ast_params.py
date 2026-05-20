# -*- coding: utf-8 -*-

"""Layer-5 oracle: parameterized SQL compilation and execution.

The compiler defaults to inline mode (byte-compatible with SQLDialect).
These tests verify the opt-in parameterized mode produces well-formed
SQL with placeholders, captures the right values, and round-trips
through a real sqlite cursor when wired via ParamSQL.
"""

from pydal import DAL, Field
from pydal.ast_translate import (
    set_to_select,
    set_to_count,
    set_to_update,
    set_to_delete,
    table_to_insert,
)
from pydal.compilers import SQLiteCompiler
from pydal.compilers.sql import ParamSQL

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestAstParamCompilation(unittest.TestCase):
    """Static checks: placeholders and params come out of the compiler
    in the expected shape.
    """

    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table(
            "t",
            Field("name"),
            Field("age", "integer"),
            Field("score", "double"),
            Field("data", "json"),  # complex type — stays inlined for now
        )
        cls.compiler = SQLiteCompiler(adapter=cls.db._adapter, parameterize=True)

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def _select_p(self, set_, fields=(), **attrs):
        return self.compiler.compile_select(set_to_select(set_, fields, attrs))

    # ---- shape of the parameterized output ----

    def test_simple_eq_string_binds_value(self):
        sql = self._select_p(self.db(self.db.t.name == "alice"), (self.db.t.id,))
        self.assertIsInstance(sql, ParamSQL)
        self.assertEqual(sql.params, ("alice",))
        self.assertIn("?", sql)
        self.assertNotIn("'alice'", sql)

    def test_integer_value_binds(self):
        sql = self._select_p(self.db(self.db.t.age > 5), (self.db.t.id,))
        self.assertEqual(sql.params, (5,))
        self.assertIn("?", sql)

    def test_double_value_binds(self):
        sql = self._select_p(self.db(self.db.t.score < 3.14), (self.db.t.id,))
        self.assertEqual(sql.params, (3.14,))

    def test_multiple_values_bind_in_order(self):
        s = self.db((self.db.t.age > 5) & (self.db.t.name == "alice"))
        sql = self._select_p(s, (self.db.t.id,))
        # Order matches encounter order in WHERE
        self.assertEqual(sql.params, (5, "alice"))
        self.assertEqual(sql.count("?"), 2)

    def test_belongs_binds_each_value(self):
        sql = self._select_p(
            self.db(self.db.t.age.belongs([1, 2, 3])), (self.db.t.id,)
        )
        self.assertEqual(sql.params, (1, 2, 3))
        # IN list: three placeholders inside (...)
        self.assertIn("IN (?,?,?)", sql)

    def test_none_stays_inline_as_NULL(self):
        # `field == None` collapses to IS NULL upstream — no binding.
        sql = self._select_p(
            self.db(self.db.t.name == None), (self.db.t.id,)  # noqa: E711
        )
        self.assertEqual(sql.params, ())
        self.assertIn("IS NULL", sql)

    # ---- complex types stay inline (intentional, layered scope) ----

    def test_json_value_stays_inline(self):
        # data is "json" type — not in the parameterizable allowlist.
        # represent() serializes it as a JSON literal: '"x"'.
        sql = self._select_p(
            self.db(self.db.t.data == "x"), (self.db.t.id,)
        )
        self.assertEqual(sql.params, ())
        self.assertNotIn("?", sql)

    # ---- date / time / datetime / boolean: bound with adaptation ----

    def test_boolean_binds_as_T_or_F(self):
        # Bool comparisons bind the dialect's true/false token.
        db = DAL("sqlite:memory")
        try:
            db.define_table("e", Field("active", "boolean"))
            c = SQLiteCompiler(adapter=db._adapter, parameterize=True)
            on_sql = c.compile_select(
                set_to_select(db(db.e.active == True), (db.e.id,), {})
            )
            self.assertEqual(on_sql.params, ("T",))
            off_sql = c.compile_select(
                set_to_select(db(db.e.active == False), (db.e.id,), {})
            )
            self.assertEqual(off_sql.params, ("F",))
        finally:
            db.close()

    def test_date_binds_as_iso_string(self):
        import datetime as _dt
        db = DAL("sqlite:memory")
        try:
            db.define_table("e", Field("d", "date"))
            c = SQLiteCompiler(adapter=db._adapter, parameterize=True)
            sql = c.compile_select(
                set_to_select(
                    db(db.e.d == _dt.date(2024, 1, 15)), (db.e.id,), {}
                )
            )
            self.assertEqual(sql.params, ("2024-01-15",))
        finally:
            db.close()

    def test_datetime_binds_with_separator(self):
        import datetime as _dt
        db = DAL("sqlite:memory")
        try:
            db.define_table("e", Field("when", "datetime"))
            c = SQLiteCompiler(adapter=db._adapter, parameterize=True)
            sql = c.compile_select(
                set_to_select(
                    db(db.e["when"] == _dt.datetime(2024, 1, 15, 12, 30, 45)),
                    (db.e.id,), {},
                )
            )
            self.assertEqual(sql.params, ("2024-01-15 12:30:45",))
        finally:
            db.close()

    def test_time_binds_as_iso_string(self):
        import datetime as _dt
        db = DAL("sqlite:memory")
        try:
            db.define_table("e", Field("start", "time"))
            c = SQLiteCompiler(adapter=db._adapter, parameterize=True)
            sql = c.compile_select(
                set_to_select(
                    db(db.e.start == _dt.time(9, 30, 15)), (db.e.id,), {}
                )
            )
            self.assertEqual(sql.params, ("09:30:15",))
        finally:
            db.close()

    # ---- entry points: INSERT / UPDATE / DELETE / COUNT ----

    def test_insert_values_bind(self):
        row = self.db.t._fields_and_values_for_insert(
            {"name": "alice", "age": 30}
        )
        sql = self.compiler.compile_insert(table_to_insert(self.db.t, row.op_values()))
        self.assertIsInstance(sql, ParamSQL)
        self.assertEqual(set(sql.params), {"alice", 30})

    def test_update_set_and_where_both_bind(self):
        s = self.db(self.db.t.name == "alice")
        row = self.db.t._fields_and_values_for_update({"age": 31})
        sql = self.compiler.compile_update(set_to_update(s, row.op_values()))
        # one bind in SET, one in WHERE
        self.assertEqual(sql.count("?"), 2)
        self.assertEqual(sql.params, (31, "alice"))

    def test_delete_binds_where(self):
        s = self.db(self.db.t.name == "alice")
        sql = self.compiler.compile_delete(set_to_delete(s))
        self.assertEqual(sql.params, ("alice",))

    def test_count_binds_where(self):
        s = self.db(self.db.t.age > 10)
        sql = self.compiler.compile_count(set_to_count(s))
        self.assertEqual(sql.params, (10,))


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestAstParamExecution(unittest.TestCase):
    """End-to-end: build a real DAL, flip the compiler to parameterize
    mode, and verify ordinary pydal queries work — proving that
    ParamSQL flows through SQLAdapter.execute correctly.
    """

    def setUp(self):
        self.db = DAL("sqlite:memory")
        self.db.define_table("t", Field("name"), Field("age", "integer"))
        self.db.t.insert(name="alice", age=30)
        self.db.t.insert(name="bob",   age=25)
        self.db.t.insert(name="carol", age=42)
        self.db._adapter.compiler.parameterize = True

    def tearDown(self):
        self.db.close()

    def test_select_returns_correct_rows(self):
        rows = self.db((self.db.t.age > 20) & (self.db.t.name == "alice")).select()
        self.assertEqual([r.name for r in rows], ["alice"])

    def test_count(self):
        # alice=30, bob=25, carol=42 — all > 20. Filter on > 30 catches
        # only carol.
        self.assertEqual(self.db(self.db.t.age > 30).count(), 1)
        self.assertEqual(self.db(self.db.t.age > 20).count(), 3)

    def test_update_and_check(self):
        n = self.db(self.db.t.name == "bob").update(age=99)
        self.assertEqual(n, 1)
        self.assertEqual(
            self.db(self.db.t.name == "bob").select().first().age, 99
        )

    def test_delete_and_check(self):
        n = self.db(self.db.t.name == "carol").delete()
        self.assertEqual(n, 1)
        self.assertEqual(self.db(self.db.t.id > 0).count(), 2)

    def test_belongs_executes(self):
        rows = self.db(self.db.t.age.belongs([25, 30])).select(orderby=self.db.t.age)
        self.assertEqual([r.name for r in rows], ["bob", "alice"])

    def test_insert_via_parameterized_path(self):
        # Insert goes through the AST pipeline too — verify the new row
        # round-trips.
        rid = self.db.t.insert(name="dave", age=17)
        self.assertTrue(rid)
        row = self.db(self.db.t.id == int(rid)).select().first()
        self.assertEqual((row.name, row.age), ("dave", 17))


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestAstParamTypedFilters(unittest.TestCase):
    """End-to-end round-trip for the newly-parameterizable typed
    literals (date/time/datetime/boolean). Filters on each type and
    asserts the right row comes back, proving the value-adaptation +
    bind path matches what pydal stored.
    """

    def setUp(self):
        import datetime as _dt
        self.db = DAL("sqlite:memory")
        self.db.define_table(
            "e",
            Field("name"),
            Field("d", "date"),
            Field("start", "time"),
            Field("when", "datetime"),
            Field("active", "boolean"),
        )
        self.db.e.insert(
            name="early",
            d=_dt.date(2024, 1, 15),
            start=_dt.time(9, 0, 0),
            when=_dt.datetime(2024, 1, 15, 9, 0, 0),
            active=True,
        )
        self.db.e.insert(
            name="late",
            d=_dt.date(2024, 2, 1),
            start=_dt.time(17, 30, 0),
            when=_dt.datetime(2024, 2, 1, 17, 30, 0),
            active=False,
        )

    def tearDown(self):
        self.db.close()

    def test_filter_by_date(self):
        import datetime as _dt
        rows = self.db(self.db.e.d < _dt.date(2024, 1, 20)).select()
        self.assertEqual([r.name for r in rows], ["early"])

    def test_filter_by_time(self):
        import datetime as _dt
        rows = self.db(self.db.e.start == _dt.time(9, 0, 0)).select()
        self.assertEqual([r.name for r in rows], ["early"])

    def test_filter_by_datetime(self):
        import datetime as _dt
        rows = self.db(self.db.e["when"] >= _dt.datetime(2024, 1, 20)).select()
        self.assertEqual([r.name for r in rows], ["late"])

    def test_filter_by_boolean_true(self):
        rows = self.db(self.db.e.active == True).select()  # noqa: E712
        self.assertEqual([r.name for r in rows], ["early"])

    def test_filter_by_boolean_false(self):
        rows = self.db(self.db.e.active == False).select()  # noqa: E712
        self.assertEqual([r.name for r in rows], ["late"])

    def test_insert_round_trip_date_time_datetime(self):
        import datetime as _dt
        self.db.e.insert(
            name="x",
            d=_dt.date(2025, 6, 30),
            start=_dt.time(11, 11, 11),
            when=_dt.datetime(2025, 6, 30, 11, 11, 11),
            active=True,
        )
        row = self.db(self.db.e.name == "x").select().first()
        self.assertEqual(str(row.d), "2025-06-30")
        self.assertEqual(str(row.start), "11:11:11")
        self.assertEqual(str(row["when"]), "2025-06-30 11:11:11")
        self.assertEqual(row.active, True)
