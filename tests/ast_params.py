# -*- coding: utf-8 -*-

"""Layer-5 oracle: parameterized SQL compilation and execution.

The base compiler defaults to inline mode (byte-compatible with SQLDialect),
while supported backends can enable parameters by default. These tests verify
well-formed placeholders, captured values, and real database round-trips.
"""

import datetime

from pydal import DAL, Field
from pydal.ast_translate import (
    set_to_select,
    set_to_count,
    set_to_update,
    set_to_delete,
    table_to_insert,
)
from pydal.compilers import PostgresCompiler, PostgresPsycoCompiler, SQLiteCompiler
from pydal.compilers.sql import ParamSQL
from pydal.backends.postgres import Postgres

from ._adapt import DEFAULT_URI, IS_NOSQL, IS_POSTGRESQL
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
            Field("tags", "list:string"),
            Field("active", "boolean"),
            Field("born", "date"),
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

    def test_postgres_defaults_to_format_parameters(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        sql = compiler.compile_select(
            set_to_select(self.db(self.db.t.name == "alice"), (self.db.t.id,), {})
        )
        self.assertIsInstance(sql, ParamSQL)
        self.assertEqual(sql.params, ("alice",))
        self.assertIn("%s", sql)
        self.assertNotIn("'alice'", sql)

    def test_postgres_all_statement_entry_points_bind(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        selected = self.db(self.db.t.name == "alice")
        update_row = self.db.t._fields_and_values_for_update({"age": 31})
        insert_row = self.db.t._fields_and_values_for_insert(
            {"name": "alice", "age": 30}
        )
        statements = (
            compiler.compile_select(
                set_to_select(selected, (self.db.t.id,), {})
            ),
            compiler.compile_count(set_to_count(selected)),
            compiler.compile_update(set_to_update(selected, update_row.op_values())),
            compiler.compile_delete(set_to_delete(selected)),
            compiler.compile_insert(
                table_to_insert(self.db.t, insert_row.op_values())
            ),
        )
        for sql in statements:
            self.assertIsInstance(sql, ParamSQL)
            self.assertIn("%s", sql)
            self.assertTrue(sql.params)

    def test_postgres_insert_retains_params_and_returning(self):
        compiler = PostgresPsycoCompiler(adapter=self.db._adapter)
        row = self.db.t._fields_and_values_for_insert(
            {"name": "alice", "age": 30}
        )
        sql = compiler.compile_insert(table_to_insert(self.db.t, row.op_values()))
        self.assertIsInstance(sql, ParamSQL)
        self.assertCountEqual(sql.params, ("alice", 30))
        self.assertEqual(sql.count("%s"), 2)
        self.assertTrue(sql.endswith(' RETURNING "id";'))

        adapter = object.__new__(Postgres)
        adapter.compiler = compiler
        Postgres._insert(adapter, self.db.t, row.op_values())
        self.assertEqual(adapter._last_insert, (self.db.t.id, 1))

    def test_postgres_percent_values_and_complex_literals_are_safe(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        row = self.db.t._fields_and_values_for_insert(
            {
                "name": "50% complete %s",
                "data": {"progress": "100%", "token": "%s"},
                "tags": ["20%", "%s"],
            }
        )
        sql = compiler.compile_insert(table_to_insert(self.db.t, row.op_values()))
        self.assertEqual(sql.params, ("50% complete %s",))
        self.assertEqual(sql.replace("%%", "").count("%s"), 1)
        self.assertIn("100%%", sql)
        self.assertIn("%%s", sql)

    def test_postgres_typed_values_and_in_list_bind(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        query = (
            self.db.t.active == True  # noqa: E712
        ) & (self.db.t.born == datetime.date(2024, 1, 15)) & self.db.t.age.belongs(
            [20, 30]
        )
        sql = compiler.compile_select(
            set_to_select(self.db(query), (self.db.t.id,), {})
        )
        self.assertEqual(sql.params, ("T", "2024-01-15", 20, 30))
        self.assertEqual(sql.count("%s"), 4)

    def test_postgres_id_and_reference_values_bind(self):
        db = DAL("sqlite:memory")
        try:
            db.define_table("parent", Field("name"))
            db.define_table("child", Field("parent_id", "reference parent"))
            compiler = PostgresPsycoCompiler(adapter=db._adapter)
            sql = compiler.compile_select(
                set_to_select(
                    db(
                        (db.child.id == 7)
                        & (db.child.parent_id == 3)
                    ),
                    (db.child.id,),
                    {},
                )
            )
            self.assertEqual(sql.params, (7, 3))
            self.assertEqual(sql.count("%s"), 2)
        finally:
            db.close()

    def test_postgres_custom_key_reference_uses_target_type(self):
        db = DAL("sqlite:memory")
        try:
            db.define_table("keyed", Field("code"), primarykey=["code"])
            db.define_table(
                "linked",
                Field("keyed_code", "reference keyed.code"),
            )
            compiler = PostgresPsycoCompiler(adapter=db._adapter)
            sql = compiler.compile_select(
                set_to_select(
                    db(db.linked.keyed_code == "alpha"),
                    (db.linked.keyed_code,),
                    {},
                )
            )
            self.assertEqual(sql.params, ("alpha",))
            sql = compiler.compile_select(
                set_to_select(
                    db(db.linked.keyed_code == 123),
                    (db.linked.keyed_code,),
                    {},
                )
            )
            self.assertEqual(sql.params, ("123",))
        finally:
            db.close()

    def test_postgres_string_field_coerces_boolean_to_text(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        sql = compiler.compile_select(
            set_to_select(
                self.db(self.db.t.name == True),  # noqa: E712
                (self.db.t.id,),
                {},
            )
        )
        self.assertEqual(sql.params, ("True",))

    def test_postgres_pattern_helpers_bind_transformed_values(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        expressions = (
            (self.db.t.name.like(r"A\%"), r"A\\%"),
            (self.db.t.name.ilike("A%"), "a%"),
            (self.db.t.name.startswith("a%b"), r"a\%b%"),
            (self.db.t.name.endswith("a_b"), r"%a\_b"),
            (self.db.t.name.contains("a%b"), r"%a\%b%"),
        )
        for expression, expected in expressions:
            with self.subTest(expression=str(expression)):
                sql = compiler.compile_select(
                    set_to_select(self.db(expression), (self.db.t.id,), {})
                )
                self.assertIsInstance(sql, ParamSQL)
                self.assertEqual(sql.params, (expected,))
                self.assertEqual(sql.count("%s"), 1)

    def test_postgres_pattern_bind_order_follows_sql_order(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        expressions = (
            ("like", self.db.t.name.coalesce("fallback").like("a%"), ("fallback", "a%")),
            ("ilike", self.db.t.name.coalesce("fallback").ilike("A%"), ("fallback", "a%")),
            (
                "contains",
                self.db.t.name.coalesce("fallback").contains("a%b"),
                ("fallback", r"%a\%b%"),
            ),
        )
        for name, expression, expected in expressions:
            with self.subTest(expression=name):
                sql = compiler.compile_select(
                    set_to_select(self.db(expression), (self.db.t.id,), {})
                )
                self.assertEqual(sql.params, expected)
                self.assertLess(sql.index("%s"), sql.rindex("%s"))

    def test_postgres_pattern_helpers_decode_bytes_and_keep_null(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        expressions = (
            ("bytes", self.db.t.name.ilike("Ä\\%".encode("utf-8")), ("ä\\\\%",)),
            ("null", self.db.t.name.like(None), ()),
        )
        for label, expression, expected in expressions:
            with self.subTest(expression=label):
                sql = compiler.compile_select(
                    set_to_select(self.db(expression), (self.db.t.id,), {})
                )
                if label == "null":
                    self.assertEqual(sql.params, ())
                    self.assertIn("LIKE NULL", sql)
                else:
                    self.assertEqual(sql.params, expected)

    def test_postgres_case_branch_literals_bind(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        expression = (self.db.t.age > 0).case("positive", "other")
        sql = compiler.compile_select(
            set_to_select(self.db(self.db.t.id > 0), (expression,), {})
        )
        self.assertEqual(sql.params, (0, "positive", "other", 0))
        self.assertEqual(sql.count("%s"), 4)

    def test_set_select_inspection_remains_inline(self):
        compiler = PostgresPsycoCompiler(represent=self.db._adapter.represent)
        original = self.db._adapter.compiler
        self.db._adapter.compiler = compiler
        try:
            sql = self.db(self.db.t.name == "50%")._select(self.db.t.id)
        finally:
            self.db._adapter.compiler = original
        self.assertIs(type(sql), str)
        self.assertNotIsInstance(sql, ParamSQL)
        self.assertIn("'50%'", sql)
        self.assertTrue(compiler.parameterize)

    def test_jdbc_postgres_keeps_inline_compiler(self):
        from pydal.backends.postgres import JDBCPostgres
        from pydal.compilers import compilers

        adapter = object.__new__(JDBCPostgres)
        compiler = compilers.get_for(adapter)
        self.assertIsInstance(compiler, PostgresCompiler)
        self.assertNotIsInstance(compiler, PostgresPsycoCompiler)
        self.assertFalse(compiler.parameterize)

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


@unittest.skipUnless(IS_POSTGRESQL, "live PostgreSQL only")
class TestPostgresParamExecution(unittest.TestCase):
    """Round-trip PostgreSQL's default parameterized compiler path."""

    tablename = "pydal_ast_param_roundtrip"

    def setUp(self):
        self.db = DAL(DEFAULT_URI, migrate=False)
        self.db.executesql('DROP TABLE IF EXISTS "%s";' % self.tablename)
        self.db.commit()
        self.table = self.db.define_table(
            self.tablename,
            Field("name"),
            Field("age", "integer"),
            Field("active", "boolean"),
            Field("born", "date"),
            Field("created", "datetime"),
            Field("payload", "json"),
            Field("tags", "list:string"),
            migrate=False,
        )
        ddl = self.db._adapter.migrator.create_table(self.table, migrate=False)
        self.db._adapter.create_sequence_and_triggers(ddl, self.table)
        self.db.commit()
        self.commands = []
        self._execute = self.db._adapter.driver_io.execute

        def capture(sql, *args, **kwargs):
            self.commands.append((str(sql), getattr(sql, "params", None)))
            return self._execute(sql, *args, **kwargs)

        self.db._adapter.driver_io.execute = capture

    def tearDown(self):
        self.db._adapter.driver_io.execute = self._execute
        try:
            self.db.rollback()
            self.db.executesql('DROP TABLE IF EXISTS "%s";' % self.tablename)
            self.db.commit()
        finally:
            self.db.close()

    def _insert(self, name="50% complete %s", age=30, active=True):
        return self.table.insert(
            name=name,
            age=age,
            active=active,
            born=datetime.date(2024, 1, 15),
            created=datetime.datetime(2024, 1, 15, 12, 30, 45),
            payload={"progress": "100%", "token": "%s"},
            tags=["20%", "%s", "comma, value"],
        )

    def test_crud_and_returning_round_trip(self):
        row_id = self._insert()
        self.assertGreater(int(row_id), 0)
        insert_sql, insert_params = self.commands[-1]
        self.assertTrue(insert_sql.startswith("INSERT INTO "))
        self.assertTrue(insert_sql.endswith(' RETURNING "id";'))
        self.assertIn("%s", insert_sql)
        self.assertIn("50% complete %s", insert_params)

        rows = self.db(
            (self.table.name == "50% complete %s")
            & self.table.age.belongs([20, 30])
        ).select()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows.first().id, int(row_id))
        self.assertEqual(
            self.db(self.table.born == datetime.date(2024, 1, 15)).count(), 1
        )

        updated = self.db(self.table.name == "50% complete %s").update(
            name="75% complete", active=False
        )
        self.assertEqual(updated, 1)
        row = self.db(self.table.name == "75% complete").select().first()
        self.assertEqual((row.name, row.active), ("75% complete", False))

        deleted = self.db(self.table.name == "75% complete").delete()
        self.assertEqual(deleted, 1)
        self.assertEqual(self.db(self.table.age > 0).count(), 0)

        prefixes = tuple(command.split(" ", 1)[0] for command, _ in self.commands)
        for prefix in ("INSERT", "SELECT", "UPDATE", "DELETE"):
            self.assertIn(prefix, prefixes)
        self.assertTrue(
            any(command.startswith("SELECT COUNT(") for command, _ in self.commands)
        )
        self.assertTrue(
            all(params is not None for command, params in self.commands if "%s" in command)
        )

    def test_typed_json_and_array_values_round_trip(self):
        self._insert()
        row = self.db(self.table.active == True).select().first()  # noqa: E712
        self.assertEqual(row.born, datetime.date(2024, 1, 15))
        self.assertEqual(row.created, datetime.datetime(2024, 1, 15, 12, 30, 45))
        self.assertEqual(row.payload, {"progress": "100%", "token": "%s"})
        self.assertEqual(row.tags, ["20%", "%s", "comma, value"])
        insert_sql, insert_params = next(
            (command, params)
            for command, params in self.commands
            if command.startswith("INSERT INTO ")
        )
        self.assertNotIn(self.table.payload.name, insert_params)
        self.assertIn("100%%", insert_sql)
        self.assertIn("%%s", insert_sql)

    def test_value_varied_queries_reuse_command_text(self):
        self._insert(name="alice", age=20)
        self._insert(name="bob", age=30)
        self.commands[:] = []
        self.assertEqual(self.db(self.table.name == "alice").count(), 1)
        self.assertEqual(self.db(self.table.name == "bob").count(), 1)
        counts = [
            (command, params)
            for command, params in self.commands
            if command.startswith("SELECT COUNT(")
        ]
        self.assertEqual(len(counts), 2)
        self.assertEqual(counts[0][0], counts[1][0])
        self.assertEqual(counts[0][1], ("alice",))
        self.assertEqual(counts[1][1], ("bob",))
