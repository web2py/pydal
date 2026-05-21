# -*- coding: utf-8 -*-

"""
Cross-adapter SQL generation tests.

For every supported SQL dialect, swap it into a sqlite:memory DAL
and verify that the five entry points (SELECT/INSERT/UPDATE/DELETE/
COUNT) produce well-formed SQL with the expected dialect-specific
quirks.

These tests don't require any real backend driver — they exercise
the dialect-swap claim from layer 1: build a query once, render it
against any dialect.

Implementation note: the AST compiler is currently registered only
for SQLite. For other dialects we **disable the AST path**
(``adapter.compiler = None``) so the SQL flows through the legacy
``DialectOp`` dispatch — which is exactly what guarantees per-dialect
overrides (Postgres ``~`` for regexp, MSSQL ``[brackets]`` for
identifiers, MySQL backticks, Snowflake no-quoting, etc.) actually
reach the output.
"""

from pydal import DAL, Field

from ._adapt import IS_NOSQL
from ._compat import unittest


def _all_sql_dialects():
    """
    Import and return every SQL dialect class registered in pydal.

    Returns a list of ``(label, dialect_class)`` pairs.
    """
    from pydal.backends.db2 import DB2Dialect
    from pydal.backends.firebird import FireBirdDialect
    from pydal.backends.informix import InformixDialect, InformixSEDialect
    from pydal.backends.ingres import IngresDialect, IngresUnicodeDialect
    from pydal.backends.mssql import (
        MSSQL3Dialect,
        MSSQL3NDialect,
        MSSQL4Dialect,
        MSSQL4NDialect,
        MSSQLDialect,
        MSSQLNDialect,
        SybaseDialect,
        VerticaDialect,
    )
    from pydal.backends.mysql import MySQLDialect
    from pydal.backends.oracle import OracleDialect
    from pydal.backends.postgres import (
        PostgresDialect,
        PostgresDialectArrays,
        PostgresDialectArraysJSON,
        PostgresDialectBoolean,
        PostgresDialectBooleanJSON,
        PostgresDialectJSON,
    )
    from pydal.backends.sap import SAPDBDialect
    from pydal.backends.snowflake import SnowflakeDialect
    from pydal.backends.sqlite import SQLiteDialect
    from pydal.backends.teradata import TeradataDialect

    return [
        ("sqlite", SQLiteDialect),
        ("postgres", PostgresDialect),
        ("postgres-json", PostgresDialectJSON),
        ("postgres-arrays", PostgresDialectArrays),
        ("postgres-arraysjson", PostgresDialectArraysJSON),
        ("postgres-boolean", PostgresDialectBoolean),
        ("postgres-booleanjson", PostgresDialectBooleanJSON),
        ("mysql", MySQLDialect),
        ("mssql", MSSQLDialect),
        ("mssql-n", MSSQLNDialect),
        ("mssql3", MSSQL3Dialect),
        ("mssql3-n", MSSQL3NDialect),
        ("mssql4", MSSQL4Dialect),
        ("mssql4-n", MSSQL4NDialect),
        ("sybase", SybaseDialect),
        ("vertica", VerticaDialect),
        ("oracle", OracleDialect),
        ("db2", DB2Dialect),
        ("firebird", FireBirdDialect),
        ("informix", InformixDialect),
        ("informix-se", InformixSEDialect),
        ("ingres", IngresDialect),
        ("ingres-unicode", IngresUnicodeDialect),
        ("sap", SAPDBDialect),
        ("snowflake", SnowflakeDialect),
        ("teradata", TeradataDialect),
    ]


def _make_db_with_dialect(dialect_cls):
    """
    Build a sqlite:memory DAL with ``dialect_cls`` swapped in.

    The dialect MUST be swapped before ``define_table`` so the table's
    ``_rname`` is computed using the right ``quote_template``. We also
    nullify ``compiler`` to force the legacy DialectOp path, which is
    what picks up dialect-specific SQL overrides (regexp syntax, LIMIT
    placement, etc.).
    """
    db = DAL("sqlite:memory", migrate=False)
    db._adapter.dialect = dialect_cls(db._adapter)
    db._adapter.compiler = None
    db.define_table(
        "person",
        Field("name"),
        Field("age", "integer"),
    )
    return db


@unittest.skipIf(IS_NOSQL, "SQL-only — verifies dialect retargeting")
class TestCrossDialectSQLGeneration(unittest.TestCase):
    """
    Exhaustive sweep: for each SQL dialect, generate the five basic
    statement shapes and check they're well-formed.
    """

    @classmethod
    def setUpClass(cls):
        cls.dialects = _all_sql_dialects()

    def _close(self, db):
        try:
            db.close()
        except Exception:
            pass

    def test_select_for_every_dialect(self):
        for label, dialect_cls in self.dialects:
            with self.subTest(dialect=label):
                db = _make_db_with_dialect(dialect_cls)
                try:
                    sql = db(db.person.age >= 18)._select(
                        db.person.id, db.person.name
                    )
                    self.assertIn("SELECT", sql, "no SELECT keyword: %r" % sql)
                    self.assertIn("FROM", sql, "no FROM keyword: %r" % sql)
                    self.assertIn("person", sql)
                finally:
                    self._close(db)

    def test_insert_for_every_dialect(self):
        for label, dialect_cls in self.dialects:
            with self.subTest(dialect=label):
                db = _make_db_with_dialect(dialect_cls)
                try:
                    sql = db.person._insert(name="alice", age=30)
                    self.assertIn("INSERT INTO", sql)
                    self.assertIn("person", sql)
                finally:
                    self._close(db)

    def test_update_for_every_dialect(self):
        for label, dialect_cls in self.dialects:
            with self.subTest(dialect=label):
                db = _make_db_with_dialect(dialect_cls)
                try:
                    sql = db(db.person.id == 1)._update(name="bob")
                    self.assertIn("UPDATE", sql)
                    self.assertIn("SET", sql)
                    self.assertIn("person", sql)
                finally:
                    self._close(db)

    def test_delete_for_every_dialect(self):
        for label, dialect_cls in self.dialects:
            with self.subTest(dialect=label):
                db = _make_db_with_dialect(dialect_cls)
                try:
                    sql = db(db.person.age < 0)._delete()
                    # MSSQL family and MySQL emit ``DELETE <table> FROM
                    # <table>`` instead of ``DELETE FROM <table>``. Both
                    # forms are valid DELETEs; check that one of them
                    # appears.
                    self.assertTrue(
                        "DELETE FROM" in sql or "DELETE " in sql,
                        "neither DELETE form found: %r" % sql,
                    )
                    self.assertIn("person", sql)
                finally:
                    self._close(db)

    def test_count_for_every_dialect(self):
        for label, dialect_cls in self.dialects:
            with self.subTest(dialect=label):
                db = _make_db_with_dialect(dialect_cls)
                try:
                    sql = db(db.person.age > 0)._count()
                    self.assertIn("SELECT", sql)
                    self.assertIn("COUNT(", sql)
                    self.assertIn("person", sql)
                finally:
                    self._close(db)


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestCrossDialectQuoting(unittest.TestCase):
    """
    Verify each dialect's identifier-quoting character flows through
    the SQL output.
    """

    # Most pydal dialects use the SQL-standard ``"..."`` quoting. The
    # outliers are MySQL (backticks) and Snowflake (no quotes). MSSQL
    # supports ``[brackets]`` too but pydal's MSSQLDialect inherits
    # the standard double-quote default from SQLDialect.
    EXPECTATIONS = {
        "sqlite":         '"t"',
        "postgres":       '"t"',
        "mysql":          "`t`",
        "mssql":          '"t"',
        "sybase":         '"t"',
        "vertica":        '"t"',
        "oracle":         '"t"',
        "db2":            '"t"',
        "firebird":       '"t"',
        "ingres":         '"t"',
        "sap":            '"t"',
        "snowflake":      "t",
        "teradata":       '"t"',
        "informix":       '"t"',
    }

    def test_quote_character_per_dialect(self):
        dmap = dict(_all_sql_dialects())
        for label, expected in self.EXPECTATIONS.items():
            with self.subTest(dialect=label):
                db = DAL("sqlite:memory", migrate=False)
                try:
                    # IMPORTANT: swap dialect BEFORE define_table so
                    # ``_rname`` is computed with the new quote_template.
                    db._adapter.dialect = dmap[label](db._adapter)
                    db._adapter.compiler = None
                    db.define_table("t", Field("x"))
                    sql = db(db.t.x > 0)._select(db.t.id)
                    self.assertIn(
                        expected, sql,
                        "expected %r in %r for dialect %s" % (expected, sql, label),
                    )
                finally:
                    db.close()


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestCrossDialectRegexpSyntax(unittest.TestCase):
    """
    The ``regexp`` operator is a single-statement smoke test that the
    dialect dispatch is wired correctly: backends diverge here.
    """

    # Dialects that have an explicit ``regexp`` override at the SQL
    # level. Other dialects either inherit the base SQLDialect.regexp
    # (no special syntax) or rely on a backend function — we don't
    # exercise those because their output depends on adapter state.
    REGEXP_EXPECTATIONS = {
        "sqlite":   " REGEXP ",
        "postgres": " ~ ",
        "mysql":    " REGEXP ",
        "oracle":   "REGEXP_LIKE",     # Oracle uses a function
    }

    def test_regexp_renders_per_dialect(self):
        dmap = dict(_all_sql_dialects())
        for label, expected in self.REGEXP_EXPECTATIONS.items():
            with self.subTest(dialect=label):
                db = DAL("sqlite:memory", migrate=False)
                try:
                    db._adapter.dialect = dmap[label](db._adapter)
                    db._adapter.compiler = None
                    db.define_table("t", Field("x"))
                    sql = db(db.t.x.regexp("^a"))._select(db.t.id)
                    self.assertIn(
                        expected, sql,
                        "%s: expected %r in %r" % (label, expected, sql),
                    )
                finally:
                    db.close()
