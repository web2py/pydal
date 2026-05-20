# -*- coding: utf-8 -*-

"""Layer 4 tests: the I/O Driver split.

Driver is a thin delegate that knows about cursor + DB-API only.
SQLAdapter.execute/commit/rollback/lastrowid forward to it.
"""

from pydal import DAL, Field
from pydal.driver import Driver

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "SQL adapters only")
class TestDriverLayer(unittest.TestCase):

    def setUp(self):
        self.db = DAL("sqlite:memory")
        self.db.define_table("t", Field("name"), Field("age", "integer"))

    def tearDown(self):
        self.db.close()

    def test_driver_io_is_attached_to_adapter(self):
        self.assertIsInstance(self.db._adapter.driver_io, Driver)

    def test_adapter_execute_delegates_to_driver(self):
        # Wrap driver.execute to detect that it's called.
        adapter = self.db._adapter
        seen = []
        orig = adapter.driver_io.execute
        def tap(*a, **kw):
            seen.append(a[0])
            return orig(*a, **kw)
        adapter.driver_io.execute = tap
        try:
            self.db.t.insert(name="a", age=1)
        finally:
            adapter.driver_io.execute = orig
        self.assertTrue(seen, "Driver.execute was not invoked")

    def test_commit_rollback_delegate(self):
        adapter = self.db._adapter
        # Sanity: these don't raise, and round-trip through the Driver.
        self.db.t.insert(name="x", age=1)
        adapter.commit()  # delegates to driver_io.commit()
        adapter.rollback()  # idempotent here; just checks the path

    def test_lastrowid_delegates(self):
        # After an insert, adapter.lastrowid(table) should match
        # driver_io.lastrowid(), and both should reflect what we
        # just inserted.
        adapter = self.db._adapter
        rid = self.db.t.insert(name="bob", age=42)
        self.assertEqual(adapter.lastrowid(self.db.t), adapter.driver_io.lastrowid())
        self.assertEqual(int(rid), adapter.driver_io.lastrowid())

    def test_driver_forwards_paramsql_params(self):
        # End-to-end check: ParamSQL.params attached to the SQL flow
        # through Driver.execute into cursor.execute. We verify by
        # running a literal-bound SELECT and observing the result.
        from pydal.compilers.sql import ParamSQL
        adapter = self.db._adapter
        # need an open cursor — touch the connection
        adapter.cursor  # noqa: B018
        adapter.driver_io.execute(ParamSQL("SELECT ?", ("alice",)))
        self.assertEqual(adapter.cursor.fetchone(), ("alice",))

    def test_driver_execute_falls_back_to_plain_str(self):
        # A plain str SQL with no .params just runs verbatim through
        # the Driver — no surprise binding.
        adapter = self.db._adapter
        adapter.cursor  # noqa: B018
        adapter.driver_io.execute("SELECT 1")
        self.assertEqual(adapter.cursor.fetchone(), (1,))
