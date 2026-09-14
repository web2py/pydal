import pickle
import time

from pydal import DAL, Field
from pydal.compilers import PostgresPsycoCompiler
from pydal.compilers.sql import ParamSQL
from pydal.utils import hashlib_md5

from ._adapt import DEFAULT_URI, IS_IMAP, IS_MSSQL
from ._compat import unittest
from ._helpers import DALtest


class SimpleCache(object):
    storage = {}

    def clear(self):
        self.storage.clear()

    def _encode(self, value):
        return value

    def _decode(self, value):
        return value

    def __call__(self, key, f, time_expire=300):
        dt = time_expire
        now = time.time()

        item = self.storage.get(key, None)
        if item and f is None:
            del self.storage[key]

        if f is None:
            return None
        if item and (dt is None or item[0] > now - dt):
            return self._decode(item[1])

        value = f()
        self.storage[key] = (now, self._encode(value))
        return value


class PickleCache(SimpleCache):
    def _encode(self, value):
        return pickle.dumps(value, pickle.HIGHEST_PROTOCOL)

    def _decode(self, value):
        return pickle.loads(value)


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestCache(DALtest):
    def testRun(self):
        cache = SimpleCache()
        db = self.connect()
        db.define_table("tt", Field("aa"))
        db.tt.insert(aa="1")
        r0 = db().select(db.tt.ALL)
        r1 = db().select(db.tt.ALL, cache=(cache, 1000))
        self.assertEqual(len(r0), len(r1))
        r2 = db().select(db.tt.ALL, cache=(cache, 1000))
        self.assertEqual(len(r0), len(r2))
        r3 = db().select(db.tt.ALL, cache=(cache, 1000), cacheable=True)
        self.assertEqual(len(r0), len(r3))
        r4 = db().select(db.tt.ALL, cache=(cache, 1000), cacheable=True)
        self.assertEqual(len(r0), len(r4))

    @unittest.skipIf(IS_MSSQL, "Class nesting in ODBC driver breaks pickle")
    def testPickling(self):
        db = self.connect()
        cache = (PickleCache(), 1000)
        db.define_table(
            "tt",
            Field("aa"),
            Field("bb", type="integer"),
            Field("cc", type="decimal(5,2)"),
        )
        db.tt.insert(aa="1", bb=2, cc=3)
        r0 = db(db.tt).select(db.tt.ALL)
        csv0 = str(r0)
        r1 = db(db.tt).select(db.tt.ALL, cache=cache)
        self.assertEqual(csv0, str(r1))
        r2 = db(db.tt).select(db.tt.ALL, cache=cache)
        self.assertEqual(csv0, str(r2))
        r3 = db(db.tt).select(db.tt.ALL, cache=cache, cacheable=True)
        self.assertEqual(csv0, str(r3))
        r4 = db(db.tt).select(db.tt.ALL, cache=cache, cacheable=True)
        self.assertEqual(csv0, str(r4))

    def testParameterizedSelectCacheKeys(self):
        db = self.connect("sqlite:memory")
        cache = SimpleCache()
        cache.clear()
        db.define_table("person", Field("name"))
        db.person.insert(name="alice")
        db.person.insert(name="bob")
        db._adapter.compiler = PostgresPsycoCompiler(
            adapter=db._adapter, placeholder_style="qmark"
        )

        for cacheable, cache_option in (
            (False, (cache, 60)),
            (True, (cache, 60)),
        ):
            cache.clear()
            alice = db(db.person.name == "alice").select(
                db.person.ALL, cache=cache_option, cacheable=cacheable
            )
            bob = db(db.person.name == "bob").select(
                db.person.ALL, cache=cache_option, cacheable=cacheable
            )
            alice_again = db(db.person.name == "alice").select(
                db.person.ALL, cache=cache_option, cacheable=cacheable
            )
            self.assertEqual([row.name for row in alice], ["alice"])
            self.assertEqual([row.name for row in bob], ["bob"])
            self.assertEqual([row.name for row in alice_again], ["alice"])
            self.assertEqual(len(cache.storage), 2)

        cache.clear()
        dict_cache = {"model": cache, "expiration": 60}
        alice = db(db.person.name == "alice").select(db.person.ALL, cache=dict_cache)
        bob = db(db.person.name == "bob").select(db.person.ALL, cache=dict_cache)
        self.assertEqual([row.name for row in alice], ["alice"])
        self.assertEqual([row.name for row in bob], ["bob"])
        self.assertEqual(len(cache.storage), 2)

        cache.clear()
        custom_cache = {
            "model": cache,
            "expiration": 60,
            "key": "custom-select-key",
        }
        db(db.person.name == "alice").select(db.person.ALL, cache=custom_cache)
        db(db.person.name == "bob").select(db.person.ALL, cache=custom_cache)
        self.assertEqual(list(cache.storage), ["custom-select-key"])

    def testSelectCacheKeyCompatibility(self):
        db = self.connect("sqlite:memory")
        adapter = db._adapter
        sql = "SELECT 1"
        empty_params = ParamSQL(sql, ())
        expected = hashlib_md5(adapter.uri + "/" + sql).hexdigest()
        expected_rows = hashlib_md5(adapter.uri + "/" + sql + "/rows").hexdigest()
        self.assertEqual(adapter._select_cache_key(sql), expected)
        self.assertEqual(adapter._select_cache_key(sql, rows=True), expected_rows)
        self.assertEqual(adapter._select_cache_key(empty_params), expected)
        self.assertEqual(adapter._select_cache_key(empty_params, rows=True), expected_rows)

        one = ParamSQL("SELECT ?, ?", (1, "1"))
        one_again = ParamSQL("SELECT ?, ?", (1, "1"))
        different_type = ParamSQL("SELECT ?, ?", ("1", "1"))
        different_boundaries = ParamSQL("SELECT ?, ?", ("a|b", "c"))
        shifted_boundaries = ParamSQL("SELECT ?, ?", ("a", "b|c"))
        self.assertEqual(
            adapter._select_cache_key(one), adapter._select_cache_key(one_again)
        )
        self.assertNotEqual(
            adapter._select_cache_key(one), adapter._select_cache_key(different_type)
        )
        self.assertNotEqual(
            adapter._select_cache_key(different_boundaries),
            adapter._select_cache_key(shifted_boundaries),
        )
        self.assertNotEqual(
            adapter._select_cache_key(one),
            adapter._select_cache_key(one, rows=True),
        )
