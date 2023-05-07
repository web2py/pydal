import pickle
import time

from pydal import DAL, Field

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
