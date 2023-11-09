from pydal import DAL

from ._adapt import DEFAULT_URI, drop
from ._compat import unittest


class DALtest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DALtest, self).__init__(*args, **kwargs)
        self._connections = []

    def connect(self, *args, **kwargs):
        if not args:
            kwargs.setdefault("uri", DEFAULT_URI)
        kwargs.setdefault("check_reserved", ["all"])
        ret = DAL(*args, **kwargs)
        self._connections.append(ret)
        return ret

    def tearDown(self):
        for db in self._connections:
            db.commit()
            tablist = list(db.tables)
            for table in reversed(tablist):
                drop(db[table])
            db.close()
        self._connections = []
