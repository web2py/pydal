import locale
import platform
import re
import sys
import uuid
from datetime import datetime
from time import mktime
from .._compat import PY2, pjoin
from .base import SQLAdapter
from . import adapters


@adapters.register_for("sqlite", "sqlite:memory")
class SQLite(SQLAdapter):
    dbengine = "sqlite"
    drivers = ("sqlite2", "sqlite3")

    def _initialize_(self):
        self.pool_size = 0
        super(SQLite, self)._initialize_()
        path_encoding = (
            sys.getfilesystemencoding() or locale.getdefaultlocale()[1] or "utf8"
        )
        if ":memory" in self.uri.split("://", 1)[0]:
            self.dbpath = "file:%s?mode=memory&cache=shared" % uuid.uuid4()
            self.driver_args["uri"] = True
        else:
            self.dbpath = self.uri.split("://", 1)[1]
            if self.dbpath[0] != "/":
                if PY2:
                    self.dbpath = pjoin(
                        self.folder.decode(path_encoding).encode("utf8"), self.dbpath
                    )
                else:
                    self.dbpath = pjoin(self.folder, self.dbpath)
        if "check_same_thread" not in self.driver_args:
            self.driver_args["check_same_thread"] = False
        if "detect_types" not in self.driver_args:
            self.driver_args["detect_types"] = self.driver.PARSE_DECLTYPES

    def _driver_from_uri(self):
        return None

    def connector(self):
        return self.driver.Connection(self.dbpath, **self.driver_args)

    @staticmethod
    def web2py_extract(lookup, s):
        table = {
            "year": (0, 4),
            "month": (5, 7),
            "day": (8, 10),
            "hour": (11, 13),
            "minute": (14, 16),
            "second": (17, 19),
        }
        try:
            if lookup != "epoch":
                (i, j) = table[lookup]
                return int(s[i:j])
            else:
                return mktime(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())
        except:
            return None

    @staticmethod
    def web2py_regexp(expression, item):
        if item is None:
            return False
        return re.compile(expression).search(item) is not None

    def _register_extract(self):
        self.connection.create_function("web2py_extract", 2, self.web2py_extract)

    def _register_regexp(self):
        self.connection.create_function("REGEXP", 2, self.web2py_regexp)

    def after_connection(self):
        self._register_extract()
        self._register_regexp()
        if self.adapter_args.get("foreign_keys", True):
            self.execute("PRAGMA foreign_keys=ON;")

    def select(self, query, fields, attributes):
        if attributes.get("for_update", False) and "cache" not in attributes:
            self.execute("BEGIN IMMEDIATE TRANSACTION;")
        return super(SQLite, self).select(query, fields, attributes)

    def delete(self, table, query):
        db = self.db
        deleted = [x[table._id.name] for x in db(query).select(table._id)]
        counter = super(SQLite, self).delete(table, query)
        if counter:
            for field in table._referenced_by:
                if (
                    field.type == "reference " + table._dalname
                    and field.ondelete == "CASCADE"
                ):
                    db(field.belongs(deleted)).delete()
        return counter


@adapters.register_for("spatialite", "spatialite:memory")
class Spatialite(SQLite):
    dbengine = "spatialite"

    SPATIALLIBS = {
        "Windows": "mod_spatialite.dll",
        "Linux": "libspatialite.so",
        "Darwin": "libspatialite.dylib",
    }

    def after_connection(self):
        self.connection.enable_load_extension(True)
        libspatialite = self.SPATIALLIBS[platform.system()]
        self.execute(r'SELECT load_extension("%s");' % libspatialite)
        super(Spatialite, self).after_connection()


@adapters.register_for("jdbc:sqlite", "jdbc:sqlite:memory")
class JDBCSQLite(SQLite):
    drivers = ("zxJDBC_sqlite",)

    def connector(self):
        return self.driver.connect(
            self.driver.getConnection("jdbc:sqlite:" + self.dbpath), **self.driver_args
        )

    def after_connection(self):
        self._register_extract()
