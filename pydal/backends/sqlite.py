"""Backend module for sqlite."""

# ============================================================
# Adapter
# ============================================================

import platform
import re
import uuid
from datetime import date, datetime
from os.path import join as pjoin
from time import mktime

from ..backend_base import adapters
from ..backend_base import SQLAdapter


def convert_date(val):
    """Decoder registered with sqlite3 for ``DATE`` declared types."""
    return date.fromisoformat(val.decode("utf-8"))


def convert_datetime(val):
    """Decoder registered with sqlite3 for ``DATETIME`` / ``TIMESTAMP`` types."""
    return datetime.fromisoformat(val.decode("utf-8"))


@adapters.register_for("sqlite", "sqlite:memory")
class SQLite(SQLAdapter):
    """
    SQLite adapter — covers ``sqlite://path``, ``sqlite:memory``.

    Drivers tried: ``sqlite2`` (legacy pysqlite2), then ``sqlite3``
    (stdlib). ``sqlite:memory`` uses a per-DAL shared in-memory
    database via the ``file:<uuid>?mode=memory&cache=shared`` URI.

    Pool size is forced to 0 — SQLite connections aren't pool-safe
    across threads.
    """

    dbengine = "sqlite"
    drivers = ("sqlite2", "sqlite3")

    def _initialize_(self):
        self.pool_size = 0
        super(SQLite, self)._initialize_()
        if ":memory" in self.uri.split("://", 1)[0]:
            self.dbpath = "file:%s?mode=memory&cache=shared" % uuid.uuid4()
            self.driver_args["uri"] = True
        else:
            self.dbpath = self.uri.split("://", 1)[1]
            if self.dbpath[0] != "/":
                self.dbpath = pjoin(self.folder, self.dbpath)
        if "check_same_thread" not in self.driver_args:
            self.driver_args["check_same_thread"] = False
        if "detect_types" not in self.driver_args:
            self.driver_args["detect_types"] = self.driver.PARSE_DECLTYPES

        import sqlite3

        sqlite3.register_converter("DATE", convert_date)
        sqlite3.register_converter("TIMESTAMP", convert_datetime)

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
    """
    SpatiaLite adapter — SQLite + the SpatiaLite GIS extension.

    Loads the platform-appropriate ``mod_spatialite`` library via
    ``SELECT load_extension(...)`` on first connection. The library
    path is platform-detected from ``SPATIALLIBS``.
    """

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
    """SQLite via the zxJDBC bridge (Jython only)."""

    drivers = ("zxJDBC_sqlite",)

    def connector(self):
        return self.driver.connect(
            self.driver.getConnection("jdbc:sqlite:" + self.dbpath), **self.driver_args
        )

    def after_connection(self):
        self._register_extract()

# ============================================================
# Dialect
# ============================================================

from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(SQLite)
class SQLiteDialect(SQLDialect):
    """
    SQLite dialect.

    SQLite is dynamically typed at the value level — most column types
    map to broad affinities (``CHAR``, ``DOUBLE``, ``INTEGER``, ...).
    Regexp is routed through a user function (``web2py_extract`` for
    EXTRACT, ``REGEXP`` operator for matching).
    """

    @sqltype_for("string")
    def type_string(self):
        """Map ``string`` to ``CHAR(n)``."""
        return "CHAR(%(length)s)"

    @sqltype_for("float")
    def type_float(self):
        return "DOUBLE"

    @sqltype_for("double")
    def type_double(self):
        return self.types["float"]

    @sqltype_for("decimal")
    def type_decimal(self):
        return self.types["float"]

    def extract(self, field, what, query_env={}):
        return "web2py_extract('%s', %s)" % (
            what,
            self.expand(field, query_env=query_env),
        )

    def regexp(self, first, second, match_parameter=None, query_env={}):
        return "(%s REGEXP %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
        )

    def select(
        self,
        fields,
        tables,
        where=None,
        groupby=None,
        having=None,
        orderby=None,
        limitby=None,
        distinct=False,
        for_update=False,
        with_cte="",
    ):
        if distinct and distinct is not True:
            raise SyntaxError("DISTINCT ON is not supported by SQLite")
        return super(SQLiteDialect, self).select(
            fields,
            tables,
            where,
            groupby,
            having,
            orderby,
            limitby,
            distinct,
            for_update,
            with_cte,
        )

    def truncate(self, table, mode=""):
        tablename = self.adapter.expand(table._raw_rname, "string")
        return [
            self.delete(table),
            "DELETE FROM sqlite_sequence WHERE name=%s" % tablename,
        ]

    def writing_alias(self, table):
        if table._dalname != table._tablename:
            raise SyntaxError("SQLite does not support UPDATE/DELETE on aliased table")
        return table._rname


@dialects.register_for(Spatialite)
class SpatialiteDialect(SQLiteDialect):
    """
    SpatiaLite dialect — adds a ``geometry`` column type and a
    handful of GIS function emitters (``AsGeoJSON``, ``Distance``,
    ``Within``, ...).
    """

    @sqltype_for("geometry")
    def type_geometry(self):
        """Map ``geometry`` to SpatiaLite's ``GEOMETRY`` type."""
        return "GEOMETRY"

    def st_asgeojson(self, first, second, query_env={}):
        return "AsGeoJSON(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            second["precision"],
            second["options"],
        )

    def st_astext(self, first, query_env={}):
        return "AsText(%s)" % self.expand(first, query_env=query_env)

    def st_contains(self, first, second, query_env={}):
        return "Contains(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_distance(self, first, second, query_env={}):
        return "Distance(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_equals(self, first, second, query_env={}):
        return "Equals(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_intersects(self, first, second, query_env={}):
        return "Intersects(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_overlaps(self, first, second, query_env={}):
        return "Overlaps(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_simplify(self, first, second, query_env={}):
        return "Simplify(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "double", query_env=query_env),
        )

    def st_touches(self, first, second, query_env={}):
        return "Touches(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_within(self, first, second, query_env={}):
        return "Within(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

# ============================================================
# Parser
# ============================================================

from datetime import date, datetime
from decimal import Decimal

from ..backend_base import before_parse, for_type, parsers
from ..backend_base import DateParser, DateTimeParser, JSONParser, ListsParser, TimeParser


@parsers.register_for(SQLite)
class SQLiteParser(ListsParser, DateParser, TimeParser, DateTimeParser, JSONParser):
    """
    SQLite parser composing list/date/time/datetime/JSON handlers with
    decimal-precision support extracted from the type string.

    SQLite has no native ``DECIMAL`` storage class — values arrive as
    floats and we re-quantize them at parse time to the field's
    declared precision.
    """

    @before_parse("decimal")
    def decimal_extras(self, field_type):
        """Pull the precision off a ``decimal(p,d)`` type string."""
        return {"decimals": field_type[8:-1].split(",")[1].strip()}

    @for_type("decimal")
    def _decimal(self, value, decimals):
        """Quantize the driver-supplied float to ``decimals`` places."""
        value = "{0:.{precision}f}".format(value, precision=decimals)
        return Decimal(value)

    @for_type("date")
    def _date(self, value):
        """Fall back to string parsing if the driver didn't decode a date."""
        if not isinstance(value, date):
            return DateParser._declared_parsers_["_date"].f(self, value)
        return value

    @for_type("datetime")
    def _datetime(self, value):
        """Fall back to string parsing if the driver didn't decode a datetime."""
        if not isinstance(value, datetime):
            return DateTimeParser._declared_parsers_["_datetime"].f(self, value)
        return value

# ============================================================
# Representer
# ============================================================

from ..backend_base import before_type, repr_for_type, representers
from ..backend_base import JSONRepresenter, SQLRepresenter


@representers.register_for(SQLite)
class SQLiteRepresenter(SQLRepresenter, JSONRepresenter):
    """Plain SQL + JSON; SQLite has no per-type overrides."""


@representers.register_for(Spatialite)
class SpatialiteRepresenter(SQLRepresenter):
    """SpatiaLite representer with PostGIS-style ``ST_GeomFromText``."""

    @before_type("geometry")
    def geometry_extras(self, field_type):
        """Extract the SRID from a ``geometry(POINT,4326)``-style type string."""
        srid = 4326
        geotype, params = field_type[:-1].split("(")
        params = params.split(",")
        if len(params) >= 2:
            schema, srid = params[:2]
        return {"srid": srid}

    @repr_for_type("geometry", adapt=False)
    def _geometry(self, value, srid):
        """Render WKT geometry via ``ST_GeomFromText('<wkt>', srid)``."""
        return "ST_GeomFromText('%s',%s)" % (value, srid)

