"""Backend module for postgres."""

# ============================================================
# Adapter
# ============================================================

import os.path
import re

from .._globals import IDENTITY, THREAD_LOCAL
from ..drivers import psycopg2_adapt
from ..utils import split_uri_args
from ..backend_base import AdapterMeta, adapters, with_connection, with_connection_or_raise
from ..backend_base import SQLAdapter


class PostgresMeta(AdapterMeta):
    """
    Metaclass that picks the right Postgres subclass at construction.

    Reads the driver hint out of the URI (e.g. ``postgres:pg8000://``)
    and the set of installed drivers, then re-dispatches the
    constructor to the matching registered class.
    """

    def __call__(cls, *args, **kwargs):
        if cls not in [Postgres, PostgresNew, PostgresBoolean]:
            return AdapterMeta.__call__(cls, *args, **kwargs)
        # choose driver according uri
        available_drivers = [
            driver
            for driver in cls.drivers
            if driver in kwargs["db"]._drivers_available
        ]
        uri_items = kwargs["uri"].split("://", 1)[0].split(":")
        uri_driver = uri_items[1] if len(uri_items) > 1 else None
        if uri_driver and uri_driver in available_drivers:
            driver = uri_driver
        else:
            driver = available_drivers[0] if available_drivers else cls.drivers[0]
        cls = adapters._registry_[uri_items[0] + ":" + driver]
        return AdapterMeta.__call__(cls, *args, **kwargs)


@adapters.register_for("postgres")
class Postgres(SQLAdapter, metaclass=PostgresMeta):
    """
    Base PostgreSQL adapter — psycopg2 driver, distributed transactions
    via ``PREPARE TRANSACTION`` / ``COMMIT PREPARED``.

    URI shape: ``postgres://user:pass@host:port/dbname?sslmode=...``.
    """

    dbengine = "postgres"
    drivers = ("psycopg2",)
    support_distributed_transaction = True

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]*|\[[^\]]+\])(:(?P<port>\d+))?"
        "/(?P<db>[^?]+)"
        r"(\?(?P<uriargs>.*))?$"
    )  # sslmode, ssl (no value) and unix_socket

    def __init__(
        self,
        db,
        uri,
        pool_size=0,
        folder=None,
        db_codec="UTF-8",
        credential_decoder=IDENTITY,
        driver_args={},
        adapter_args={},
        srid=4326,
        after_connection=None,
    ):
        self.srid = srid
        super(Postgres, self).__init__(
            db,
            uri,
            pool_size,
            folder,
            db_codec,
            credential_decoder,
            driver_args,
            adapter_args,
            after_connection,
        )
        self._config_json()

    def _initialize_(self):
        super(Postgres, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        host = m.group("host")
        uriargs = m.group("uriargs")
        if uriargs:
            uri_args = split_uri_args(uriargs, need_equal=False)
        else:
            uri_args = dict()
        socket = uri_args.get("unix_socket")
        if not host and not socket:
            raise SyntaxError("Host or UNIX socket name required")
        db = m.group("db")
        self.driver_args.update(user=user, database=db)
        if password is not None:
            self.driver_args["password"] = password
        if socket:
            if not os.path.exists(socket):
                raise ValueError("UNIX socket %r not found" % socket)
            if self.driver_name == "psycopg2":
                # the psycopg2 driver let you configure the socket directory
                # only (not the socket file name) by passing it as the host
                # (must be an absolute path otherwise the driver tries a TCP/IP
                # connection to host); this behaviour is due to the underlying
                # libpq used by the driver
                socket_dir = os.path.abspath(os.path.dirname(socket))
                self.driver_args["host"] = socket_dir
        else:
            port = int(m.group("port") or 5432)
            self.driver_args.update(host=host, port=port)
            sslmode = uri_args.get("sslmode")
            if sslmode and self.driver_name == "psycopg2":
                self.driver_args["sslmode"] = sslmode
        if self.driver:
            self.__version__ = "%s %s" % (self.driver.__name__, self.driver.__version__)
        else:
            self.__version__ = None
        THREAD_LOCAL._pydal_last_insert_ = None
        self.get_connection()

    def _get_json_dialect(self):

        return PostgresDialectJSON

    def _get_json_parser(self):

        return PostgresAutoJSONParser

    @property
    def _last_insert(self):
        return THREAD_LOCAL._pydal_last_insert_

    @_last_insert.setter
    def _last_insert(self, value):
        THREAD_LOCAL._pydal_last_insert_ = value

    def connector(self):
        return self.driver.connect(**self.driver_args)

    def after_connection(self):
        self.execute("SET CLIENT_ENCODING TO 'UTF8'")
        self.execute("SET standard_conforming_strings=on;")

    def lastrowid(self, table):
        if self._last_insert:
            return int(self.cursor.fetchone()[0])
        sequence_name = table._sequence_name
        self.execute("SELECT currval(%s);" % self.adapt(sequence_name))
        return int(self.cursor.fetchone()[0])

    def _insert(self, table, fields):
        self._last_insert = None
        if fields:
            retval = None
            if hasattr(table, "_id"):
                self._last_insert = (table._id, 1)
                retval = table._id._rname
            return self.dialect.insert(
                table._rname,
                ",".join(el[0]._rname for el in fields),
                ",".join(self.expand(v, f.type) for f, v in fields),
                retval,
            )
        return self.dialect.insert_empty(table._rname)

    @with_connection
    def prepare(self, key):
        self.execute("PREPARE TRANSACTION '%s';" % key)

    @with_connection
    def commit_prepared(self, key):
        self.execute("COMMIT PREPARED '%s';" % key)

    @with_connection
    def rollback_prepared(self, key):
        self.execute("ROLLBACK PREPARED '%s';" % key)


@adapters.register_for("postgres:psycopg2")
class PostgresPsyco(Postgres):
    """
    psycopg2-specific Postgres adapter.

    Detects driver / server version to enable JSON support (psycopg2
    ≥ 2.0.12 + PostgreSQL ≥ 9.2) and adopts the JSON-aware dialect and
    parser. ``adapt`` uses psycopg2's native ``adapt`` to defeat SQL
    injection without round-tripping through pydal's representer.
    """

    drivers = ("psycopg2",)

    def _config_json(self):
        use_json = (
            self.driver.__version__ >= "2.0.12"
            and self.connection.server_version >= 90200
        )
        if use_json:
            self.dialect = self._get_json_dialect()(self)
            if self.driver.__version__ >= "2.5.0":
                self.parser = self._get_json_parser()(self)

    def adapt(self, obj):
        adapted = psycopg2_adapt(obj)
        # deal with new relic Connection Wrapper (newrelic>=2.10.0.8)
        cxn = getattr(self.connection, "__wrapped__", self.connection)
        adapted.prepare(cxn)
        rv = adapted.getquoted()
        if isinstance(rv, bytes):
            return rv.decode("utf-8")
        return rv


@adapters.register_for("postgres2")
class PostgresNew(Postgres):
    """Postgres variant with native ``ARRAY`` column support."""

    def _get_json_dialect(self):
        """JSON-aware dialect that also handles ``ARRAY`` columns."""

        return PostgresDialectArraysJSON

    def _get_json_parser(self):
        """Parser for drivers that auto-decode JSON columns."""

        return PostgresNewAutoJSONParser


@adapters.register_for("postgres2:psycopg2")
class PostgresPsycoNew(PostgresPsyco, PostgresNew):
    """psycopg2 + native arrays — the most common modern Postgres setup."""


@adapters.register_for("postgres3")
class PostgresBoolean(PostgresNew):
    """Variant for drivers that return native Python booleans (skip T/F)."""

    def _get_json_dialect(self):
        """JSON-aware dialect that also expects native booleans."""

        return PostgresDialectBooleanJSON

    def _get_json_parser(self):
        """Parser that trusts the driver's native boolean decoding."""

        return PostgresBooleanAutoJSONParser


@adapters.register_for("postgres3:psycopg2")
class PostgresPsycoBoolean(PostgresPsycoNew, PostgresBoolean):
    """psycopg2 + native arrays + native booleans."""


@adapters.register_for("jdbc:postgres")
class JDBCPostgres(Postgres):
    """PostgreSQL via the zxJDBC bridge (Jython only)."""

    drivers = ("zxJDBC",)

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?"
        "/(?P<db>[^?]+)$"
    )

    def _initialize_(self):
        super(Postgres, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        if password is None:
            password = ""
        host = m.group("host")
        db = m.group("db")
        port = m.group("port") or "5432"
        self.dsn = ("jdbc:postgresql://%s:%s/%s" % (host, port, db), user, password)
        if self.driver:
            self.__version__ = "%s %s" % (self.driver.__name__, self.driver.__version__)
        else:
            self.__version__ = None
        THREAD_LOCAL._pydal_last_insert_ = None
        self.get_connection()

    def connector(self):
        return self.driver.connect(*self.dsn, **self.driver_args)

    def after_connection(self):
        self.connection.set_client_encoding("UTF8")
        self.execute("BEGIN;")
        self.execute("SET CLIENT_ENCODING TO 'UNICODE';")

    def _config_json(self):
        use_json = self.connection.dbversion >= "9.2.0"
        if use_json:
            self.dialect = self._get_json_dialect()(self)

# ============================================================
# Dialect
# ============================================================

from ..helpers.methods import varquote_aux
from ..objects import Expression
from ..backend_base import dialects, register_expression, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(Postgres)
class PostgresDialect(SQLDialect):
    """
    PostgreSQL dialect (base).

    Native ``BOOLEAN`` and ``BYTEA``; ``regexp`` uses ``~`` /
    ``regexp_matches``. Sequences are explicit (``SERIAL`` / ``BIGSERIAL``).
    Distributed transactions supported via ``PREPARE TRANSACTION`` /
    ``COMMIT PREPARED``.
    """

    true_exp = "TRUE"
    false_exp = "FALSE"

    @sqltype_for("blob")
    def type_blob(self):
        return "BYTEA"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_double(self):
        return "FLOAT8"

    @sqltype_for("id")
    def type_id(self):
        return "SERIAL PRIMARY KEY"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGSERIAL PRIMARY KEY"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s ON UPDATE %(on_update_action)s %(null)s %(unique)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            ' CONSTRAINT "FK_%(constraint_name)s_PK" FOREIGN KEY '
            + "(%(field_name)s) REFERENCES %(foreign_table)s"
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s ON UPDATE %(on_update_action)s"
        )

    @sqltype_for("geometry")
    def type_geometry(self):
        return "GEOMETRY"

    @sqltype_for("geography")
    def type_geography(self):
        return "GEOGRAPHY"

    def varquote(self, val):
        return varquote_aux(val, '"%s"')

    def sequence_name(self, tablename):
        return self.quote("%s_id_seq" % tablename)

    def insert(self, table, fields, values, returning=None):
        ret = ""
        if returning:
            ret = "RETURNING %s" % returning
        return "INSERT INTO %s(%s) VALUES (%s)%s;" % (table, fields, values, ret)

    @property
    def random(self):
        return "RANDOM()"

    def add(self, first, second, query_env={}):
        t = first.type
        if t in ("text", "string", "password", "json", "jsonb", "upload", "blob"):
            return "(%s || %s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, first.type, query_env=query_env),
            )
        else:
            return "(%s + %s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, first.type, query_env=query_env),
            )

    def regexp(self, first, second, match_parameter=None, query_env={}, **kwargs):
        return "(%s ~ %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
        )

    def like(self, first, second, escape=None, query_env={}):
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env)
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        if first.type not in ("string", "text", "json", "jsonb"):
            return "(%s LIKE %s ESCAPE '%s')" % (
                self.cast(
                    self.expand(first, query_env=query_env), "CHAR(%s)" % first.length
                ),
                second,
                escape,
            )
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.expand(first, query_env=query_env),
            second,
            escape,
        )

    def ilike(self, first, second, escape=None, query_env={}):
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env)
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        if first.type not in ("string", "text", "json", "jsonb", "list:string"):
            return "(%s ILIKE %s ESCAPE '%s')" % (
                self.cast(
                    self.expand(first, query_env=query_env), "CHAR(%s)" % first.length
                ),
                second,
                escape,
            )
        return "(%s ILIKE %s ESCAPE '%s')" % (
            self.expand(first, query_env=query_env),
            second,
            escape,
        )

    def drop_table(self, table, mode):
        if mode not in ["restrict", "cascade", ""]:
            raise ValueError("Invalid mode: %s" % mode)
        return ["DROP TABLE " + table._rname + " " + mode + ";"]

    def create_index(self, name, table, expressions, unique=False, where=None):
        uniq = " UNIQUE" if unique else ""
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        with self.adapter.index_expander():
            rv = "CREATE%s INDEX %s ON %s (%s)%s;" % (
                uniq,
                self.quote(name),
                table._rname,
                ",".join(self.expand(field) for field in expressions),
                whr,
            )
        return rv

    def st_asgeojson(self, first, second, query_env={}):
        return "ST_AsGeoJSON(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            second["precision"],
            second["options"],
        )

    def st_astext(self, first, query_env={}):
        return "ST_AsText(%s)" % self.expand(first, query_env=query_env)

    def st_aswkb(self, first, query_env={}):
        return "%s" % self.expand(first, query_env=query_env)

    def st_x(self, first, query_env={}):
        return "ST_X(%s)" % (self.expand(first, query_env=query_env))

    def st_y(self, first, query_env={}):
        return "ST_Y(%s)" % (self.expand(first, query_env=query_env))

    def st_contains(self, first, second, query_env={}):
        return "ST_Contains(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_distance(self, first, second, query_env={}):
        return "ST_Distance(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_equals(self, first, second, query_env={}):
        return "ST_Equals(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_intersects(self, first, second, query_env={}):
        return "ST_Intersects(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_overlaps(self, first, second, query_env={}):
        return "ST_Overlaps(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_simplify(self, first, second, query_env={}):
        return "ST_Simplify(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "double", query_env=query_env),
        )

    def st_simplifypreservetopology(self, first, second, query_env={}):
        return "ST_SimplifyPreserveTopology(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "double", query_env=query_env),
        )

    def st_touches(self, first, second, query_env={}):
        return "ST_Touches(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_within(self, first, second, query_env={}):
        return "ST_Within(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_dwithin(self, first, tup, query_env={}):
        return "ST_DWithin(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(tup[0], first.type, query_env=query_env),
            self.expand(tup[1], "double", query_env=query_env),
        )

    def st_transform(self, first, second, query_env={}):
        # The SRID argument can be provided as an integer SRID or a Proj4 string
        if isinstance(second, int):
            return "ST_Transform(%s,%s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, "integer", query_env=query_env),
            )
        else:
            return "ST_Transform(%s,%s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, "string", query_env=query_env),
            )

    @register_expression("doy")
    def extract_doy(self, expr):
        return Expression(expr.db, self.extract, expr, "doy", "integer")

    @register_expression("dow")
    def extract_dow(self, expr):
        return Expression(expr.db, self.extract, expr, "dow", "integer")

    @register_expression("isodow")
    def extract_isodow(self, expr):
        return Expression(expr.db, self.extract, expr, "isodow", "integer")

    @register_expression("isoyear")
    def extract_isoyear(self, expr):
        return Expression(expr.db, self.extract, expr, "isoyear", "integer")

    @register_expression("quarter")
    def extract_quarter(self, expr):
        return Expression(expr.db, self.extract, expr, "quarter", "integer")

    @register_expression("week")
    def extract_week(self, expr):
        return Expression(expr.db, self.extract, expr, "week", "integer")

    @register_expression("decade")
    def extract_decade(self, expr):
        return Expression(expr.db, self.extract, expr, "decade", "integer")

    @register_expression("century")
    def extract_century(self, expr):
        return Expression(expr.db, self.extract, expr, "century", "integer")

    @register_expression("millenium")
    def extract_millenium(self, expr):
        return Expression(expr.db, self.extract, expr, "millenium", "integer")


class PostgresDialectJSON(PostgresDialect):
    @sqltype_for("json")
    def type_json(self):
        return "JSON"

    @sqltype_for("jsonb")
    def type_jsonb(self):
        return "JSONB"

    def st_astext(self, first, query_env={}):
        return "ST_AsText(%s)" % self.expand(first, query_env=query_env)

    def st_asgeojson(self, first, second, query_env={}):
        return "ST_AsGeoJSON(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            second["precision"],
            second["options"],
        )

    def json_key(self, first, key, query_env=None):
        """Get the json in key which you can use for more queries"""
        if isinstance(key, str):
            key = self.expand(key, "string", query_env=query_env)
        elif not isinstance(key, int):
            raise TypeError("Key must be a string or int")
        return "%s->%s" % (self.expand(first, query_env=query_env or {}), key)

    def json_key_value(self, first, key, query_env=None):
        """Get the value int or text in key"""
        if isinstance(key, str):
            key = self.expand(key, "string", query_env=query_env)
        elif isinstance(key, int):
            key = self.expand(key, "integer", query_env=query_env)
        else:
            raise TypeError("Key must be a string or int")
        return "%s->>%s" % (self.expand(first, query_env=query_env or {}), key)

    def json_path(self, first, path, query_env=None):
        """Get the json in path which you can use for more queries"""
        return "%s#>%s" % (
            self.expand(first, query_env=query_env or {}),
            self.adapter.adapt(path),
        )

    def json_path_value(self, first, path, query_env=None):
        """Get the json in path which you can use for more queries"""
        return "%s#>>%s" % (
            self.expand(first, query_env=query_env or {}),
            self.adapter.adapt(path),
        )

    # JSON Queries
    def json_contains(self, first, jsonvalue, query_env=None):
        # requires jsonb, value is json e.g. '{"country": "Peru"}'
        return "%s::jsonb@>%s::jsonb" % (
            self.expand(first, query_env=query_env or {}),
            self.adapter.adapt(jsonvalue),
        )


@dialects.register_for(PostgresNew)
class PostgresDialectArrays(PostgresDialect):
    @sqltype_for("list:integer")
    def type_list_integer(self):
        return "BIGINT[]"

    @sqltype_for("list:string")
    def type_list_string(self):
        return "TEXT[]"

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return "BIGINT[]"

    def any(self, val, query_env={}):
        return "ANY(%s)" % self.expand(val, query_env=query_env)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        if first.type.startswith("list:"):
            f = self.expand(
                second,
                "string" if first.type == "list:string" else "integer",
                query_env=query_env,
            )
            s = self.any(first, query_env)
            if not case_sensitive and first.type == "list:string":
                return self.ilike(f, s, escape="\\", query_env=query_env)
            return self.eq(f, s)
        return super(PostgresDialectArrays, self).contains(
            first, second, case_sensitive=case_sensitive, query_env=query_env
        )

    def ilike(self, first, second, escape=None, query_env={}):
        if first and "type" not in first:
            args = (first, self.expand(second, query_env=query_env))
            return "(%s ILIKE %s)" % args
        return super(PostgresDialectArrays, self).ilike(
            first, second, escape=escape, query_env=query_env
        )

    def eq(self, first, second=None, query_env={}):
        if first and "type" not in first:
            return "(%s = %s)" % (first, self.expand(second, query_env=query_env))
        return super(PostgresDialectArrays, self).eq(first, second, query_env)


class PostgresDialectArraysJSON(PostgresDialectArrays, PostgresDialectJSON):
    pass


@dialects.register_for(PostgresBoolean)
class PostgresDialectBoolean(PostgresDialectArrays):
    @sqltype_for("boolean")
    def type_boolean(self):
        return "BOOLEAN"


class PostgresDialectBooleanJSON(PostgresDialectBoolean, PostgresDialectJSON):
    pass

# ============================================================
# Parser
# ============================================================

from ..backend_base import for_type, parsers
from ..backend_base import BasicParser, JSONParser, ListsParser


@parsers.register_for(Postgres)
class PostgresParser(ListsParser, JSONParser):
    """Postgres parser with ``jsonb`` reusing the ``json`` parser."""

    @for_type("jsonb")
    def _jsonb(self, value):
        """Parse ``jsonb`` columns the same way as ``json``."""
        return self.registered["json"](value, "json")


class PostgresAutoJSONParser(ListsParser):
    """Variant for drivers that auto-decode JSON; no JSON parsing needed."""


@parsers.register_for(PostgresNew)
class PostgresNewParser(JSONParser):
    """Parser for newer Postgres adapters (no separate list/array handling)."""


class PostgresNewAutoJSONParser(BasicParser):
    """Newer Postgres + auto-JSON-decoding driver."""


@parsers.register_for(PostgresBoolean)
class PostgresBooleanParser(JSONParser):
    """
    Postgres parser that trusts the driver's native boolean conversion.

    The generic JSONParser would re-route booleans through string
    coercion; pass values through unchanged here.
    """

    @for_type("boolean")
    def _boolean(self, value):
        """Pass the driver's already-decoded boolean through unchanged."""
        return value


class PostgresBooleanAutoJSONParser(BasicParser):
    """Postgres native boolean + auto-JSON driver — same passthrough."""

    @for_type("boolean")
    def _boolean(self, value):
        """Pass driver-native booleans through."""
        return value

# ============================================================
# Representer
# ============================================================

from ..helpers.serializers import serializers
from ..backend_base import before_type, repr_for_type, representers
from ..backend_base import JSONRepresenter, SQLRepresenter


@representers.register_for(Postgres)
class PostgresRepresenter(SQLRepresenter, JSONRepresenter):
    """PostgreSQL representer with PostGIS geometry/geography + JSONB."""

    def _make_geoextra(self, field_type):
        """
        Extract the SRID from a PostGIS type string like
        ``geometry(POINT,4326)``. Defaults to 4326 (WGS 84).
        """
        srid = 4326
        geotype, params = field_type[:-1].split("(")
        params = params.split(",")
        if len(params) >= 2:
            schema, srid = params[:2]
        return {"srid": srid}

    @before_type("geometry")
    def geometry_extras(self, field_type):
        """Extract SRID for ``geometry`` columns."""
        return self._make_geoextra(field_type)

    @repr_for_type("geometry", adapt=False)
    def _geometry(self, value, srid):
        """Render either WKB hex (when value starts with ``0``) or WKT via ST_GeomFromText."""
        # If the value starts with a ``0`` we treat it as a WKB hex
        # blob and pass it through; otherwise we treat it as WKT and
        # route through ``ST_GeomFromText``.
        if value[0] == "0":
            return "E'%s'" % value
        return "ST_GeomFromText('%s',%s)" % (value, srid)

    @before_type("geography")
    def geography_extras(self, field_type):
        """Extract SRID for ``geography`` columns."""
        return self._make_geoextra(field_type)

    @repr_for_type("geography", adapt=False)
    def _geography(self, value, srid):
        """Render WKT via ``ST_GeogFromText('SRID=...;<wkt>')``."""
        return "ST_GeogFromText('SRID=%s;%s')" % (srid, value)

    @repr_for_type("jsonb", encode=True)
    def _jsonb(self, value):
        """Serialize Python values to a JSONB string literal."""
        return serializers.json(value)


@representers.register_for(PostgresNew)
class PostgresArraysRepresenter(PostgresRepresenter):
    """
    PostgreSQL representer for adapters with array support.

    Adds the ``{a,b,c}`` array-literal serialization used by
    Postgres' ``ARRAY`` columns.
    """

    def _listify_elements(self, elements):
        """Render a Python sequence as a Postgres array literal ``{a,b,c}``."""
        return "{" + ",".join(str(el) for el in elements) + "}"

