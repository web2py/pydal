"""Backend module for mssql."""

# ============================================================
# Adapter
# ============================================================

import re

from .._globals import IDENTITY
from ..utils import split_uri_args
from ..backend_base import adapters, with_connection_or_raise
from ..backend_base import SQLAdapter


class Slicer(object):
    """
    Mixin: client-side slicing for adapters where the dialect can't
    express ``LIMIT``/``OFFSET`` in pure SQL.
    """

    def rowslice(self, rows, minimum=0, maximum=None):
        """Slice ``rows[minimum:maximum]`` in Python."""
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]


class MSSQL(SQLAdapter):
    """
    MSSQL base adapter — drivers tried in order: ``pyodbc``, ``pytds``,
    ``pymssql``, ``mssql-python``.

    URI shape: ``mssql://user:pass@host:port/dbname?<odbc_opts>``.
    The ``srid`` parameter (default 4326) is forwarded to the GIS
    representer for ``geometry`` / ``geography`` columns.
    """

    dbengine = "mssql"
    drivers = ("pyodbc", "pytds", "pymssql", "mssql-python")

    REGEX_DSN = "^.+$"
    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?"
        "/(?P<db>[^?]+)"
        r"(\?(?P<uriargs>.*))?$"
    )

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
        super(MSSQL, self).__init__(
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

    def _initialize_(self):
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            self.dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group("user"))
            password = self.credential_decoder(m.group("password"))
            if password is None:
                password = ""
            host = m.group("host")
            db = m.group("db")
            port = m.group("port") or "1433"
            # Parse the optional uri name-value arg pairs after the '?'
            # (in the form of arg1=value1&arg2=value2&...)
            # (drivers like FreeTDS insist on uppercase parameter keys)
            argsdict = {"DRIVER": "{SQL Server}"}
            uriargs = m.group("uriargs")
            if uriargs:
                for argkey, argvalue in split_uri_args(
                    uriargs, separators="&", need_equal=True
                ).items():
                    argsdict[argkey.upper()] = argvalue
            uriargs = ";".join(["%s=%s" % (ak, av) for (ak, av) in argsdict.items()])
            self.dsn = "SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;%s" % (
                host,
                port,
                db,
                user,
                password,
                uriargs,
            )

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)

    def lastrowid(self, table):
        self.execute("SELECT SCOPE_IDENTITY();")
        return int(self.cursor.fetchone()[0])


@adapters.register_for("mssql")
class MSSQL1(MSSQL, Slicer):
    pass


@adapters.register_for("mssql3")
class MSSQL3(MSSQL):
    pass


@adapters.register_for("mssql4")
class MSSQL4(MSSQL):
    pass


class MSSQLN(MSSQL):
    def represent(self, obj, field_type):
        rv = super(MSSQLN, self).represent(obj, field_type)
        if field_type in ("string", "text", "json") and rv.startswith("'"):
            rv = "N" + rv
        return rv

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        return super(MSSQLN, self).execute(*args, **kwargs)


@adapters.register_for("mssqln", "mssql2")
class MSSQL1N(MSSQLN, Slicer):
    pass


@adapters.register_for("mssql3n")
class MSSQL3N(MSSQLN):
    pass


@adapters.register_for("mssql4n")
class MSSQL4N(MSSQLN):
    pass


@adapters.register_for("pytds")
class PyTDS(MSSQL):
    def _initialize_(self):
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            self.dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            self.dsn = m.group("host")
            self.driver_args.update(
                user=self.credential_decoder(m.group("user")),
                password=self.credential_decoder(m.group("password")) or "",
                database=m.group("db"),
                port=m.group("port") or "1433",
            )

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)


@adapters.register_for("vertica")
class Vertica(MSSQL1):
    def lastrowid(self, table):
        self.execute("SELECT SCOPE_IDENTITY();")
        return int(self.cursor.fetchone()[0])


@adapters.register_for("sybase")
class Sybase(MSSQL1):
    dbengine = "sybase"

    def _initialize_(self):
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group("user"))
            password = self.credential_decoder(m.group("password"))
            if password is None:
                password = ""
            host = m.group("host")
            db = m.group("db")
            port = m.group("port") or "1433"
            self.dsn = "sybase:host=%s:%s;dbname=%s" % (host, port, db)
            self.driver_args.update(
                user=self.credential_decoder(user),
                passwd=self.credential_decoder(password),
            )


# Added support for Pymssql
@adapters.register_for("pymssql")
class PyMssql(MSSQL):
    def _initialize_(self):
        import pymssql

        self.driver = pymssql
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            self.dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            self.dsn = m.group("host")
            self.driver_args.update(
                user=self.credential_decoder(m.group("user")),
                password=self.credential_decoder(m.group("password")) or "",
                database=m.group("db"),
                port=m.group("port") or "1433",
            )

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)


# Microsoft mssql-python driver support (SQL Server 2012+)
@adapters.register_for("mssqlpython")
class MSSQLPython(MSSQL4):
    """
    MSSQL adapter for Microsoft's official mssql-python driver.

    Supports SQL Server 2012+ with modern OFFSET/FETCH pagination.
    Features encryption-by-default and Microsoft Entra ID authentication.

    URI format:
        mssqlpython://user:password@server:port/database?param=value

    Supported URI parameters:
        - encrypt: Enable encryption (default: yes, values: yes/no/true/false/1/0)
        - trust_server_certificate: Trust self-signed certificates (default: no)
        - authentication: Authentication mode. Supported values:
            * ActiveDirectoryPassword: Username/password authentication
            * ActiveDirectoryInteractive: Interactive browser login
            * ActiveDirectoryMSI: Managed Identity (system/user-assigned)
            * ActiveDirectoryServicePrincipal: Service principal with client ID/secret
            * ActiveDirectoryIntegrated: Integrated Windows Auth/Kerberos
            * ActiveDirectoryDefault: Default authentication based on environment
            * ActiveDirectoryDeviceCode: Device code flow authentication

    Example URIs:
        # Standard SQL authentication with encryption
        mssqlpython://user:pass@server.database.windows.net:1433/mydb

        # Entra ID Interactive authentication
        mssqlpython://user@server.database.windows.net/mydb?authentication=ActiveDirectoryInteractive

        # Managed Identity (no user/password needed)
        mssqlpython://@server.database.windows.net/mydb?authentication=ActiveDirectoryMSI

        # Disable encryption (for testing/legacy)
        mssqlpython://user:pass@localhost:1433/mydb?encrypt=no&trust_server_certificate=yes
    """

    drivers = (
        "mssql-python",
    )  # Note: URI scheme is mssqlpython://, driver name is mssql-python

    # Override REGEX_URI to make user optional for Managed Identity scenarios
    REGEX_URI = (
        r"^(?P<user>[^:@]*)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?"
        r"/(?P<db>[^?]+)"
        r"(\?(?P<uriargs>.*))?$"
    )

    def _initialize_(self):
        import mssql_python

        self.driver = mssql_python
        # Skip MSSQL._initialize_() and call grandparent to avoid base MSSQL's REGEX
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)

        # Parse URI query parameters
        uriargs = m.group("uriargs")
        uri_args = split_uri_args(uriargs, need_equal=True) if uriargs else {}

        # Extract basic connection parameters
        user = self.credential_decoder(m.group("user")) if m.group("user") else ""
        password = (
            self.credential_decoder(m.group("password")) if m.group("password") else ""
        )

        # Build driver connection arguments
        self.driver_args.update(
            server=m.group("host"),
            database=m.group("db"),
            port=int(m.group("port") or "1433"),
        )

        # Add credentials if provided (not required for some Entra ID auth modes)
        if user:
            self.driver_args["user"] = user
        if password:
            self.driver_args["password"] = password

        # Handle authentication parameter
        # Supported modes: ActiveDirectoryPassword, ActiveDirectoryInteractive,
        # ActiveDirectoryMSI, ActiveDirectoryServicePrincipal,
        # ActiveDirectoryIntegrated, ActiveDirectoryDefault, ActiveDirectoryDeviceCode
        if "authentication" in uri_args:
            self.driver_args["authentication"] = uri_args["authentication"]

        # Handle encryption parameters (default: enabled for security)
        encrypt_param = uri_args.get("encrypt", "yes").lower()
        # mssql-python expects string values "yes"/"no", not boolean
        if encrypt_param in ("yes", "true", "1"):
            self.driver_args["encrypt"] = "yes"
        else:
            self.driver_args["encrypt"] = "no"

        # Handle trust_server_certificate parameter (default: disabled for security)
        if "trust_server_certificate" in uri_args:
            trust_param = uri_args["trust_server_certificate"].lower()
            # mssql-python expects string values "yes"/"no", not boolean
            if trust_param in ("yes", "true", "1"):
                self.driver_args["trust_server_certificate"] = "yes"
            else:
                self.driver_args["trust_server_certificate"] = "no"

        # Pass through any additional driver-specific parameters
        # Exclude already-handled parameters
        handled_params = {"authentication", "encrypt", "trust_server_certificate"}
        for key, value in uri_args.items():
            if key not in handled_params:
                self.driver_args[key] = value

    def connector(self):
        return self.driver.connect(**self.driver_args)

# ============================================================
# Dialect
# ============================================================

from ..helpers.methods import varquote_aux
from ..objects import Expression
from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(MSSQL)
class MSSQLDialect(SQLDialect):
    """
    MSSQL base dialect (legacy ``TOP``-based pagination).

    Use only when targeting MSSQL < 2005; newer servers should use
    ``MSSQL3Dialect`` / ``MSSQL4Dialect`` for better pagination semantics.
    """

    true = 1
    false = 0
    true_exp = "1=1"
    false_exp = "1=0"
    dt_sep = "T"

    @sqltype_for("boolean")
    def type_boolean(self):
        return "BIT"

    @sqltype_for("blob")
    def type_blob(self):
        return "IMAGE"

    @sqltype_for("integer")
    def type_integer(self):
        return "INT"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_double(self):
        return "FLOAT"

    @sqltype_for("date")
    def type_date(self):
        return "DATE"

    @sqltype_for("time")
    def type_time(self):
        return "CHAR(8)"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "DATETIME"

    @sqltype_for("id")
    def type_id(self):
        return "INT IDENTITY PRIMARY KEY"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INT%(null)s%(unique)s, CONSTRAINT %(constraint_name)s "
            + "FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_key)s ON "
            + "DELETE %(on_delete_action)s"
        )

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT IDENTITY PRIMARY KEY"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT%(null)s%(unique)s, CONSTRAINT %(constraint_name)s"
            + " FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ", CONSTRAINT FK_%(constraint_name)s FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_key)s ON DELETE "
            + "%(on_delete_action)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            " CONSTRAINT FK_%(constraint_name)s_PK FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_table)s "
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("geometry")
    def type_geometry(self):
        return "geometry"

    @sqltype_for("geography")
    def type_geography(self):
        return "geography"

    def varquote(self, val):
        return varquote_aux(val, "[%s]")

    def update(self, table, values, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "UPDATE %s SET %s FROM %s%s;" % (
            table.sql_shortref,
            values,
            tablename,
            whr,
        )

    def delete(self, table, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "DELETE %s FROM %s%s;" % (table.sql_shortref, tablename, whr)

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
        with_cte=None,  # ['recursive' | '', sql]
    ):
        dst, whr, grp, order, limit, upd = "", "", "", "", "", ""
        if distinct is True:
            dst = " DISTINCT"
        elif distinct:
            dst = " DISTINCT ON (%s)" % distinct
        if where:
            whr = " %s" % self.where(where)
        if groupby:
            grp = " GROUP BY %s" % groupby
            if having:
                grp += " HAVING %s" % having
        if orderby:
            order = " ORDER BY %s" % orderby
        if limitby:
            (lmin, lmax) = limitby
            limit = " TOP %i" % lmax
        if for_update:
            upd = " FOR UPDATE"

        if with_cte:
            recursive, cte = with_cte
            recursive = " RECURSIVE" if recursive else ""
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

        return "%sSELECT%s%s %s FROM %s%s%s%s%s;" % (
            with_cte,
            dst,
            limit,
            fields,
            tables,
            whr,
            grp,
            order,
            upd,
        )

    def left_join(self, val, query_env={}):
        # Left join must always have an ON clause
        if not isinstance(val, str):
            val = self.expand(val, query_env=query_env)
        return "LEFT OUTER JOIN %s" % val

    def random(self):
        return "NEWID()"

    def cast(self, first, second, query_env={}):
        # apparently no cast necessary in MSSQL
        return first

    def _mssql_like_normalizer(self, term):
        term = term.replace("[", "[[]")
        return term

    def _like_escaper_default(self, term):
        if isinstance(term, Expression):
            return term
        return self._mssql_like_normalizer(
            super(MSSQLDialect, self)._like_escaper_default(term)
        )

    def concat(self, *items, **kwargs):
        query_env = kwargs.get("query_env", {})
        tmp = (self.expand(x, "string", query_env=query_env) for x in items)
        return "(%s)" % " + ".join(tmp)

    def regexp(self, first, second, match_parameter=None, query_env={}):
        second = self.expand(second, "string", query_env=query_env)
        second = second.replace("\\", "\\\\")
        second = second.replace(r"%", r"\%").replace("*", "%").replace(".", "_")
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first, query_env=query_env),
            second,
        )

    def extract(self, first, what, query_env={}):
        return "DATEPART(%s,%s)" % (what, self.expand(first, query_env=query_env))

    def epoch(self, val, query_env={}):
        return "DATEDIFF(second, '1970-01-01 00:00:00', %s)" % self.expand(
            val, query_env=query_env
        )

    def length(self, val, query_env={}):
        return "LEN(%s)" % self.expand(val, query_env=query_env)

    def aggregate(self, first, what, query_env={}):
        if what == "LENGTH":
            what = "LEN"
        return super(MSSQLDialect, self).aggregate(first, what, query_env)

    @property
    def allow_null(self):
        return " NULL"

    def substring(self, field, parameters, query_env={}):
        return "SUBSTRING(%s,%s,%s)" % (
            self.expand(field, query_env=query_env),
            parameters[0],
            parameters[1],
        )

    def primary_key(self, key):
        return "PRIMARY KEY CLUSTERED (%s)" % key

    def concat_add(self, tablename):
        return "; ALTER TABLE %s ADD " % tablename

    def drop_index(self, name, table, if_exists=False):
        return "DROP INDEX %s ON %s;" % (self.quote(name), table._rname)

    def st_astext(self, first, query_env={}):
        return "%s.STAsText()" % self.expand(first, query_env=query_env)

    def st_contains(self, first, second, query_env={}):
        return "%s.STContains(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_distance(self, first, second, query_env={}):
        return "%s.STDistance(%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_equals(self, first, second, query_env={}):
        return "%s.STEquals(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_intersects(self, first, second, query_env={}):
        return "%s.STIntersects(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_overlaps(self, first, second, query_env={}):
        return "%s.STOverlaps(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_touches(self, first, second, query_env={}):
        return "%s.STTouches(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_within(self, first, second, query_env={}):
        return "%s.STWithin(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )


@dialects.register_for(MSSQLN)
class MSSQLNDialect(MSSQLDialect):
    @sqltype_for("string")
    def type_string(self):
        return "NVARCHAR(%(length)s)"

    @sqltype_for("text")
    def type_text(self):
        return "NTEXT"

    def ilike(self, first, second, escape=None, query_env={}):
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env).lower()
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        if second.startswith("n'"):
            second = "N'" + second[2:]
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.lower(first, query_env),
            second,
            escape,
        )


@dialects.register_for(MSSQL3)
class MSSQL3Dialect(MSSQLDialect):
    @sqltype_for("text")
    def type_text(self):
        return "VARCHAR(MAX)"

    @sqltype_for("time")
    def type_time(self):
        return "TIME(7)"

    def _rebuild_select_for_limit(
        self, fields, tables, dst, whr, grp, order, lmin, lmax
    ):
        f_outer = ["f_%s" % i for i in range(len(fields.split(",")))]
        f_inner = [field for field in fields.split(", ")]
        f_iproxy = ", ".join([self._as(o, n) for (o, n) in zip(f_inner, f_outer)])
        f_oproxy = ", ".join(f_outer)
        interp = (
            "SELECT%s %s FROM ("
            + "SELECT%s ROW_NUMBER() OVER (%s) AS w_row, %s FROM %s%s%s)"
            + " TMP WHERE w_row BETWEEN %i and %i;"
        )
        return interp % (
            dst,
            f_oproxy,
            dst,
            order,
            f_iproxy,
            tables,
            whr,
            grp,
            lmin,
            lmax,
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
        with_cte=None,  # ['recursive' | '', sql]
    ):
        dst, whr, grp, order, limit, offset, upd = "", "", "", "", "", "", ""
        if distinct is True:
            dst = " DISTINCT"
        elif distinct:
            dst = " DISTINCT ON (%s)" % distinct
        if where:
            whr = " %s" % self.where(where)
        if groupby:
            grp = " GROUP BY %s" % groupby
            if having:
                grp += " HAVING %s" % having
        if orderby:
            order = " ORDER BY %s" % orderby
        if limitby:
            (lmin, lmax) = limitby
            if lmin == 0:
                dst += " TOP %i" % lmax
            else:
                return self._rebuild_select_for_limit(
                    fields, tables, dst, whr, grp, order, lmin, lmax
                )
        if for_update:
            upd = " FOR UPDATE"

        if with_cte:
            recursive, cte = with_cte
            recursive = " RECURSIVE" if recursive else ""
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

        return "%sSELECT%s %s FROM %s%s%s%s%s%s%s;" % (
            with_cte,
            dst,
            fields,
            tables,
            whr,
            grp,
            order,
            limit,
            offset,
            upd,
        )


@dialects.register_for(MSSQL4)
class MSSQL4Dialect(MSSQL3Dialect):
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
        with_cte=None,  # ['recursive' | '', sql]
    ):
        dst, whr, grp, order, limit, offset, upd = "", "", "", "", "", "", ""
        if distinct is True:
            dst = " DISTINCT"
        elif distinct:
            dst = " DISTINCT ON (%s)" % distinct
        if where:
            whr = " %s" % self.where(where)
        if groupby:
            grp = " GROUP BY %s" % groupby
            if having:
                grp += " HAVING %s" % having
        if orderby:
            order = " ORDER BY %s" % orderby
        if limitby:
            (lmin, lmax) = limitby
            if lmin == 0:
                dst += " TOP %i" % lmax
            else:
                if not order:
                    order = " ORDER BY %s" % self.random
                offset = " OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" % (
                    lmin,
                    (lmax - lmin),
                )
        if for_update:
            upd = " FOR UPDATE"

        if with_cte:
            recursive, cte = with_cte
            recursive = " RECURSIVE" if recursive else ""
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

        return "%sSELECT%s %s FROM %s%s%s%s%s%s%s;" % (
            with_cte,
            dst,
            fields,
            tables,
            whr,
            grp,
            order,
            limit,
            offset,
            upd,
        )


@dialects.register_for(MSSQL3N)
class MSSQL3NDialect(MSSQLNDialect, MSSQL3Dialect):
    @sqltype_for("text")
    def type_text(self):
        return "NVARCHAR(MAX)"


@dialects.register_for(MSSQL4N)
class MSSQL4NDialect(MSSQLNDialect, MSSQL4Dialect):
    @sqltype_for("text")
    def type_text(self):
        return "NVARCHAR(MAX)"


@dialects.register_for(Vertica)
class VerticaDialect(MSSQLDialect):
    dt_sep = " "

    @sqltype_for("boolean")
    def type_boolean(self):
        return "BOOLEAN"

    @sqltype_for("text")
    def type_text(self):
        return "BYTEA"

    @sqltype_for("json")
    def type_json(self):
        return self.types["string"]

    @sqltype_for("blob")
    def type_blob(self):
        return "BYTEA"

    @sqltype_for("double")
    def type_double(self):
        return "DOUBLE PRECISION"

    @sqltype_for("time")
    def type_time(self):
        return "TIME"

    @sqltype_for("id")
    def type_id(self):
        return "IDENTITY"

    @sqltype_for("reference")
    def type_reference(self):
        return "INT REFERENCES %(foreign_key)s ON DELETE %(on_delete_action)s"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return "BIGINT REFERENCES %(foreign_key)s ON DELETE" + " %(on_delete_action)s"

    def extract(self, first, what, query_env={}):
        return "DATE_PART('%s', TIMESTAMP %s)" % (
            what,
            self.expand(first, query_env=query_env),
        )

    def truncate(self, table, mode=""):
        if mode:
            mode = " %s" % mode
        return ["TRUNCATE %s%s;" % (table._rname, mode)]

    def select(self, *args, **kwargs):
        return SQLDialect.select(self, *args, **kwargs)


@dialects.register_for(Sybase)
class SybaseDialect(MSSQLDialect):
    @sqltype_for("string")
    def type_string(self):
        return "CHAR VARYING(%(length)s)"

    @sqltype_for("date")
    def type_date(self):
        return "DATETIME"

# ============================================================
# Representer
# ============================================================

from ..backend_base import before_type, repr_for_type, representers
from ..backend_base import JSONRepresenter, SQLRepresenter


@representers.register_for(MSSQL)
class MSSQLRepresenter(SQLRepresenter, JSONRepresenter):
    """MSSQL representer with GIS WKT → STGeomFromText conversion."""

    def _make_geoextra(self, field_type, srid):
        """Extract SRID from ``geometry(...)`` / ``geography(...)`` type strings."""
        geotype, params = field_type[:-1].split("(")
        if params:
            srid = params
        return {"srid": srid}

    @before_type("geometry")
    def geometry_extras(self, field_type):
        """Default geometry SRID is 0 (no projection)."""
        return self._make_geoextra(field_type, 0)

    @repr_for_type("geometry", adapt=False)
    def _geometry(self, value, srid):
        """Render WKT via ``geometry::STGeomFromText(...)``."""
        return "geometry::STGeomFromText('%s',%s)" % (value, srid)

    @before_type("geography")
    def geography_extras(self, field_type):
        """Default geography SRID is 4326 (WGS 84 lat/lon)."""
        return self._make_geoextra(field_type, 4326)

    @repr_for_type("geography", adapt=False)
    def _geography(self, value, srid):
        """Render WKT via ``geography::STGeomFromText(...)``."""
        # Argument order matches geometry: (value, srid). Previously
        # the args were swapped, producing invalid SQL.
        return "geography::STGeomFromText('%s',%s)" % (value, srid)

