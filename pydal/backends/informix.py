"""Backend module for informix."""

# ============================================================
# Adapter
# ============================================================

import re

from ..backend_base import adapters, with_connection_or_raise
from ..backend_base import SQLAdapter


@adapters.register_for("informix")
class Informix(SQLAdapter):
    """
    Informix adapter using the ``informixdb`` driver.

    URI shape: ``informix://user:password@host/dbname``. Strips the
    trailing ``;`` from emitted SQL since informixdb rejects it.
    ``dbms_version`` is captured on the first connection so the
    dialect can branch on ``SKIP`` / ``FIRST`` support (Informix 9+).
    """

    dbengine = "informix"
    drivers = ("informixdb",)

    # ``self.REGEX_URI`` was referenced but never defined on the class
    # — connecting would raise AttributeError. Match the same shape as
    # the MySQL / MSSQL adapters.
    REGEX_URI = re.compile(
        r"^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])/(?P<db>[^?]+)$"
    )

    def _initialize_(self):
        """Parse URI components and stash a DSN for ``connector``."""
        super()._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = self.REGEX_URI.match(ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        if not user:
            raise SyntaxError("User required")
        password = self.credential_decoder(m.group("password"))
        if not password:
            password = ""
        host = m.group("host")
        if not host:
            raise SyntaxError("Host name required")
        db = m.group("db")
        if not db:
            raise SyntaxError("Database name required")
        self.dsn = "%s@%s" % (db, host)
        self.driver_args.update(user=user, password=password)
        self.get_connection()

    def connector(self):
        """Open a new Informix connection."""
        return self.driver.connect(self.dsn, **self.driver_args)

    def _after_first_connection(self):
        """Capture the major DBMS version (used by the dialect for syntax branches)."""
        self.dbms_version = int(self.connection.dbms_version.split(".")[0])

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        """Execute a statement, stripping the trailing ``;`` that informixdb rejects."""
        command = self.filter_sql_command(args[0])
        if command[-1:] == ";":
            command = command[:-1]
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def test_connection(self):
        """Ping the connection with ``SELECT COUNT(*) FROM systables;``."""
        self.execute("SELECT COUNT(*) FROM systables;")

    def lastrowid(self, table):
        """Return ``cursor.sqlerrd[1]`` — Informix-specific ID-of-last-row."""
        return self.cursor.sqlerrd[1]


@adapters.register_for("informix-se")
class InformixSE(Informix):
    """
    Informix Standard Edition — no ``SKIP``/``FIRST``, so slicing must
    happen client-side in ``rowslice``.
    """

    def rowslice(self, rows, minimum=0, maximum=None):
        """Slice the result set in Python since SE has no LIMIT/OFFSET."""
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]

# ============================================================
# Dialect
# ============================================================

from ..backend_base import dialects, sqltype_for
from .firebird import FireBirdDialect


@dialects.register_for(Informix)
class InformixDialect(FireBirdDialect):
    """
    Informix dialect (versions 9 and up).

    Synthetic IDs use ``SERIAL`` / ``BIGSERIAL``. Pagination uses
    ``SKIP n FIRST m`` placed between ``SELECT`` and the field list
    (gated by ``adapter.dbms_version``). Inherits from FireBird for
    shared trigger/sequence semantics.
    """

    @sqltype_for("id")
    def type_id(self):
        """Informix synthetic ID: ``SERIAL``."""
        return "SERIAL"

    @sqltype_for("big-id")
    def type_big_id(self):
        """Informix 64-bit ID: ``BIGSERIAL``."""
        return "BIGSERIAL"

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            "REFERENCES %(foreign_key)s ON DELETE %(on_delete_action)s "
            + "CONSTRAINT FK_%(table_name)s_%(field_name)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            "FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_table)s"
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s "
            + "CONSTRAINT TFK_%(table_name)s_%(field_name)s"
        )

    @property
    def random(self):
        return "Random()"

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
        with_cte=None,
    ):
        """
        Informix SELECT. ``SKIP n FIRST m`` between ``SELECT`` and the
        field list (no ``LIMIT``/``OFFSET``). ``with_cte`` accepted for
        signature compatibility with the base SQLDialect.
        """
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
            fetch_amt = lmax - lmin
            if lmin and self.adapter.dbms_version >= 10:
                offset = " SKIP %i" % lmin
            if fetch_amt and self.adapter.dbms_version >= 9:
                limit = " FIRST %i" % fetch_amt
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s%s%s %s FROM %s%s%s%s%s;" % (
            dst,
            offset,
            limit,
            fields,
            tables,
            whr,
            grp,
            order,
            upd,
        )


@dialects.register_for(InformixSE)
class InformixSEDialect(InformixDialect):
    """
    Informix Standard Edition dialect.

    SE has no ``SKIP``/``FIRST`` — ``limitby`` is silently dropped at
    the SQL level and slicing happens client-side via the adapter's
    ``rowslice``.
    """

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
        with_cte=None,
    ):
        """
        Informix Standard Edition SELECT — no ``SKIP``/``FIRST`` support;
        ``limitby`` is silently dropped.
        """
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
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s %s FROM %s%s%s%s%s%s%s;" % (
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

# ============================================================
# Representer
# ============================================================

import datetime

from ..backend_base import representers
from ..backend_base import SQLRepresenter


@representers.register_for(Informix)
class InformixRepresenter(SQLRepresenter):
    """Informix-specific date/datetime rendering using ``to_date()``."""

    def exceptions(self, obj, field_type):
        """Render dates/datetimes via Informix's ``to_date(value, format)``."""
        if field_type == "date":
            if isinstance(obj, (datetime.date, datetime.datetime)):
                obj = obj.isoformat()[:10]
            else:
                obj = str(obj)
            return "to_date('%s','%%Y-%%m-%%d')" % obj
        if field_type == "datetime":
            if isinstance(obj, datetime.datetime):
                obj = obj.isoformat()[:19].replace("T", " ")
            elif isinstance(obj, datetime.date):
                obj = obj.isoformat()[:10] + " 00:00:00"
            else:
                obj = str(obj)
            return "to_date('%s','%%Y-%%m-%%d %%H:%%M:%%S')" % obj
        return None

