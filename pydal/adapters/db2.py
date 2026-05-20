"""DB2 adapters (``ibm_db_dbi`` and ``pyodbc`` variants)."""

from . import adapters, with_connection_or_raise
from .base import SQLAdapter


class DB2(SQLAdapter):
    """
    Base DB2 adapter — shared logic for both driver variants.

    Trailing ``;`` is stripped from statements (DB2 rejects it).
    ``lastrowid`` reads ``IDENTITY_VAL_LOCAL()`` from the table.
    Slicing happens client-side in ``rowslice`` since DB2 has no
    ``LIMIT``/``OFFSET`` in older releases.
    """

    dbengine = "db2"

    def _initialize_(self):
        """Stash the raw connection-string portion of the URI."""
        super()._initialize_()
        self.ruri = self.uri.split("://", 1)[1]

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        """
        Execute a statement, stripping the trailing ``;`` DB2 rejects.

        Accepts ``placeholders=`` in kwargs (pydal convention) and
        appends them as a positional arg.
        """
        args = list(args)
        command = self.filter_sql_command(args[0])
        if command[-1:] == ";":
            command = command[:-1]
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        # ``args`` was previously a tuple; ``.append`` would raise
        # AttributeError whenever placeholders were supplied.
        if kwargs.get("placeholders"):
            args.append(kwargs["placeholders"])
            del kwargs["placeholders"]
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def lastrowid(self, table):
        """Return the most-recent IDENTITY via ``IDENTITY_VAL_LOCAL()``."""
        self.execute(
            "SELECT DISTINCT IDENTITY_VAL_LOCAL() FROM %s;" % table._rname
            if table._rname
            else table
        )
        return int(self.cursor.fetchone()[0])

    def rowslice(self, rows, minimum=0, maximum=None):
        """Client-side slice — DB2 ``LIMIT``/``OFFSET`` isn't universal."""
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]

    def test_connection(self):
        """Ping with the standard DB2 ``SYSIBM.SYSDUMMY1`` smoke query."""
        self.execute("select * from sysibm.sysdummy1")


@adapters.register_for("db2:ibm_db_dbi")
class DB2IBM(DB2):
    """DB2 via the IBM-supplied ``ibm_db_dbi`` Python driver."""

    drivers = ("ibm_db_dbi",)

    def connector(self):
        """Parse ``DSN=...;UID=...;PWD=...`` and open the connection."""
        uriparts = self.ruri.split(";")
        cnxn = {}
        for var in uriparts:
            v = var.split("=")
            cnxn[v[0].lower()] = v[1]
        return self.driver.connect(
            cnxn["dsn"], cnxn["uid"], cnxn["pwd"], **self.driver_args
        )


@adapters.register_for("db2:pyodbc")
class DB2Pyodbc(DB2):
    """DB2 via pyodbc. Forces UTF-8 client encoding."""

    drivers = ("pyodbc",)

    def connector(self):
        """Open the pyodbc connection and force UTF-8 client encoding."""
        conn = self.driver.connect(self.ruri, **self.driver_args)
        conn.setencoding(encoding="utf-8")
        return conn
