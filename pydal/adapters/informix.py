"""Informix adapter (uses ``informixdb``)."""

import re

from . import adapters, with_connection_or_raise
from .base import SQLAdapter


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
