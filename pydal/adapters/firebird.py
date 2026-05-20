"""FireBird / FireBird Embedded adapters."""

import re

from . import adapters
from .base import SQLAdapter


@adapters.register_for("firebird")
class FireBird(SQLAdapter):
    """
    FireBird adapter.

    Drivers (tried in order): ``kinterbasdb``, ``firebirdsql``,
    ``fdb``, ``pyodbc``.

    Distributed transactions are supported; ALTER TABLE auto-commits
    (FireBird DDL is implicitly transactional).

    URI shape: ``firebird://user:pass@host:port/db?set_encoding=UTF8``.
    Synthetic IDs come from per-table generators; an INSERT trigger
    populates the id column via ``gen_id()`` when not supplied.
    """

    dbengine = "firebird"
    drivers = ("kinterbasdb", "firebirdsql", "fdb", "pyodbc")

    support_distributed_transaction = True
    commit_on_alter_table = True

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?"
        "/(?P<db>[^?]+)"
        r"(\?set_encoding=(?P<charset>\w+))?$"
    )

    def _initialize_(self):
        """Parse the URI, build the ``host/port:db`` DSN, stash in driver_args."""
        super()._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        if password is None:
            password = ""
        host = m.group("host")
        db = self.credential_decoder(m.group("db"))
        port = int(m.group("port") or 3050)
        charset = m.group("charset") or "UTF8"
        self.driver_args.update(
            dsn="%s/%s:%s" % (host, port, db),
            user=user,
            password=password,
            charset=charset,
        )

    def connector(self):
        """Open a new FireBird connection using the stashed driver_args."""
        return self.driver.connect(**self.driver_args)

    def test_connection(self):
        """Ping with the standard ``SELECT current_timestamp FROM RDB$DATABASE``."""
        self.execute("select current_timestamp from RDB$DATABASE")

    def lastrowid(self, table):
        """Fetch the current generator value for the table's sequence."""
        sequence_name = table._sequence_name
        self.execute("SELECT gen_id(%s, 0) FROM rdb$database" % sequence_name)
        return int(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        """
        Create the table, its generator, and the BEFORE-INSERT trigger
        that auto-populates the id column when not supplied.
        """
        tablename = table._rname
        sequence_name = table._sequence_name
        trigger_name = table._trigger_name
        self.execute(query)
        self.execute("create generator %s;" % sequence_name)
        self.execute("set generator %s to 0;" % sequence_name)
        qid = self.dialect.quote_template % "id"
        self.execute(
            "create trigger %s for %s active before insert position 0 as\n"
            "begin\nif(new.%s is null) then new.%s = gen_id(%s, 1);\n"
            "end;" % (trigger_name, tablename, qid, qid, sequence_name)
        )


@adapters.register_for("firebird_embedded")
class FireBirdEmbedded(FireBird):
    """
    FireBird Embedded variant — local-file connection, no host:port.

    URI shape: ``firebird_embedded://user:pass@/path/to/db?set_encoding=UTF8``.
    """

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<path>[^?]+)(\?set_encoding=(?P<charset>\w+))?$"
    )

    def _initialize_(self):
        """
        Parse the embedded-URI form and prepare driver_args.

        Deliberately skips ``FireBird._initialize_`` (which would parse
        the host/port URI) and goes straight to ``SQLAdapter`` — hence
        the explicit ``super(FireBird, self)._initialize_()`` rather
        than ``super()._initialize_()``.
        """
        super(FireBird, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        if password is None:
            password = ""
        pathdb = m.group("path")
        charset = m.group("charset") or "UTF8"
        self.driver_args.update(
            host="", database=pathdb, user=user, password=password, charset=charset
        )
