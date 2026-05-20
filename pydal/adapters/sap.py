"""SAPDB / MaxDB adapter (experimental)."""

import re

from . import adapters
from .base import SQLAdapter


@adapters.register_for("sapdb")
class SAPDB(SQLAdapter):
    """
    SAPDB / MaxDB adapter.

    Driver: ``sapdb``. Sequences are explicit (``CREATE SEQUENCE ...``)
    and IDs are fetched via ``SELECT seq.NEXTVAL FROM DUAL``.

    URI shape: ``sapdb://user:pass@host/dbname``.
    """

    dbengine = "sapdb"
    drivers = ("sapdb",)

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])/(?P<db>[^?]+)$"
    )

    def _initialize_(self):
        """Parse the URI and populate ``driver_args`` for ``connector``."""
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
        db = m.group("db")
        self.driver_args.update(user=user, password=password, database=db, host=host)

    def connector(self):
        """Open a new SAPDB connection. Returns the connection."""
        # Previously this method discarded the return value of
        # ``driver.connect`` and returned ``None``, which broke pooling
        # for SAPDB.
        return self.driver.connect(**self.driver_args)

    def lastrowid(self, table):
        """Fetch the next IDENTITY via ``SELECT seq.NEXTVAL FROM dual``."""
        self.execute("select %s.NEXTVAL from dual" % table._sequence_name)
        return int(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        """Create the per-table sequence and wire it as the id column default."""
        self.execute("CREATE SEQUENCE %s;" % table._sequence_name)
        self.execute(
            "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s');"
            % (table._rname, table._id._rname, table._sequence_name)
        )
        self.execute(query)
