"""Teradata adapter (uses pyodbc; experimental)."""

from . import adapters
from .base import SQLAdapter


@adapters.register_for("teradata")
class Teradata(SQLAdapter):
    """
    Teradata adapter via pyodbc.

    Teradata has two quirks worth noting:

    * Cursors must be explicitly closed before the connection, otherwise
      ``SQL_ACTIVE_STATEMENTS`` limit errors accumulate over time.
    * IDENTITY columns are not retrievable through ``lastrowid`` and
      aren't sequential anyway — ``lastrowid`` always returns 1.

    Driver: ``pyodbc``.
    """

    dbengine = ""
    drivers = ("pyodbc",)

    def _initialize_(self):
        """Stash the post-``://`` portion of the URI as the raw DSN string."""
        super()._initialize_()
        self.ruri = self.uri.split("://", 1)[1]

    def connector(self):
        """Open a new pyodbc connection using ``self.ruri`` as the DSN string."""
        return self.driver.connect(self.ruri, **self.driver_args)

    def close(self):
        """Close the cursor first; Teradata doesn't do this implicitly."""
        self.cursor.close()
        super().close()

    def lastrowid(self, table):
        """Always returns 1 — Teradata can't retrieve IDENTITY values."""
        return 1
