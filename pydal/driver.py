"""
Layer 4: I/O driver.

A thin wrapper around the DB-API connection and cursor. The driver
runs SQL, manages transactions, and pulls last-row-id from the cursor.
It knows about ParamSQL (so it can forward bound parameters to
``cursor.execute``) but otherwise does not know SQL syntax, dialects,
compilers, or pydal schema concepts — those live in the layers above.

Design choices for simplicity:

* One class, no subclasses. Backend differences (sqlite vs postgres
  vs ...) ride on the underlying DB-API module already exposed as
  ``adapter.driver``, which the Driver borrows.
* No connection pooling here — ``ConnectionPool`` (mixed into the
  adapter) still owns that. The driver borrows ``adapter.cursor`` and
  ``adapter.connection`` at call time.
* No type adaptation here. Value encoding is the compiler's concern
  for inline literals, and the DB-API does the rest for bound params.

The adapter holds a Driver as ``self.driver_io`` and delegates its
existing ``execute`` / ``commit`` / ``rollback`` / ``lastrowid``
methods to it.
"""

from __future__ import annotations


class Driver:
    """Connection-level operations for an adapter."""

    def __init__(self, adapter):
        # Held by reference, not owned. The adapter owns connection
        # pooling, find_driver(), and pre-execute handler setup; we
        # just call into them for the actual I/O.
        self._adapter = adapter

    # -- execution ----------------------------------------------------

    def execute(self, sql, *rest, **kwargs):
        """
        Run a SQL statement through the adapter's current cursor.

        If ``sql`` is a ParamSQL (compiler output with bound params) and
        the caller hasn't supplied explicit positional params, the
        params attached to the SQL flow through to ``cursor.execute``.

        Returns whatever the DB-API cursor returns (typically ``None``).
        """
        adapter = self._adapter
        command = adapter.filter_sql_command(sql)
        if not rest:
            attached = getattr(command, "params", None)
            if attached:
                rest = (attached,)
        handlers = adapter._build_handlers_for_execution()
        for h in handlers:
            h.before_execute(command)
        rv = adapter.cursor.execute(command, *rest, **kwargs)
        for h in handlers:
            h.after_execute(command)
        return rv

    # -- transactions -------------------------------------------------

    def commit(self):
        """Commit the current transaction on the adapter's connection."""
        return self._adapter.connection.commit()

    def rollback(self):
        """Roll back the current transaction on the adapter's connection."""
        return self._adapter.connection.rollback()

    # -- result inspection --------------------------------------------

    def lastrowid(self):
        """Return the auto-increment id produced by the most recent INSERT."""
        return self._adapter.cursor.lastrowid


__all__ = ["Driver"]
