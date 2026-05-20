# -*- coding: utf-8 -*-
# pylint: disable=no-member

"""
DB-API connection pool mixed into ``BaseAdapter``.

Connection state (the connection object and its cursor) is kept on
``THREAD_LOCAL`` so multiple threads sharing an adapter don't trample
each other. The class-level ``POOLS`` dict maps a connection URI to
a free-list of reusable connections; pulling from the free-list
amortizes connect cost across requests.

Public surface (all consumed via composition into adapters):

* ``connection`` / ``get_connection(use_pool=True)`` — lazy connect.
* ``cursor`` — current thread-local cursor.
* ``reset_cursor()`` — re-issue a cursor on the existing connection.
* ``close(action="commit", really=True)`` — commit/rollback + recycle.
* ``set_folder(folder)`` — set the per-thread default DB folder.
* ``close_all_instances(action)`` — clean shutdown for every pydal
  instance attached to the current thread.

Hooks subclasses may override:

* ``after_connection()`` — called every time a new connection opens.
* ``_after_first_connection()`` — called the first time only.
* ``test_connection()`` — sanity-ping (e.g. ``SELECT 1``).
"""

import os
from typing import Any, Callable, Dict, List, Optional, Union

from ._globals import GLOBAL_LOCKER, THREAD_LOCAL


class ConnectionPool:
    """Per-adapter thread-local connection management."""

    POOLS: Dict[str, List[Any]] = {}
    check_active_connection: bool = True

    def __init__(self):
        self._first_connection = False

    @property
    def _connection_uname_(self) -> str:
        """Per-pid, per-instance key for storing the connection on THREAD_LOCAL."""
        return "_pydal_connection_%s_%s" % (id(self), os.getpid())

    @property
    def _cursors_uname_(self) -> str:
        """Per-pid, per-instance key for storing the cursor on THREAD_LOCAL."""
        return "_pydal_cursor_%s_%s" % (id(self), os.getpid())

    @staticmethod
    def set_folder(folder: str) -> None:
        """
        Set the default folder for DAL migration files in this thread.

        DAL instances created without an explicit ``folder=`` will pick
        this value up at construction time.
        """
        THREAD_LOCAL._pydal_folder_ = folder

    @property
    def connection(self) -> Any:
        """Lazy property: return (or open) the connection for this thread."""
        return self.get_connection()

    def get_connection(self, use_pool: bool = True) -> Any:
        """
        Return a live connection for the current thread.

        Lookup order:

        1. The thread-local slot for this adapter — if set, return as-is.
        2. The pool (when ``pool_size > 0`` and ``use_pool``): pop free
           connections one at a time, accept the first that passes
           ``test_connection``.
        3. Otherwise open a fresh connection via ``connector()`` and
           run the after-connection hooks.
        """
        connection = getattr(THREAD_LOCAL, self._connection_uname_, None)
        if connection is not None:
            return connection

        # Try the pool.
        if use_pool and self.pool_size:
            try:
                GLOBAL_LOCKER.acquire()
                pool = ConnectionPool.POOLS.get(self.uri, [])
                ConnectionPool.POOLS[self.uri] = pool
                # Pop until we find a usable connection (or exhaust pool).
                while connection is None and pool:
                    connection = pool.pop()
                    try:
                        self.set_connection(connection, run_hooks=False)
                    except Exception:
                        connection = None
            finally:
                GLOBAL_LOCKER.release()

        # Still nothing — open fresh and run hooks.
        if connection is None:
            connection = self.connector()
            self.set_connection(connection, run_hooks=True)

        return connection

    def set_connection(self, connection: Any, run_hooks: bool = False) -> None:
        """
        Bind ``connection`` (or ``None``) into thread-local storage.

        When ``connection`` is non-None: also issue a cursor; run the
        hooks if requested; run ``test_connection`` if
        ``check_active_connection`` is True.
        """
        setattr(THREAD_LOCAL, self._connection_uname_, connection)
        if connection:
            setattr(THREAD_LOCAL, self._cursors_uname_, connection.cursor())
            if run_hooks:
                self.after_connection_hook()
            if self.check_active_connection:
                self.test_connection()
        else:
            setattr(THREAD_LOCAL, self._cursors_uname_, None)

    def reset_cursor(self) -> None:
        """Issue a fresh cursor on the existing connection (no reconnect)."""
        setattr(THREAD_LOCAL, self._cursors_uname_, self.connection.cursor())

    @property
    def cursor(self) -> Any:
        """The current thread-local cursor for this adapter."""
        return getattr(THREAD_LOCAL, self._cursors_uname_)

    def _clean_tlocals(self) -> None:
        """
        Drop the cursor and connection slots from thread-local storage.

        Called during DAL teardown; safe against absent attributes (an
        adapter that never connected won't have the slots).
        """
        for name in (self._cursors_uname_, self._connection_uname_):
            if hasattr(THREAD_LOCAL, name):
                delattr(THREAD_LOCAL, name)

    def close(
        self,
        action: Optional[Union[str, Callable]] = "commit",
        really: bool = True,
    ) -> None:
        """
        Wind down the current thread's connection.

        ``action`` is run before closing — typically ``"commit"`` or
        ``"rollback"`` (method names on this object) or a callable
        ``f(self)``. If ``action`` raises, the connection is treated
        as broken and dropped rather than recycled.

        ``really`` controls whether the underlying DB-API connection is
        actually closed. When pooling is enabled and there's room in
        the pool, the connection is recycled there instead, regardless
        of ``really``.
        """
        # If we never opened, nothing to do.
        if getattr(THREAD_LOCAL, self._connection_uname_, None) is None:
            return
        # Try the user-supplied action (commit/rollback).
        succeeded = True
        if action:
            try:
                if callable(action):
                    action(self)
                else:
                    getattr(self, action)()
            except Exception:
                # action failed — drop the connection.
                succeeded = False
        # Close the cursor unconditionally.
        self.cursor.close()
        # Recycle into pool if possible.
        if self.pool_size and succeeded:
            try:
                GLOBAL_LOCKER.acquire()
                pool = ConnectionPool.POOLS[self.uri]
                if len(pool) < int(self.pool_size):
                    pool.append(self.connection)
                    really = False
            finally:
                GLOBAL_LOCKER.release()
        # Actually close the DB-API connection when:
        # - the action raised
        # - no pool, or
        # - pool was full
        if really:
            try:
                self.close_connection()
            except Exception:
                pass
        # Always unset the thread-local slots.
        self.set_connection(None)

    @staticmethod
    def close_all_instances(action: Union[str, Callable]) -> None:
        """
        Close every pydal connection bound to the current thread.

        Used at process shutdown / between requests in long-running
        servers. ``action`` is forwarded to each adapter's ``close``;
        callable actions are also invoked once globally at the end.
        """
        dbs = getattr(THREAD_LOCAL, "_pydal_db_instances_", {}).items()
        for db_uid, db_group in dbs:
            for db in db_group:
                if hasattr(db, "_adapter"):
                    db._adapter.close(action)
        getattr(THREAD_LOCAL, "_pydal_db_instances_", {}).clear()
        getattr(THREAD_LOCAL, "_pydal_db_instances_zombie_", {}).clear()
        if callable(action):
            action(None)

    def _find_work_folder(self) -> None:
        """Pick up ``set_folder``'s value from THREAD_LOCAL into ``self.folder``."""
        self.folder = getattr(THREAD_LOCAL, "_pydal_folder_", "")

    def after_connection_hook(self) -> None:
        """
        Run the first-connection and per-connection hooks.

        Order: ``_after_first_connection`` (once), the user-supplied
        ``_after_connection`` callable (if any), then the adapter's
        ``after_connection``.
        """
        if not self._first_connection:
            self._after_first_connection()
            self._first_connection = True
        if callable(self._after_connection):
            self._after_connection(self)
        self.after_connection()

    def after_connection(self) -> None:
        """Per-connection hook — overridden by adapter subclasses."""

    def _after_first_connection(self) -> None:
        """First-connection-only hook — overridden by adapter subclasses."""

    def reconnect(self) -> None:
        """
        Close the current connection and re-open a fresh one.

        Legacy helper retained for backward compatibility; equivalent to
        ``self.close(); self.get_connection()``.
        """
        self.close()
        self.get_connection()
