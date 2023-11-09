# -*- coding: utf-8 -*-
# pylint: disable=no-member

import os
import threading

from ._compat import itervalues
from ._globals import GLOBAL_LOCKER, THREAD_LOCAL
from ._load import OrderedDict


class ConnectionPool(object):
    POOLS = {}
    check_active_connection = True

    def __init__(self):
        self._first_connection = False

    @property
    def _connection_uname_(self):
        return "_pydal_connection_%s_%s" % (id(self), os.getpid())

    @property
    def _cursors_uname_(self):
        return "_pydal_cursor_%s_%s" % (id(self), os.getpid())

    @staticmethod
    def set_folder(folder):
        THREAD_LOCAL._pydal_folder_ = folder

    @property
    def connection(self):
        return self.get_connection()

    def get_connection(self, use_pool=True):
        """
        if `self.pool_size>0` it will try pull the connection from the pool
        if the connection is not active (closed by db server) it will loop
        if not `self.pool_size` or no active connections in pool makes a new one
        """
        # check we we have a connection for this process/thread/object id
        connection = getattr(THREAD_LOCAL, self._connection_uname_, None)
        # if so, return it
        if connection is not None:
            return connection

        # if not and we have a pool
        if use_pool and self.pool_size:
            try:
                GLOBAL_LOCKER.acquire()
                pool = ConnectionPool.POOLS.get(self.uri, [])
                ConnectionPool.POOLS[self.uri] = pool
                # pop from the pool until we find a valid connection
                while connection is None and pool:
                    connection = pool.pop()
                    try:
                        self.set_connection(connection, run_hooks=False)
                    except:
                        connection = None
            finally:
                GLOBAL_LOCKER.release()

        # if still no connection, make a new one and run the hooks
        # note we serialize actual connections to protect hooks
        if connection is None:
            connection = self.connector()
            self.set_connection(connection, run_hooks=True)

        return connection

    def set_connection(self, connection, run_hooks=False):
        # store the connection in the thread local object and run hooks (optional)
        setattr(THREAD_LOCAL, self._connection_uname_, connection)
        if connection:
            setattr(THREAD_LOCAL, self._cursors_uname_, connection.cursor())
            # run hooks
            if run_hooks:
                self.after_connection_hook()
            # some times we want to check the connection is still good
            if self.check_active_connection:
                self.test_connection()
        else:
            setattr(THREAD_LOCAL, self._cursors_uname_, None)

    def reset_cursor(self):
        """get a new cursor for the existing connection"""
        setattr(THREAD_LOCAL, self._cursors_uname_, self.connection.cursor())

    @property
    def cursor(self):
        """retrieve the cursor of the connection"""
        return getattr(THREAD_LOCAL, self._cursors_uname_)

    def _clean_tlocals(self):
        """delete cusor and connection from the thead local"""
        delattr(THREAD_LOCAL, self._cursors_uname_)
        delattr(THREAD_LOCAL, self._connection_uname_)

    def close(self, action="commit", really=True):
        """if we have an action (commit, rollback), try to execute it"""
        # if the connection was never established, nothing to do
        if getattr(THREAD_LOCAL, self._connection_uname_, None) is None:
            return
        # try commit or rollback
        succeeded = True
        if action:
            try:
                if callable(action):
                    action(self)
                else:
                    getattr(self, action)()
            except:
                #: connection had some problems, we want to drop it
                succeeded = False
        # close the cursor
        self.cursor.close()
        # if we have pools, we should recycle the connection (but only when
        # we succeded in `action`, if any and `len(pool)` is good)
        if self.pool_size and succeeded:
            try:
                GLOBAL_LOCKER.acquire()
                pool = ConnectionPool.POOLS[self.uri]
                if len(pool) < int(self.pool_size):
                    pool.append(self.connection)
                    really = False
            finally:
                GLOBAL_LOCKER.release()
        #: closing the connection when we `really` want to, in particular:
        #    - when we had an exception running `action`
        #    - when we don't have pools
        #    - when we have pools but they're full
        if really:
            try:
                self.close_connection()
            except:
                pass
        #: always unset `connection` attribute
        self.set_connection(None)

    @staticmethod
    def close_all_instances(action):
        """to close cleanly databases in a multithreaded environment"""
        dbs = getattr(THREAD_LOCAL, "_pydal_db_instances_", {}).items()
        for db_uid, db_group in dbs:
            for db in db_group:
                if hasattr(db, "_adapter"):
                    db._adapter.close(action)
        getattr(THREAD_LOCAL, "_pydal_db_instances_", {}).clear()
        getattr(THREAD_LOCAL, "_pydal_db_instances_zombie_", {}).clear()
        if callable(action):
            action(None)

    def _find_work_folder(self):
        self.folder = getattr(THREAD_LOCAL, "_pydal_folder_", "")

    def after_connection_hook(self):
        """Hook for the after_connection parameter"""
        # some work must be done on first connection only
        if not self._first_connection:
            self._after_first_connection()
            self._first_connection = True
        # handle user specified hooks if present
        if callable(self._after_connection):
            self._after_connection(self)
        # handle global adapter hooks
        self.after_connection()

    def after_connection(self):
        # this it is supposed to be overloaded by adapters
        pass

    def _after_first_connection(self):
        """called only after first connection"""
        pass

    def reconnect(self):
        """legacy method - no longer needed"""
        self.close()
        self.get_connection()
