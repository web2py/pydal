# -*- coding: utf-8 -*-
import os
from ._compat import itervalues
from ._globals import GLOBAL_LOCKER, THREAD_LOCAL
from ._load import OrderedDict
from .helpers._internals import Cursor


class ConnectionPool(object):
    POOLS = {}
    check_active_connection = True

    def __init__(self):
        _iid_ = str(id(self))
        self._connection_thname_ = '_pydal_connection_' + _iid_ + '_'
        self._cursors_thname_ = '_pydal_cursors_' + _iid_ + '_'

    @property
    def _pid_(self):
        return str(os.getpid())

    @property
    def _connection_uname_(self):
        return self._connection_thname_ + self._pid_

    @property
    def _cursors_uname_(self):
        return self._cursors_thname_ + self._pid_

    @staticmethod
    def set_folder(folder):
        THREAD_LOCAL._pydal_folder_ = folder

    @property
    def connection(self):
        return getattr(THREAD_LOCAL, self._connection_uname_)

    @connection.setter
    def connection(self, val):
        setattr(THREAD_LOCAL, self._connection_uname_, val)
        self._clean_cursors()
        if val is not None:
            self._build_cursor()

    def _clean_cursors(self):
        setattr(THREAD_LOCAL, self._cursors_uname_, OrderedDict())

    @property
    def cursors(self):
        return getattr(THREAD_LOCAL, self._cursors_uname_)

    def _build_cursor(self):
        rv = Cursor(self.connection)
        self.cursors[id(rv.cursor)] = rv
        return rv

    def _get_or_build_free_cursor(self):
        for handler in itervalues(self.cursors):
            if handler.available:
                return handler
        return self._build_cursor()

    @property
    def cursor(self):
        return self._get_or_build_free_cursor().cursor

    def lock_cursor(self, cursor):
        self.cursors[id(cursor)].lock()

    def release_cursor(self, cursor):
        self.cursors[id(cursor)].release()

    def close_cursor(self, cursor):
        cursor.close()
        del self.cursors[id(cursor)]

    def _clean_tlocals(self):
        delattr(THREAD_LOCAL, self._cursors_uname_)
        delattr(THREAD_LOCAL, self._connection_uname_)

    def close(self, action='commit', really=True):
        #: if we have an action (commit, rollback), try to execute it
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
        #: if we have pools, we should recycle the connection (but only when
        #  we succeded in `action`, if any and `len(pool)` is good)
        if self.pool_size and succeeded:
            GLOBAL_LOCKER.acquire()
            pool = ConnectionPool.POOLS[self.uri]
            if len(pool) < self.pool_size:
                pool.append(self.connection)
                really = False
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
        self.connection = None

    @staticmethod
    def close_all_instances(action):
        """ to close cleanly databases in a multithreaded environment """
        dbs = getattr(THREAD_LOCAL, '_pydal_db_instances_', {}).items()
        for db_uid, db_group in dbs:
            for db in db_group:
                if hasattr(db, '_adapter'):
                    db._adapter.close(action)
        getattr(THREAD_LOCAL, '_pydal_db_instances_', {}).clear()
        getattr(THREAD_LOCAL, '_pydal_db_instances_zombie_', {}).clear()
        if callable(action):
            action(None)
        return

    def _find_work_folder(self):
        self.folder = getattr(THREAD_LOCAL, '_pydal_folder_', '')

    def after_connection_hook(self):
        """Hook for the after_connection parameter"""
        if callable(self._after_connection):
            self._after_connection(self)
        self.after_connection()

    def after_connection(self):
        #this it is supposed to be overloaded by adapters
        pass

    def reconnect(self):
        """
        Defines: `self.connection` and `self.cursor`
        if `self.pool_size>0` it will try pull the connection from the pool
        if the connection is not active (closed by db server) it will loop
        if not `self.pool_size` or no active connections in pool makes a new one
        """
        if getattr(THREAD_LOCAL, self._connection_uname_, None) is not None:
            return

        if not self.pool_size:
            self.connection = self.connector()
            self.after_connection_hook()
        else:
            uri = self.uri
            POOLS = ConnectionPool.POOLS
            while True:
                GLOBAL_LOCKER.acquire()
                if uri not in POOLS:
                    POOLS[uri] = []
                if POOLS[uri]:
                    self.connection = POOLS[uri].pop()
                    GLOBAL_LOCKER.release()
                    try:
                        if self.check_active_connection:
                            self.test_connection()
                        break
                    except:
                        pass
                else:
                    GLOBAL_LOCKER.release()
                    self.connection = self.connector()
                    self.after_connection_hook()
                    break
