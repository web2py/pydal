# -*- coding: utf-8 -*-
# pylint: disable=no-member

"""
pydal.base — the ``DAL`` class.

A DAL ties together a connection URI, an adapter, a dialect, and a
collection of tables. It's the only entry point most users need.

Example::

    from pydal import DAL, Field

    db = DAL("sqlite://storage.sqlite")
    db.define_table("person", Field("name"))
    db.person.insert(name="Alice")
    for row in db(db.person.name.startswith("A")).select():
        print(row.id, row.name)
    db.commit()

Supported URI prefixes (driver may need to be installed separately):

* ``sqlite://`` / ``spatialite://`` / ``sqlite:memory``
* ``mysql://``
* ``postgres://`` (also ``postgres:psycopg2://``, ``postgres:pg8000://``)
* ``mssql://``, ``mssql2``, ``mssql3``, ``mssql4`` — pagination variants
* ``mssqlpython``, ``pytds``, ``pymssql`` — alternate MS SQL drivers
* ``oracle://``
* ``firebird://`` / ``firebird_embedded://``
* ``db2:ibm_db_dbi://`` / ``db2:pyodbc://``
* ``informix://`` / ``informixu://``
* ``ingres://``
* ``teradata://``
* ``snowflake://``
* ``mongodb://``
* ``firestore`` / ``google:sql``
* ``imap://`` (experimental)
* ``jdbc:<engine>://`` (Jython only)

The DAL constructor accepts a single URI or a list/tuple of URIs.
With multiple URIs it tries each in order and keeps the first
connection that succeeds — useful for replication / failover.
"""

import contextlib
import glob
import logging
import os
import socket
import threading
import time
import traceback
import urllib

import copyreg
import pickle
from os.path import join as pjoin
from urllib.parse import unquote

from ._globals import DEFAULT, GLOBAL_LOCKER, THREAD_LOCAL
from .utils import hashlib_md5
from ._load import OrderedDict
from .backend_base import BaseAdapter, NullAdapter
from .default_validators import default_validators
from .helpers.classes import (
    BasicStorage,
    RecordDeleter,
    RecordUpdater,
    Serializable,
    SQLCallableList,
    TimingHandler,
)
from .helpers.methods import (
    auto_represent,
    auto_validators,
    hide_password,
    smart_query,
    uuidstr,
)
from .helpers.regex import REGEX_DBNAME, REGEX_PYTHON_KEYWORDS
from .helpers.rest import RestParser
from .helpers.serializers import serializers
from .objects import Field, Row, Rows, Set, Table

TABLE_ARGS = set(
    (
        "migrate",
        "primarykey",
        "fake_migrate",
        "format",
        "redefine",
        "singular",
        "plural",
        "trigger_name",
        "sequence_name",
        "fields",
        "common_filter",
        "table_class",
        "on_define",
        "rname",
    )
)


class MetaDAL(type):
    """
    Metaclass for ``DAL`` that intercepts a fixed set of constructor
    kwargs and applies them as class-level overrides before the
    instance is created.

    The intercepted names — ``logger``, ``representers``,
    ``serializers``, ``uuid``, ``validators``, ``validators_method``,
    ``Table``, ``Row`` — are pydal customization hooks. Passing one of
    them to ``DAL(...)`` is equivalent to subclassing DAL and setting
    the same class attribute.
    """

    def __call__(cls, *args, **kwargs):
        intercepts = [
            "logger",
            "representers",
            "serializers",
            "uuid",
            "validators",
            "validators_method",
            "Table",
            "Row",
        ]
        intercepted = []
        for name in intercepts:
            val = kwargs.get(name)
            if val:
                intercepted.append((name, val))
                del kwargs[name]
        for tup in intercepted:
            setattr(cls, tup[0], tup[1])

        obj = super(MetaDAL, cls).__call__(*args, **kwargs)
        return obj


class DAL(Serializable, BasicStorage, metaclass=MetaDAL):
    """
    A pydal database handle: connection + adapter + dialect + tables.

    Args:
        uri: connection string or list of connection strings. Defaults
            to ``"sqlite://dummy.db"``. Multiple URIs are tried in order
            until one connects (useful for replication / failover).
        pool_size: max pooled connections. ``0`` disables pooling.
        folder: where ``.table`` snapshot files are written. Required
            when using SQLite outside a web framework.
        db_codec: string encoding the database expects (default UTF-8).
        check_reserved: list of dialect names to validate identifiers
            against (``["common"]`` is recommended; ``["all"]`` checks
            every known SQL keyword; ``["<name>_nonreserved"]`` uses a
            looser per-adapter set).
        migrate: default migrate behavior for new tables.
        fake_migrate: default fake-migrate behavior for new tables.
        migrate_enabled: master switch — when False, no migration runs
            regardless of per-table settings.
        fake_migrate_all: mark every defined table as fake-migrated
            (use once after a hand-applied schema change).
        attempts: connection retries.
        auto_import: True to auto-import table definitions from
            snapshot files in ``folder``.
        bigint_id: True to use ``bigint`` instead of ``int`` for ``id``
            and ``reference`` columns.
        lazy_tables: True to defer ``define_table`` work until first
            access — useful with many tables and lazy clients.
        after_connection: callable run once per fresh connection.
        table_hash: override the auto-derived hash used to prefix
            snapshot files. Pass when you want to share snapshots
            across DAL instances.

    Example::

        db = DAL("sqlite://test.db")
        db.define_table("thing", Field("name"))

    See ``README.md`` for the full tour.
    """

    serializers = None
    validators = None
    representers = {}
    validators_method = default_validators
    uuid = staticmethod(uuidstr)
    logger = logging.getLogger("pyDAL")

    Field = Field
    Table = Table
    Rows = Rows
    Row = Row

    record_operators = {"update_record": RecordUpdater, "delete_record": RecordDeleter}

    execution_handlers = [TimingHandler]

    def __new__(cls, uri="sqlite://dummy.db", *args, **kwargs):
        if not hasattr(THREAD_LOCAL, "_pydal_db_instances_"):
            THREAD_LOCAL._pydal_db_instances_ = {}
        if not hasattr(THREAD_LOCAL, "_pydal_db_instances_zombie_"):
            THREAD_LOCAL._pydal_db_instances_zombie_ = {}
        if uri == "<zombie>":
            db_uid = kwargs["db_uid"]  # a zombie must have a db_uid!
            if db_uid in THREAD_LOCAL._pydal_db_instances_:
                db_group = THREAD_LOCAL._pydal_db_instances_[db_uid]
                db = db_group[-1]
            elif db_uid in THREAD_LOCAL._pydal_db_instances_zombie_:
                db = THREAD_LOCAL._pydal_db_instances_zombie_[db_uid]
            else:
                db = super(DAL, cls).__new__(cls)
                THREAD_LOCAL._pydal_db_instances_zombie_[db_uid] = db
        else:
            db_uid = kwargs.get("db_uid", hashlib_md5(repr(uri)).hexdigest())
            if db_uid in THREAD_LOCAL._pydal_db_instances_zombie_:
                db = THREAD_LOCAL._pydal_db_instances_zombie_[db_uid]
                del THREAD_LOCAL._pydal_db_instances_zombie_[db_uid]
            else:
                db = super(DAL, cls).__new__(cls)
            db_group = THREAD_LOCAL._pydal_db_instances_.get(db_uid, [])
            db_group.append(db)
            THREAD_LOCAL._pydal_db_instances_[db_uid] = db_group
        db._db_uid = db_uid
        return db

    @staticmethod
    def set_folder(folder: str) -> None:
        """
        Set the default snapshot/upload folder for DAL instances
        created later in this thread.

        Equivalent to passing ``folder=`` to every ``DAL()`` call.
        """
        BaseAdapter.set_folder(folder)

    @staticmethod
    def get_instances():
        """
        Returns a dictionary with uri as key with timings and defined tables::

            {'sqlite://storage.sqlite': {
                'dbstats': [(select auth_user.email from auth_user, 0.02009)],
                'dbtables': {
                    'defined': ['auth_cas', 'auth_event', 'auth_group',
                        'auth_membership', 'auth_permission', 'auth_user'],
                    'lazy': '[]'
                    }
                }
            }

        """
        dbs = getattr(THREAD_LOCAL, "_pydal_db_instances_", {}).items()
        infos = {}
        for db_uid, db_group in dbs:
            for db in db_group:
                if not db._uri:
                    continue
                k = hide_password(db._adapter.uri)
                infos[k] = dict(
                    dbstats=[(row[0], row[1]) for row in db._timings],
                    dbtables={
                        "defined": sorted(
                            list(set(db.tables) - set(db._LAZY_TABLES.keys()))
                        ),
                        "lazy": sorted(db._LAZY_TABLES.keys()),
                    },
                )
        return infos

    @staticmethod
    def _distributed_keys(instances):
        thread_key = "%s.%s" % (socket.gethostname(), threading.current_thread())
        keys = ["%s.%i" % (thread_key, i) for i in range(len(instances))]
        for db in instances:
            if not db._adapter.support_distributed_transaction():
                raise SyntaxError(
                    "distributed transaction not suported by %s" % db._dbname
                )
        return keys

    @staticmethod
    def distributed_transaction_begin(*instances) -> None:
        """
        Begin a 2-phase commit spanning multiple DAL instances.

        Pair with ``distributed_transaction_commit`` (or rollback each
        instance on failure). Every passed adapter must report
        ``support_distributed_transaction()``.
        """
        if not instances:
            return
        keys = DAL._distributed_keys(instances)
        for i, db in enumerate(instances):
            db._adapter.distributed_transaction_begin(keys[i])

    @staticmethod
    def distributed_transaction_commit(*instances) -> None:
        """
        Two-phase commit across ``instances``.

        Phase 1 runs ``PREPARE`` on every adapter; if any prepare fails,
        every adapter is rolled back and a ``RuntimeError`` is raised.
        Phase 2 runs ``COMMIT PREPARED`` on each.
        """
        if not instances:
            return
        keys = DAL._distributed_keys(instances)
        try:
            for i, db in enumerate(instances):
                db._adapter.prepare(keys[i])
        except Exception:
            for i, db in enumerate(instances):
                db._adapter.rollback_prepared(keys[i])
            raise RuntimeError("failure to commit distributed transaction")
        for i, db in enumerate(instances):
            db._adapter.commit_prepared(keys[i])

    def __init__(
        self,
        uri="sqlite://dummy.db",
        pool_size=0,
        folder=None,
        db_codec="UTF-8",
        check_reserved=None,
        migrate=True,
        fake_migrate=False,
        migrate_enabled=True,
        fake_migrate_all=False,
        decode_credentials=False,
        driver_args=None,
        adapter_args=None,
        attempts=5,
        auto_import=False,
        bigint_id=False,
        debug=False,
        lazy_tables=False,
        db_uid=None,
        after_connection=None,
        tables=None,
        ignore_field_case=True,
        entity_quoting=True,
        table_hash=None,
    ):
        if uri == "<zombie>" and db_uid is not None:
            return
        super(DAL, self).__init__()

        # Detect the common dev paper-cut: someone deleted storage.db but
        # the per-table *.table migration markers for this exact DB hash
        # are still on disk. Pydal would then think the schema is already
        # applied and refuse to recreate it, surfacing later as the
        # obscure "no such table". We scope the check by uri hash so .table
        # files belonging to other databases in the same folder don't
        # trigger a false positive.
        if (
            isinstance(uri, str)
            and uri.startswith("sqlite://")
            and folder
            and not fake_migrate_all
            and not fake_migrate
            and migrate
            and migrate_enabled
        ):
            db_filename = uri[len("sqlite://"):]
            if db_filename and "://" not in db_filename and os.path.isdir(folder):
                db_path = os.path.join(folder, db_filename)
                if not os.path.exists(db_path):
                    expected_hash = (
                        table_hash or hashlib_md5(uri).hexdigest()
                    )
                    prefix = expected_hash + "_"
                    try:
                        my_markers = [
                            n for n in os.listdir(folder)
                            if n.endswith(".table") and n.startswith(prefix)
                        ]
                    except OSError:
                        my_markers = []
                    if my_markers:
                        raise RuntimeError(
                            "pydal: stale migration markers in %r: %d "
                            ".table file(s) for this database exist but %r "
                            "is missing. Either delete the matching .table "
                            "files (rm %s/%s*.table) to force a clean "
                            "migration, or pass fake_migrate_all=True once "
                            "to rebuild from the existing markers."
                            % (folder, len(my_markers), db_filename,
                               folder, expected_hash)
                        )

        if not issubclass(self.Rows, Rows):
            raise RuntimeError("`Rows` class must be a subclass of pydal.objects.Rows")

        if not issubclass(self.Row, Row):
            raise RuntimeError("`Row` class must be a subclass of pydal.objects.Row")

        from .drivers import DRIVERS, is_jdbc

        self._drivers_available = DRIVERS

        if not decode_credentials:
            credential_decoder = lambda cred: cred
        else:
            credential_decoder = lambda cred: unquote(cred)
        self._folder = folder
        if folder:
            self.set_folder(folder)
        self._uri = uri
        self._pool_size = pool_size
        self._db_codec = db_codec
        self._pending_references = {}
        self._request_tenant = "request_tenant"
        self._common_fields = []
        self._referee_name = "%(table)s"
        self._bigint_id = bigint_id
        self._debug = debug
        self._migrated = []
        self._LAZY_TABLES = {}
        self._lazy_tables = lazy_tables
        self._tables = SQLCallableList()
        self._aliased_tables = threading.local()
        self._driver_args = driver_args
        self._adapter_args = adapter_args
        self._check_reserved = check_reserved
        self._decode_credentials = decode_credentials
        self._attempts = attempts
        self._ignore_field_case = ignore_field_case
        self._dbname = None

        if not str(attempts).isdigit() or attempts < 0:
            attempts = 5
        if uri:
            uris = isinstance(uri, (list, tuple)) and uri or [uri]
            connected = False
            for k in range(attempts):
                for uri in uris:
                    try:
                        from . import backends  # noqa: F401  — triggers backend registration
                        from .backend_base import adapters

                        if is_jdbc and not uri.startswith("jdbc:"):
                            uri = "jdbc:" + uri
                        self._dbname = REGEX_DBNAME.match(uri).group()
                        # notice that driver args or {} else driver_args
                        # defaults to {} global, not correct
                        kwargs = dict(
                            db=self,
                            uri=uri,
                            pool_size=pool_size,
                            folder=folder,
                            db_codec=db_codec,
                            credential_decoder=credential_decoder,
                            driver_args=driver_args or {},
                            adapter_args=adapter_args or {},
                            after_connection=after_connection,
                            entity_quoting=entity_quoting,
                        )
                        adapter = adapters.get_for(self._dbname)
                        self._adapter = adapter(**kwargs)
                        # self._adapter.ignore_field_case = ignore_field_case
                        if bigint_id:
                            self._adapter.dialect._force_bigints()
                        # if there are multiple URIs to try in sequence, do not defer connection
                        if len(uris) > 1:
                            self._adapter.connector()
                        connected = True
                        break
                    except SyntaxError:
                        raise
                    except Exception:
                        tb = traceback.format_exc()
                        self.logger.debug(
                            "DEBUG: connect attempt %i, connection error:\n%s" % (k, tb)
                        )
                if connected:
                    break
                else:
                    time.sleep(1)
            if not connected:
                raise RuntimeError(
                    "Failure to connect, tried %d times:\n%s" % (attempts, tb)
                )
        else:
            self._adapter = NullAdapter(
                db=self,
                pool_size=0,
                uri="None",
                folder=folder,
                db_codec=db_codec,
                after_connection=after_connection,
                entity_quoting=entity_quoting,
            )
            migrate = fake_migrate = False
            self.validators_method = None
            self.validators = None
        adapter = self._adapter
        self._uri_hash = table_hash or hashlib_md5(adapter.uri).hexdigest()
        if check_reserved:
            from .contrib.reserved_sql_keywords import ADAPTERS as RSK

            self.RSK = RSK
        self._migrate = migrate
        self._fake_migrate = fake_migrate
        self._migrate_enabled = migrate_enabled
        self._fake_migrate_all = fake_migrate_all
        if self.serializers is not None:
            for k, v in self.serializers.items():
                serializers._custom_[k] = v
        if auto_import or tables:
            self.import_table_definitions(adapter.folder, tables=tables)

    @contextlib.contextmanager
    def single_transaction(self):
        """
        Context manager: commit on success, rollback on exception.

        Wraps a fresh connection and closes it at exit. Suitable for
        background scripts that want all-or-nothing semantics::

            with db.single_transaction():
                db.thing.insert(...)
                db.other.insert(...)
        """
        self._adapter.reconnect()
        try:
            yield self
        except Exception:
            self._adapter.rollback()
        else:
            self._adapter.commit()
        finally:
            self.close()

    @property
    def tables(self):
        return self._tables

    @property
    def _timings(self):
        return getattr(THREAD_LOCAL, "_pydal_timings_", [])

    @property
    def _lastsql(self):
        return self._timings[-1] if self._timings else None

    def import_table_definitions(
        self, path, migrate=False, fake_migrate=False, tables=None
    ):
        if tables:
            for table in tables:
                self.define_table(**table)
        else:
            pattern = pjoin(path, self._uri_hash + "_*.table")
            for filename in glob.glob(pattern):
                tfile = self._adapter.migrator.file_open(filename, "rb")
                try:
                    sql_fields = pickle.load(tfile)
                    name = filename[len(pattern) - 7 : -6]
                    mf = [
                        (
                            value["sortable"],
                            Field(
                                key,
                                type=value["type"],
                                length=value.get("length", None),
                                notnull=value.get("notnull", False),
                                unique=value.get("unique", False),
                            ),
                        )
                        for key, value in sql_fields.items()
                    ]
                    mf.sort(key=lambda a: a[0])
                    self.define_table(
                        name,
                        *[item[1] for item in mf],
                        **dict(migrate=migrate, fake_migrate=fake_migrate),
                    )
                finally:
                    self._adapter.migrator.file_close(tfile)

    def check_reserved_keyword(self, name):
        """
        Validates `name` against SQL keywords
        Uses self._check_reserved which is a list of operators to use.
        """
        for backend in self._check_reserved:
            if name.upper() in self.RSK[backend]:
                raise SyntaxError(
                    'invalid table/column name "%s" is a "%s" reserved SQL/NOSQL keyword'
                    % (name, backend.upper())
                )

    def parse_as_rest(self, patterns, args, vars, queries=None, nested_select=True):
        """
        Dispatch a REST request against ``RestParser``.

        See ``pydal/helpers/rest.py::RestParser.parse`` for the
        pattern grammar and response shape.
        """
        return RestParser(self).parse(patterns, args, vars, queries, nested_select)

    def define_table(self, tablename: str, *fields, **kwargs):
        """
        Define (or redefine) a table on this DAL.

        ``fields`` is a sequence of ``Field`` objects. Recognized
        kwargs are listed in ``TABLE_ARGS`` (``migrate``,
        ``fake_migrate``, ``primarykey``, ``format``, ``redefine``,
        ``singular``, ``plural``, ``trigger_name``, ``sequence_name``,
        ``fields``, ``common_filter``, ``table_class``, ``on_define``,
        ``rname``).

        Returns the new ``Table`` (or ``None`` when ``lazy_tables`` is
        enabled and the table hasn't been materialized yet).
        """
        invalid_kwargs = set(kwargs) - TABLE_ARGS
        if invalid_kwargs:
            raise SyntaxError(
                'invalid table "%s" attributes: %s' % (tablename, invalid_kwargs)
            )
        if not fields and "fields" in kwargs:
            fields = kwargs.get("fields", ())
        if not isinstance(tablename, str):
            raise SyntaxError("missing table name")
        redefine = kwargs.get("redefine", False)
        if tablename in self.tables:
            if redefine:
                try:
                    delattr(self, tablename)
                except AttributeError:
                    pass
            else:
                raise SyntaxError("table already defined: %s" % tablename)
        elif (
            tablename.startswith("_")
            or hasattr(self, tablename)
            or REGEX_PYTHON_KEYWORDS.match(tablename)
        ):
            raise SyntaxError("invalid table name: %s" % tablename)
        elif self._check_reserved:
            self.check_reserved_keyword(tablename)
        if self._lazy_tables:
            if tablename not in self._LAZY_TABLES or redefine:
                self._LAZY_TABLES[tablename] = (tablename, fields, kwargs)
            table = None
        else:
            table = self.lazy_define_table(tablename, *fields, **kwargs)
        if tablename not in self.tables:
            self.tables.append(tablename)
        return table

    def lazy_define_table(self, tablename: str, *fields, **kwargs):
        """
        Internal: actually materialize a table previously deferred by
        ``define_table`` under ``lazy_tables=True``.

        Direct callers should use ``define_table``; this method runs the
        common-fields merge, instantiates the Table, resolves references,
        installs default validators/representers, and triggers any
        pending migration.
        """
        kwargs_get = kwargs.get
        common_fields = self._common_fields
        if common_fields:
            fields = list(fields) + [
                f if isinstance(f, Table) else f.clone() for f in common_fields
            ]

        table_class = kwargs_get("table_class", Table)
        table = table_class(self, tablename, *fields, **kwargs)
        table._actual = True
        self[tablename] = table
        # must follow above line to handle self references
        table._create_references()
        for field in table:
            if field.requires is DEFAULT:
                field.requires = auto_validators(field)
            if field.represent is None:
                field.represent = auto_represent(field)

        if self._adapter.dbengine == "firestore" or self._uri in (None, "None"):
            migrate = False
        else:
            migrate = self._migrate_enabled and kwargs_get("migrate", self._migrate)
        if migrate:
            fake_migrate = self._fake_migrate_all or kwargs_get(
                "fake_migrate", self._fake_migrate
            )
            try:
                GLOBAL_LOCKER.acquire()
                self._adapter.create_table(
                    table,
                    migrate=migrate,
                    fake_migrate=fake_migrate,
                )
            finally:
                GLOBAL_LOCKER.release()
        else:
            table._dbt = None
        on_define = kwargs_get("on_define", None)
        if on_define:
            on_define(table)
        return table

    def as_dict(self, flat=False, sanitize=True):
        db_uid = uri = None
        if not sanitize:
            uri, db_uid = (self._uri, self._db_uid)
        db_as_dict = dict(
            tables=[],
            uri=uri,
            db_uid=db_uid,
            **dict(
                [
                    (k, getattr(self, "_" + k, None))
                    for k in [
                        "pool_size",
                        "folder",
                        "db_codec",
                        "check_reserved",
                        "migrate",
                        "fake_migrate",
                        "migrate_enabled",
                        "fake_migrate_all",
                        "decode_credentials",
                        "driver_args",
                        "adapter_args",
                        "attempts",
                        "bigint_id",
                        "debug",
                        "lazy_tables",
                    ]
                ]
            ),
        )
        for table in self:
            db_as_dict["tables"].append(table.as_dict(flat=flat, sanitize=sanitize))
        return db_as_dict

    def __contains__(self, tablename):
        try:
            return tablename in self.tables
        except AttributeError:
            # The instance has no .tables attribute yet
            return False

    def __iter__(self):
        for tablename in self.tables:
            yield self[tablename]

    def __getitem__(self, key):
        return self.__getattr__(str(key))

    def __getattr__(self, key):
        if object.__getattribute__(
            self, "_lazy_tables"
        ) and key in object.__getattribute__(self, "_LAZY_TABLES"):
            tablename, fields, kwargs = self._LAZY_TABLES.pop(key)
            return self.lazy_define_table(tablename, *fields, **kwargs)
        aliased_tables = object.__getattribute__(self, "_aliased_tables")
        aliased = getattr(aliased_tables, key, None)
        if aliased:
            return aliased
        return BasicStorage.__getattribute__(self, key)

    def __setattr__(self, key, value):
        if key[:1] != "_" and key in self:
            raise SyntaxError("Object %s exists and cannot be redefined" % key)
        return super(DAL, self).__setattr__(key, value)

    def __repr__(self):
        if hasattr(self, "_uri"):
            return '<DAL uri="%s">' % hide_password(self._adapter.uri)
        else:
            return '<DAL db_uid="%s">' % self._db_uid

    def smart_query(self, fields, text):
        return Set(self, smart_query(fields, text))

    def __call__(self, query=None, ignore_common_filters=None):
        return self.where(query, ignore_common_filters)

    def where(self, query=None, ignore_common_filters=None):
        """
        Wrap a query into a ``Set``.

        ``query`` is normally a pydal ``Query``, but ``where`` also
        accepts a ``Table`` (treated as ``table.id > 0``) or a ``Field``
        (treated as ``field != None``) for ergonomic convenience.
        """
        if isinstance(query, Table):
            query = self._adapter.id_query(query)
        elif isinstance(query, Field):
            query = query != None  # noqa: E711
        elif isinstance(query, dict):
            icf = query.get("ignore_common_filters")
            if icf:
                ignore_common_filters = icf
        return Set(self, query, ignore_common_filters=ignore_common_filters)

    def commit(self) -> None:
        """COMMIT the current transaction and forget per-transaction aliases."""
        self._adapter.commit()
        object.__getattribute__(self, "_aliased_tables").__dict__.clear()

    def rollback(self) -> None:
        """ROLLBACK the current transaction and forget per-transaction aliases."""
        self._adapter.rollback()
        object.__getattribute__(self, "_aliased_tables").__dict__.clear()

    def close(self) -> None:
        """Close this DAL's connection and unregister from THREAD_LOCAL."""
        self._adapter.close()
        if self._db_uid in THREAD_LOCAL._pydal_db_instances_:
            db_group = THREAD_LOCAL._pydal_db_instances_[self._db_uid]
            db_group.remove(self)
            if not db_group:
                del THREAD_LOCAL._pydal_db_instances_[self._db_uid]
        self._adapter._clean_tlocals()

    def get_connection_from_pool_or_new(self):
        self._adapter.reconnect()

    def recycle_connection_in_pool_or_close(self, action="commit"):
        self._adapter.close(action, really=True)

    def executesql(
        self,
        query,
        placeholders=None,
        as_dict=False,
        fields=None,
        colnames=None,
        as_ordered_dict=False,
    ):
        """
        Executes an arbitrary query

        Args:
            query (str): the query to submit to the backend
            placeholders: is optional and will always be None.
                If using raw SQL with placeholders, placeholders may be
                a sequence of values to be substituted in
                or, (if supported by the DB driver), a dictionary with keys
                matching named placeholders in your SQL.
            as_dict: will always be None when using DAL.
                If using raw SQL can be set to True and the results cursor
                returned by the DB driver will be converted to a sequence of
                dictionaries keyed with the db field names. Results returned
                with as_dict=True are the same as those returned when applying
                .to_list() to a DAL query.  If "as_ordered_dict"=True the
                behaviour is the same as when "as_dict"=True with the keys
                (field names) guaranteed to be in the same order as returned
                by the select name executed on the database.
            fields: list of DAL Fields that match the fields returned from the
                DB. The Field objects should be part of one or more Table
                objects defined on the DAL object. The "fields" list can include
                one or more DAL Table objects in addition to or instead of
                including Field objects, or it can be just a single table
                (not in a list). In that case, the Field objects will be
                extracted from the table(s).

                Note:
                    if either `fields` or `colnames` is provided, the results
                    will be converted to a DAL `Rows` object using the
                    `db._adapter.parse()` method
            colnames: list of field names in tablename.fieldname format

        Note:
            It is also possible to specify both "fields" and the associated
            "colnames". In that case, "fields" can also include DAL Expression
            objects in addition to Field objects. For Field objects in "fields",
            the associated "colnames" must still be in tablename.fieldname
            format. For Expression objects in "fields", the associated
            "colnames" can be any arbitrary labels.

        DAL Table objects referred to by "fields" or "colnames" can be dummy
        tables and do not have to represent any real tables in the database.
        Also, note that the "fields" and "colnames" must be in the
        same order as the fields in the results cursor returned from the DB.

        """
        adapter = self._adapter
        if placeholders:
            adapter.execute(query, placeholders)
        else:
            adapter.execute(query)
        if as_dict or as_ordered_dict:
            if not hasattr(adapter.cursor, "description"):
                raise RuntimeError(
                    "database does not support executesql(...,as_dict=True)"
                )
            # Non-DAL legacy db query, converts cursor results to dict.
            # sequence of 7-item sequences. each sequence tells about a column.
            # first item is always the field name according to Python Database API specs
            columns = adapter.cursor.description
            # reduce the column info down to just the field names
            fields = colnames or [f[0] for f in columns]
            if len(fields) != len(set(fields)):
                raise RuntimeError(
                    "Result set includes duplicate column names. Specify unique column names using the 'colnames' argument"
                )
            #: avoid bytes strings in columns names (py3)
            if columns:
                for i in range(0, len(fields)):
                    if isinstance(fields[i], bytes):
                        fields[i] = fields[i].decode("utf8")

            # will hold our finished resultset in a list
            data = adapter.fetchall()
            # convert the list for each row into a dictionary so it's
            # easier to work with. row['field_name'] rather than row[0]
            if as_ordered_dict:
                _dict = OrderedDict
            else:
                _dict = dict
            return [_dict(zip(fields, row)) for row in data]
        try:
            data = adapter.fetchall()
        except Exception:
            return None
        if fields or colnames:
            fields = [] if fields is None else fields
            if not isinstance(fields, list):
                fields = [fields]
            extracted_fields = []
            for field in fields:
                if isinstance(field, Table):
                    extracted_fields.extend([f for f in field])
                else:
                    extracted_fields.append(field)
            if not colnames:
                colnames = [f.sqlsafe for f in extracted_fields]
            else:
                #: extracted_fields is empty we should make it from colnames
                # what 'col_fields' is for
                col_fields = []  # [[tablename, fieldname], ....]
                newcolnames = []
                for tf in colnames:
                    if "." in tf:
                        t_f = tf.split(".")
                        tf = ".".join(adapter.dialect.quote(f) for f in t_f)
                    else:
                        t_f = None
                    if not extracted_fields:
                        col_fields.append(t_f)
                    newcolnames.append(tf)
                colnames = newcolnames
            data = adapter.parse(
                data,
                fields=extracted_fields
                or [tf and self[tf[0]][tf[1]] for tf in col_fields],
                colnames=colnames,
            )
        return data

    def _remove_references_to(self, thistable):
        for table in self:
            table._referenced_by = [
                field for field in table._referenced_by if not field.table == thistable
            ]

    def has_representer(self, name):
        return callable(self.representers.get(name))

    def represent(self, name, *args, **kwargs):
        return self.representers[name](*args, **kwargs)

    def export_to_csv_file(self, ofile, *args, **kwargs):
        step = int(kwargs.get("max_fetch_rows", 500))
        write_colnames = kwargs["write_colnames"] = kwargs.get("write_colnames", True)
        for table in self.tables:
            ofile.write("TABLE %s\r\n" % table)
            query = self._adapter.id_query(self[table])
            nrows = self(query).count()
            kwargs["write_colnames"] = write_colnames
            for k in range(0, nrows, step):
                self(query).select(limitby=(k, k + step)).export_to_csv_file(
                    ofile, *args, **kwargs
                )
                kwargs["write_colnames"] = False
            ofile.write("\r\n\r\n")
        ofile.write("END")

    def import_from_csv_file(
        self,
        ifile,
        id_map=None,
        null="<NULL>",
        unique="uuid",
        map_tablenames=None,
        ignore_missing_tables=False,
        *args,
        **kwargs,
    ):
        # if id_map is None: id_map={}
        id_offset = {}  # only used if id_map is None
        map_tablenames = map_tablenames or {}
        for line in ifile:
            line = line.strip()
            if not line:
                continue
            elif line == "END":
                return
            elif not line.startswith("TABLE "):
                raise SyntaxError("Invalid file format")
            elif not line[6:] in self.tables:
                raise SyntaxError("Unknown table : %s" % line[6:])
            else:
                tablename = line[6:]
                tablename = map_tablenames.get(tablename, tablename)
                if tablename is not None and tablename in self.tables:
                    self[tablename].import_from_csv_file(
                        ifile, id_map, null, unique, id_offset, *args, **kwargs
                    )
                elif tablename is None or ignore_missing_tables:
                    # skip all non-empty lines
                    for line in ifile:
                        if not line.strip():
                            break
                else:
                    raise RuntimeError(
                        "Unable to import table that does not exist.\nTry db.import_from_csv_file(..., map_tablenames={'table':'othertable'},ignore_missing_tables=True)"
                    )

    def can_join(self):
        return self._adapter.can_join()


def DAL_unpickler(db_uid):
    return DAL("<zombie>", db_uid=db_uid)


def DAL_pickler(db):
    return DAL_unpickler, (db._db_uid,)


copyreg.pickle(DAL, DAL_pickler, DAL_unpickler)
