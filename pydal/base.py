# -*- coding: utf-8 -*-

"""
| This file is part of the web2py Web Framework
| Copyrighted by Massimo Di Pierro <mdipierro@cs.depaul.edu>
| License: LGPLv3 (http://www.gnu.org/licenses/lgpl.html)
|

This file contains the DAL support for many relational databases, including:

  - SQLite & SpatiaLite
  - MySQL
  - Postgres
  - Firebird
  - Oracle
  - MS SQL
  - DB2
  - Interbase
  - Ingres
  - Informix (9+ and SE)
  - SapDB (experimental)
  - Cubrid (experimental)
  - CouchDB (experimental)
  - MongoDB (in progress)
  - Google:nosql
  - Google:sql
  - Teradata
  - IMAP (experimental)

Example of usage::

    >>> # from dal import DAL, Field

    ### create DAL connection (and create DB if it doesn't exist)
    >>> db = DAL(('sqlite://storage.sqlite','mysql://a:b@localhost/x'),
    ... folder=None)

    ### define a table 'person' (create/alter as necessary)
    >>> person = db.define_table('person',Field('name','string'))

    ### insert a record
    >>> id = person.insert(name='James')

    ### retrieve it by id
    >>> james = person(id)

    ### retrieve it by name
    >>> james = person(name='James')

    ### retrieve it by arbitrary query
    >>> query = (person.name=='James') & (person.name.startswith('J'))
    >>> james = db(query).select(person.ALL)[0]

    ### update one record
    >>> james.update_record(name='Jim')
    <Row {'id': 1, 'name': 'Jim'}>

    ### update multiple records by query
    >>> db(person.name.like('J%')).update(name='James')
    1

    ### delete records by query
    >>> db(person.name.lower() == 'jim').delete()
    0

    ### retrieve multiple records (rows)
    >>> people = db(person).select(orderby=person.name,
    ... groupby=person.name, limitby=(0,100))

    ### further filter them
    >>> james = people.find(lambda row: row.name == 'James').first()
    >>> print james.id, james.name
    1 James

    ### check aggregates
    >>> counter = person.id.count()
    >>> print db(person).select(counter).first()(counter)
    1

    ### delete one record
    >>> james.delete_record()
    1

    ### delete (drop) entire database table
    >>> person.drop()


Supported DAL URI strings::

    'sqlite://test.db'
    'spatialite://test.db'
    'sqlite:memory'
    'spatialite:memory'
    'jdbc:sqlite://test.db'
    'mysql://root:none@localhost/test'
    'postgres://mdipierro:password@localhost/test'
    'postgres:psycopg2://mdipierro:password@localhost/test'
    'postgres:pg8000://mdipierro:password@localhost/test'
    'jdbc:postgres://mdipierro:none@localhost/test'
    'mssql://web2py:none@A64X2/web2py_test'
    'mssql2://web2py:none@A64X2/web2py_test' # alternate mappings
    'mssql3://web2py:none@A64X2/web2py_test' # better pagination (requires >= 2005)
    'mssql4://web2py:none@A64X2/web2py_test' # best pagination (requires >= 2012)
    'oracle://username:password@database'
    'firebird://user:password@server:3050/database'
    'db2:ibm_db_dbi://DSN=dsn;UID=user;PWD=pass'
    'db2:pyodbc://driver=DB2;hostname=host;database=database;uid=user;pwd=password;port=port'
    'firebird://username:password@hostname/database'
    'firebird_embedded://username:password@c://path'
    'informix://user:password@server:3050/database'
    'informixu://user:password@server:3050/database' # unicode informix
    'ingres://database'  # or use an ODBC connection string, e.g. 'ingres://dsn=dsn_name'
    'google:datastore' # for google app engine datastore (uses ndb by default)
    'google:sql' # for google app engine with sql (mysql compatible)
    'teradata://DSN=dsn;UID=user;PWD=pass; DATABASE=database' # experimental
    'imap://user:password@server:port' # experimental
    'mongodb://user:password@server:port/database' # experimental

For more info::

    help(DAL)
    help(Field)

"""

import glob
import logging
import socket
import threading
import time
import traceback
import urllib
from uuid import uuid4

from ._compat import PY2, pickle, hashlib_md5, pjoin, copyreg, integer_types, \
    with_metaclass, long, unquote, iteritems
from ._globals import GLOBAL_LOCKER, THREAD_LOCAL, DEFAULT
from ._load import OrderedDict
from .helpers.classes import Serializable, SQLCallableList, BasicStorage, \
    RecordUpdater, RecordDeleter, TimingHandler
from .helpers.methods import hide_password, smart_query, auto_validators, \
    auto_represent
from .helpers.regex import REGEX_PYTHON_KEYWORDS, REGEX_DBNAME
from .helpers.rest import RestParser
from .helpers.serializers import serializers
from .objects import Table, Field, Rows, Row, Set
from .adapters.base import BaseAdapter, NullAdapter

TABLE_ARGS = set(
    ('migrate', 'primarykey', 'fake_migrate', 'format', 'redefine',
     'singular', 'plural', 'trigger_name', 'sequence_name', 'fields',
     'common_filter', 'polymodel', 'table_class', 'on_define', 'rname'))


class MetaDAL(type):
    def __call__(cls, *args, **kwargs):
        #: intercept arguments for DAL customisation on call
        intercepts = [
            'logger', 'representers', 'serializers', 'uuid', 'validators',
            'validators_method', 'Table', 'Row']
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


class DAL(with_metaclass(MetaDAL, Serializable, BasicStorage)):
    """
    An instance of this class represents a database connection

    Args:
        uri(str): contains information for connecting to a database.
            Defaults to `'sqlite://dummy.db'`

            Note:
                experimental: you can specify a dictionary as uri
                parameter i.e. with::

                    db = DAL({"uri": "sqlite://storage.sqlite",
                              "tables": {...}, ...})

                for an example of dict input you can check the output
                of the scaffolding db model with

                    db.as_dict()

                Note that for compatibility with Python older than
                version 2.6.5 you should cast your dict input keys
                to str due to a syntax limitation on kwarg names.
                for proper DAL dictionary input you can use one of::

                    obj = serializers.cast_keys(dict, [encoding="utf-8"])
                    #or else (for parsing json input)
                    obj = serializers.loads_json(data, unicode_keys=False)

        pool_size: How many open connections to make to the database object.
        folder: where .table files will be created. Automatically set within
            web2py. Use an explicit path when using DAL outside web2py
        db_codec: string encoding of the database (default: 'UTF-8')
        table_hash: database identifier with .tables. If your connection hash
                    change you can still using old .tables if they have db_hash
                    as prefix
        check_reserved: list of adapters to check tablenames and column names
            against sql/nosql reserved keywords. Defaults to `None`

            - 'common' List of sql keywords that are common to all database
              types such as "SELECT, INSERT". (recommended)
            - 'all' Checks against all known SQL keywords
            - '<adaptername>'' Checks against the specific adapters list of
              keywords
            - '<adaptername>_nonreserved' Checks against the specific adapters
              list of nonreserved keywords. (if available)

        migrate: sets default migrate behavior for all tables
        fake_migrate: sets default fake_migrate behavior for all tables
        migrate_enabled: If set to False disables ALL migrations
        fake_migrate_all: If set to True fake migrates ALL tables
        attempts: Number of times to attempt connecting
        auto_import: If set to True, tries import automatically table
            definitions from the databases folder (works only for simple models)
        bigint_id: If set, turn on bigint instead of int for id and reference
            fields
        lazy_tables: delays table definition until table access
        after_connection: can a callable that will be executed after the
            connection

    Example:
        Use as::

           db = DAL('sqlite://test.db')

        or::

           db = DAL(**{"uri": ..., "tables": [...]...}) # experimental

           db.define_table('tablename', Field('fieldname1'),
                                        Field('fieldname2'))


    """
    serializers = None
    validators = None
    validators_method = None
    representers = {}
    uuid = lambda x: str(uuid4())
    logger = logging.getLogger("pyDAL")

    Table = Table
    Rows = Rows
    Row = Row

    record_operators = {
        'update_record': RecordUpdater,
        'delete_record': RecordDeleter
    }

    execution_handlers = [TimingHandler]

    def __new__(cls, uri='sqlite://dummy.db', *args, **kwargs):
        if not hasattr(THREAD_LOCAL, '_pydal_db_instances_'):
            THREAD_LOCAL._pydal_db_instances_ = {}
        if not hasattr(THREAD_LOCAL, '_pydal_db_instances_zombie_'):
            THREAD_LOCAL._pydal_db_instances_zombie_ = {}
        if uri == '<zombie>':
            db_uid = kwargs['db_uid']  # a zombie must have a db_uid!
            if db_uid in THREAD_LOCAL._pydal_db_instances_:
                db_group = THREAD_LOCAL._pydal_db_instances_[db_uid]
                db = db_group[-1]
            elif db_uid in THREAD_LOCAL._pydal_db_instances_zombie_:
                db = THREAD_LOCAL._pydal_db_instances_zombie_[db_uid]
            else:
                db = super(DAL, cls).__new__(cls)
                THREAD_LOCAL._pydal_db_instances_zombie_[db_uid] = db
        else:
            db_uid = kwargs.get('db_uid', hashlib_md5(repr(uri)).hexdigest())
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
    def set_folder(folder):
        # ## this allows gluon to set a folder for this thread
        # ## <<<<<<<<< Should go away as new DAL replaces old sql.py
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
        dbs = getattr(THREAD_LOCAL, '_pydal_db_instances_', {}).items()
        infos = {}
        for db_uid, db_group in dbs:
            for db in db_group:
                if not db._uri:
                    continue
                k = hide_password(db._adapter.uri)
                infos[k] = dict(
                    dbstats=[(row[0], row[1]) for row in db._timings],
                    dbtables={
                        'defined': sorted(
                            list(set(db.tables) - set(db._LAZY_TABLES.keys()))
                        ),
                        'lazy': sorted(db._LAZY_TABLES.keys())}
                )
        return infos

    @staticmethod
    def distributed_transaction_begin(*instances):
        if not instances:
            return
        thread_key = '%s.%s' % (
            socket.gethostname(), threading.currentThread())
        keys = ['%s.%i' % (thread_key, i) for (i, db) in instances]
        instances = enumerate(instances)
        for (i, db) in instances:
            if not db._adapter.support_distributed_transaction():
                raise SyntaxError(
                    'distributed transaction not suported by %s' % db._dbname)
        for (i, db) in instances:
            db._adapter.distributed_transaction_begin(keys[i])

    @staticmethod
    def distributed_transaction_commit(*instances):
        if not instances:
            return
        instances = enumerate(instances)
        thread_key = '%s.%s' % (
            socket.gethostname(), threading.currentThread())
        keys = ['%s.%i' % (thread_key, i) for (i, db) in instances]
        for (i, db) in instances:
            if not db._adapter.support_distributed_transaction():
                raise SyntaxError(
                    'distributed transaction not suported by %s' % db._dbanme)
        try:
            for (i, db) in instances:
                db._adapter.prepare(keys[i])
        except:
            for (i, db) in instances:
                db._adapter.rollback_prepared(keys[i])
            raise RuntimeError('failure to commit distributed transaction')
        else:
            for (i, db) in instances:
                db._adapter.commit_prepared(keys[i])
        return

    def __init__(self, uri='sqlite://dummy.db',
                 pool_size=0, folder=None,
                 db_codec='UTF-8', check_reserved=None,
                 migrate=True, fake_migrate=False,
                 migrate_enabled=True, fake_migrate_all=False,
                 decode_credentials=False, driver_args=None,
                 adapter_args=None, attempts=5, auto_import=False,
                 bigint_id=False, debug=False, lazy_tables=False,
                 db_uid=None, do_connect=True,
                 after_connection=None, tables=None, ignore_field_case=True,
                 entity_quoting=True, table_hash=None):

        if uri == '<zombie>' and db_uid is not None:
            return
        super(DAL, self).__init__()

        if not issubclass(self.Rows, Rows):
            raise RuntimeError(
                '`Rows` class must be a subclass of pydal.objects.Rows'
            )

        if not issubclass(self.Row, Row):
            raise RuntimeError(
                '`Row` class must be a subclass of pydal.objects.Row'
            )

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
        self._request_tenant = 'request_tenant'
        self._common_fields = []
        self._referee_name = '%(table)s'
        self._bigint_id = bigint_id
        self._debug = debug
        self._migrated = []
        self._LAZY_TABLES = {}
        self._lazy_tables = lazy_tables
        self._tables = SQLCallableList()
        self._driver_args = driver_args
        self._adapter_args = adapter_args
        self._check_reserved = check_reserved
        self._decode_credentials = decode_credentials
        self._attempts = attempts
        self._do_connect = do_connect
        self._ignore_field_case = ignore_field_case

        if not str(attempts).isdigit() or attempts < 0:
            attempts = 5
        if uri:
            uris = isinstance(uri, (list, tuple)) and uri or [uri]
            connected = False
            for k in range(attempts):
                for uri in uris:
                    try:
                        from .adapters import adapters
                        if is_jdbc and not uri.startswith('jdbc:'):
                            uri = 'jdbc:' + uri
                        self._dbname = REGEX_DBNAME.match(uri).group()
                        # notice that driver args or {} else driver_args
                        # defaults to {} global, not correct
                        kwargs = dict(db=self,
                                      uri=uri,
                                      pool_size=pool_size,
                                      folder=folder,
                                      db_codec=db_codec,
                                      credential_decoder=credential_decoder,
                                      driver_args=driver_args or {},
                                      adapter_args=adapter_args or {},
                                      do_connect=do_connect,
                                      after_connection=after_connection,
                                      entity_quoting=entity_quoting)
                        adapter = adapters.get_for(self._dbname)
                        self._adapter = adapter(**kwargs)
                        #self._adapter.ignore_field_case = ignore_field_case
                        if bigint_id:
                            self._adapter.dialect._force_bigints()
                        connected = True
                        break
                    except SyntaxError:
                        raise
                    except Exception:
                        tb = traceback.format_exc()
                        self.logger.debug(
                            'DEBUG: connect attempt %i, connection error:\n%s'
                            % (k, tb)
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
                db=self, pool_size=0, uri='None', folder=folder,
                db_codec=db_codec, after_connection=after_connection,
                entity_quoting=entity_quoting)
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
            self.import_table_definitions(adapter.folder,
                                          tables=tables)

    @property
    def tables(self):
        return self._tables

    @property
    def _timings(self):
        return getattr(THREAD_LOCAL, '_pydal_timings_', [])

    @property
    def _lastsql(self):
        return self._timings[-1] if self._timings else None

    def import_table_definitions(self, path, migrate=False,
                                 fake_migrate=False, tables=None):
        if tables:
            for table in tables:
                self.define_table(**table)
        else:
            pattern = pjoin(path, self._uri_hash + '_*.table')
            for filename in glob.glob(pattern):
                tfile = self._adapter.migrator.file_open(filename, 'r' if PY2 else 'rb')
                try:
                    sql_fields = pickle.load(tfile)
                    name = filename[len(pattern) - 7:-6]
                    mf = [
                        (value['sortable'], Field(
                            key,
                            type=value['type'],
                            length=value.get('length', None),
                            notnull=value.get('notnull', False),
                            unique=value.get('unique', False)))
                        for key, value in iteritems(sql_fields)
                    ]
                    mf.sort(key=lambda a: a[0])
                    self.define_table(name, *[item[1] for item in mf],
                                      **dict(migrate=migrate,
                                             fake_migrate=fake_migrate))
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
                    'invalid table/column name "%s" is a "%s" reserved SQL/NOSQL keyword' % (name, backend.upper()))

    def parse_as_rest(self, patterns, args, vars, queries=None,
                      nested_select=True):
        return RestParser(self).parse(
            patterns, args, vars, queries, nested_select)

    def define_table(self, tablename, *fields, **kwargs):
        invalid_kwargs = set(kwargs) - TABLE_ARGS
        if invalid_kwargs:
            raise SyntaxError('invalid table "%s" attributes: %s' %
                              (tablename, invalid_kwargs))
        if not fields and 'fields' in kwargs:
            fields = kwargs.get('fields',())
        if not isinstance(tablename, str):
            if isinstance(tablename, unicode):
                try:
                    tablename = str(tablename)
                except UnicodeEncodeError:
                    raise SyntaxError("invalid unicode table name")
            else:
                raise SyntaxError("missing table name")
        redefine = kwargs.get('redefine', False)
        if tablename in self.tables:
            if redefine:
                try:
                    delattr(self, tablename)
                except:
                    pass
            else:
                raise SyntaxError('table already defined: %s' % tablename)
        elif tablename.startswith('_') or tablename in dir(self) or \
                REGEX_PYTHON_KEYWORDS.match(tablename):
            raise SyntaxError('invalid table name: %s' % tablename)
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

    def lazy_define_table(self, tablename, *fields, **kwargs):
        kwargs_get = kwargs.get
        common_fields = self._common_fields
        if common_fields:
            fields = list(fields) + [f if isinstance(f, Table) else f.clone() for f in common_fields]

        table_class = kwargs_get('table_class', Table)
        table = table_class(self, tablename, *fields, **kwargs)
        table._actual = True
        self[tablename] = table
        # must follow above line to handle self references
        table._create_references()
        for field in table:
            if field.requires == DEFAULT:
                field.requires = auto_validators(field)
            if field.represent is None:
                field.represent = auto_represent(field)

        migrate = self._migrate_enabled and kwargs_get('migrate', self._migrate)
        if migrate and self._uri not in (None, 'None') \
                or self._adapter.dbengine == 'google:datastore':
            fake_migrate = self._fake_migrate_all or \
                kwargs_get('fake_migrate', self._fake_migrate)
            polymodel = kwargs_get('polymodel', None)
            try:
                GLOBAL_LOCKER.acquire()
                self._adapter.create_table(
                    table, migrate=migrate,
                    fake_migrate=fake_migrate,
                    polymodel=polymodel)
            finally:
                GLOBAL_LOCKER.release()
        else:
            table._dbt = None
        on_define = kwargs_get('on_define', None)
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
                [(k, getattr(self, "_" + k, None)) for k in [
                    'pool_size', 'folder', 'db_codec', 'check_reserved',
                    'migrate', 'fake_migrate', 'migrate_enabled',
                    'fake_migrate_all', 'decode_credentials', 'driver_args',
                    'adapter_args', 'attempts', 'bigint_id', 'debug',
                    'lazy_tables', 'do_connect']]))
        for table in self:
            db_as_dict["tables"].append(table.as_dict(flat=flat,
                                        sanitize=sanitize))
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
        if object.__getattribute__(self, '_lazy_tables') and \
                key in object.__getattribute__(self, '_LAZY_TABLES'):
            tablename, fields, kwargs = self._LAZY_TABLES.pop(key)
            return self.lazy_define_table(tablename, *fields, **kwargs)
        return BasicStorage.__getattribute__(self, key)

    def __setattr__(self, key, value):
        if key[:1] != '_' and key in self:
            raise SyntaxError(
                'Object %s exists and cannot be redefined' % key)
        return super(DAL, self).__setattr__(key, value)

    def __repr__(self):
        if hasattr(self, '_uri'):
            return '<DAL uri="%s">' % hide_password(self._adapter.uri)
        else:
            return '<DAL db_uid="%s">' % self._db_uid

    def smart_query(self, fields, text):
        return Set(self, smart_query(fields, text))

    def __call__(self, query=None, ignore_common_filters=None):
        return self.where(query, ignore_common_filters)

    def where(self, query=None, ignore_common_filters=None):
        if isinstance(query, Table):
            query = self._adapter.id_query(query)
        elif isinstance(query, Field):
            query = query != None
        elif isinstance(query, dict):
            icf = query.get("ignore_common_filters")
            if icf:
                ignore_common_filters = icf
        return Set(self, query, ignore_common_filters=ignore_common_filters)

    def commit(self):
        self._adapter.commit()

    def rollback(self):
        self._adapter.rollback()

    def close(self):
        self._adapter.close()
        if self._db_uid in THREAD_LOCAL._pydal_db_instances_:
            db_group = THREAD_LOCAL._pydal_db_instances_[self._db_uid]
            db_group.remove(self)
            if not db_group:
                del THREAD_LOCAL._pydal_db_instances_[self._db_uid]
        self._adapter._clean_tlocals()

    def executesql(self, query, placeholders=None, as_dict=False,
                   fields=None, colnames=None, as_ordered_dict=False):
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
            if not hasattr(adapter.cursor,'description'):
                raise RuntimeError("database does not support executesql(...,as_dict=True)")
            # Non-DAL legacy db query, converts cursor results to dict.
            # sequence of 7-item sequences. each sequence tells about a column.
            # first item is always the field name according to Python Database API specs
            columns = adapter.cursor.description
            # reduce the column info down to just the field names
            fields = colnames or [f[0] for f in columns]
            if len(fields) != len(set(fields)):
                raise RuntimeError("Result set includes duplicate column names. Specify unique column names using the 'colnames' argument")
            #: avoid bytes strings in columns names (py3)
            if columns and not PY2:
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
        except:
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
                newcolnames = []
                for tf in colnames:
                    if '.' in tf:
                        newcolnames.append('.'.join(adapter.dialect.quote(f)
                                                    for f in tf.split('.')))
                    else:
                        newcolnames.append(tf)
                colnames = newcolnames
            data = adapter.parse(
                data, fields=extracted_fields, colnames=colnames)
        return data

    def _remove_references_to(self, thistable):
        for table in self:
            table._referenced_by = [field for field in table._referenced_by
                                    if not field.table==thistable]

    def has_representer(self, name):
        return callable(self.representers.get(name))

    def represent(self, name, *args, **kwargs):
        return self.representers[name](*args, **kwargs)

    def export_to_csv_file(self, ofile, *args, **kwargs):
        step = long(kwargs.get('max_fetch_rows,',500))
        write_colnames = kwargs['write_colnames'] = \
            kwargs.get("write_colnames", True)
        for table in self.tables:
            ofile.write('TABLE %s\r\n' % table)
            query = self._adapter.id_query(self[table])
            nrows = self(query).count()
            kwargs['write_colnames'] = write_colnames
            for k in range(0,nrows,step):
                self(query).select(limitby=(k,k+step)).export_to_csv_file(
                    ofile, *args, **kwargs)
                kwargs['write_colnames'] = False
            ofile.write('\r\n\r\n')
        ofile.write('END')

    def import_from_csv_file(self, ifile, id_map=None, null='<NULL>',
                             unique='uuid', map_tablenames=None,
                             ignore_missing_tables=False,
                             *args, **kwargs):
        #if id_map is None: id_map={}
        id_offset = {} # only used if id_map is None
        map_tablenames = map_tablenames or {}
        for line in ifile:
            line = line.strip()
            if not line:
                continue
            elif line == 'END':
                return
            elif not line.startswith('TABLE ') or \
                    not line[6:] in self.tables:
                raise SyntaxError('invalid file format')
            else:
                tablename = line[6:]
                tablename = map_tablenames.get(tablename,tablename)
                if tablename is not None and tablename in self.tables:
                    self[tablename].import_from_csv_file(
                        ifile, id_map, null, unique, id_offset,
                        *args, **kwargs)
                elif tablename is None or ignore_missing_tables:
                    # skip all non-empty lines
                    for line in ifile:
                        if not line.strip():
                            break
                else:
                    raise RuntimeError("Unable to import table that does not exist.\nTry db.import_from_csv_file(..., map_tablenames={'table':'othertable'},ignore_missing_tables=True)")

    def can_join(self):
        return self._adapter.can_join()


def DAL_unpickler(db_uid):
    return DAL('<zombie>', db_uid=db_uid)


def DAL_pickler(db):
    return DAL_unpickler, (db._db_uid,)

copyreg.pickle(DAL, DAL_pickler, DAL_unpickler)
