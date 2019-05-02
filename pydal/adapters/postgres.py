import re
import os.path
from .._compat import PY2, with_metaclass, iterkeys, to_unicode, long
from .._globals import IDENTITY, THREAD_LOCAL
from ..drivers import psycopg2_adapt
from ..helpers.classes import ConnectionConfigurationMixin
from .base import SQLAdapter
from ..utils import split_uri_args
from . import AdapterMeta, adapters, with_connection, with_connection_or_raise


class PostgreMeta(AdapterMeta):
    def __call__(cls, *args, **kwargs):
        if cls not in [Postgre, PostgreNew, PostgreBoolean]:
            return AdapterMeta.__call__(cls, *args, **kwargs)
        # choose driver according uri
        available_drivers = [
            driver for driver in cls.drivers
            if driver in iterkeys(kwargs['db']._drivers_available)]
        uri_items = kwargs['uri'].split('://', 1)[0].split(':')
        uri_driver = uri_items[1] if len(uri_items) > 1 else None
        if uri_driver and uri_driver in available_drivers:
            driver = uri_driver
        else:
            driver = available_drivers[0] if available_drivers else \
                cls.drivers[0]
        cls = adapters._registry_[uri_items[0] + ":" + driver]
        return AdapterMeta.__call__(cls, *args, **kwargs)


@adapters.register_for('postgres')
class Postgre(
    with_metaclass(PostgreMeta, ConnectionConfigurationMixin, SQLAdapter)
):
    dbengine = 'postgres'
    drivers = ('psycopg2', 'pg8000')
    support_distributed_transaction = True

    REGEX_URI = \
         '^(?P<user>[^:@]+)(:(?P<password>[^@]*))?' \
        r'@(?P<host>[^:/]*|\[[^\]]+\])(:(?P<port>\d+))?' \
         '/(?P<db>[^?]+)' \
        r'(\?(?P<uriargs>.*))?$'  # sslmode, ssl (no value) and unix_socket

    def __init__(self, db, uri, pool_size=0, folder=None, db_codec='UTF-8',
                 credential_decoder=IDENTITY, driver_args={},
                 adapter_args={}, do_connect=True, srid=4326,
                 after_connection=None):
        self.srid = srid
        super(Postgre, self).__init__(
            db, uri, pool_size, folder, db_codec, credential_decoder,
            driver_args, adapter_args, do_connect, after_connection)

    def _initialize_(self, do_connect):
        super(Postgre, self)._initialize_(do_connect)
        ruri = self.uri.split('://', 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group('user'))
        password = self.credential_decoder(m.group('password'))
        host = m.group('host')
        uriargs = m.group('uriargs')
        if uriargs:
            uri_args = split_uri_args(uriargs, need_equal=False)
        else:
            uri_args = dict()
        socket = uri_args.get('unix_socket')
        if not host and not socket:
            raise SyntaxError('Host or UNIX socket name required')
        db = m.group('db')
        self.driver_args.update(user=user, database=db)
        if password is not None:
            self.driver_args['password'] = password
        if socket:
            if not os.path.exists(socket):
                raise ValueError("UNIX socket %r not found" % socket)
            if self.driver_name == 'psycopg2':
                # the psycopg2 driver let you configure the socket directory
                # only (not the socket file name) by passing it as the host
                # (must be an absolute path otherwise the driver tries a TCP/IP
                # connection to host); this behaviour is due to the underlying
                # libpq used by the driver
                socket_dir = os.path.abspath(os.path.dirname(socket))
                self.driver_args['host'] = socket_dir
            elif self.driver_name == 'pg8000':
                self.driver_args['unix_sock'] = socket
        else:
            port = int(m.group('port') or 5432)
            self.driver_args.update(host=host, port=port)
            sslmode = uri_args.get('sslmode')
            if sslmode and self.driver_name == 'psycopg2':
                self.driver_args['sslmode'] = sslmode
            if 'ssl' in uri_args and self.driver_name == 'pg8000':
                self.driver_args['ssl'] = True
        if self.driver:
            self.__version__ = "%s %s" % (self.driver.__name__,
                                          self.driver.__version__)
        else:
            self.__version__ = None
        THREAD_LOCAL._pydal_last_insert_ = None
        self._mock_reconnect()

    def _get_json_dialect(self):
        from ..dialects.postgre import PostgreDialectJSON
        return PostgreDialectJSON

    def _get_json_parser(self):
        from ..parsers.postgre import PostgreAutoJSONParser
        return PostgreAutoJSONParser

    @property
    def _last_insert(self):
        return THREAD_LOCAL._pydal_last_insert_

    @_last_insert.setter
    def _last_insert(self, value):
        THREAD_LOCAL._pydal_last_insert_ = value

    def connector(self):
        return self.driver.connect(**self.driver_args)

    def after_connection(self):
        self.execute("SET CLIENT_ENCODING TO 'UTF8'")
        self.execute("SET standard_conforming_strings=on;")

    def _configure_on_first_reconnect(self):
        self._config_json()

    def lastrowid(self, table):
        if self._last_insert:
            return long(self.cursor.fetchone()[0])
        sequence_name = table._sequence_name
        self.execute("SELECT currval(%s);" % self.adapt(sequence_name))
        return long(self.cursor.fetchone()[0])

    def _insert(self, table, fields):
        self._last_insert = None
        if fields:
            retval = None
            if hasattr(table, '_id'):
                self._last_insert = (table._id, 1)
                retval = table._id._rname
            return self.dialect.insert(
                table._rname,
                ','.join(el[0]._rname for el in fields),
                ','.join(self.expand(v, f.type) for f, v in fields),
                retval)
        return self.dialect.insert_empty(table._rname)

    @with_connection
    def prepare(self, key):
        self.execute("PREPARE TRANSACTION '%s';" % key)

    @with_connection
    def commit_prepared(self, key):
        self.execute("COMMIT PREPARED '%s';" % key)

    @with_connection
    def rollback_prepared(self, key):
        self.execute("ROLLBACK PREPARED '%s';" % key)


@adapters.register_for('postgres:psycopg2')
class PostgrePsyco(Postgre):
    drivers = ('psycopg2',)

    def _config_json(self):
        use_json = self.driver.__version__ >= "2.0.12" and \
            self.connection.server_version >= 90200
        if use_json:
            self.dialect = self._get_json_dialect()(self)
            if self.driver.__version__ >= '2.5.0':
                self.parser = self._get_json_parser()(self)

    def adapt(self, obj):
        adapted = psycopg2_adapt(obj)
        # deal with new relic Connection Wrapper (newrelic>=2.10.0.8)
        cxn = getattr(self.connection, '__wrapped__', self.connection)
        adapted.prepare(cxn)
        rv = adapted.getquoted()
        if not PY2:
            if isinstance(rv, bytes):
                return rv.decode('utf-8')
        return rv


@adapters.register_for('postgres:pg8000')
class PostgrePG8000(Postgre):
    drivers = ('pg8000',)

    def _config_json(self):
        if self.connection._server_version >= "9.2.0":
            self.dialect = self._get_json_dialect()(self)
            if self.driver.__version__ >= '1.10.2':
                self.parser = self._get_json_parser()(self)

    def adapt(self, obj):
        return "'%s'" % obj.replace("%", "%%").replace("'", "''")

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        if PY2:
            args = list(args)
            args[0] = to_unicode(args[0])
        return super(PostgrePG8000, self).execute(*args, **kwargs)


@adapters.register_for('postgres2')
class PostgreNew(Postgre):
    def _get_json_dialect(self):
        from ..dialects.postgre import PostgreDialectArraysJSON
        return PostgreDialectArraysJSON

    def _get_json_parser(self):
        from ..parsers.postgre import PostgreNewAutoJSONParser
        return PostgreNewAutoJSONParser


@adapters.register_for('postgres2:psycopg2')
class PostgrePsycoNew(PostgrePsyco, PostgreNew):
    pass


@adapters.register_for('postgres2:pg8000')
class PostgrePG8000New(PostgrePG8000, PostgreNew):
    pass


@adapters.register_for('postgres3')
class PostgreBoolean(PostgreNew):
    def _get_json_dialect(self):
        from ..dialects.postgre import PostgreDialectBooleanJSON
        return PostgreDialectBooleanJSON

    def _get_json_parser(self):
        from ..parsers.postgre import PostgreBooleanAutoJSONParser
        return PostgreBooleanAutoJSONParser


@adapters.register_for('postgres3:psycopg2')
class PostgrePsycoBoolean(PostgrePsycoNew, PostgreBoolean):
    pass


@adapters.register_for('postgres3:pg8000')
class PostgrePG8000Boolean(PostgrePG8000New, PostgreBoolean):
    pass


@adapters.register_for('jdbc:postgres')
class JDBCPostgre(Postgre):
    drivers = ('zxJDBC',)

    REGEX_URI = \
         '^(?P<user>[^:@]+)(:(?P<password>[^@]*))?' \
        r'@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?' \
         '/(?P<db>[^?]+)$'

    def _initialize_(self, do_connect):
        super(Postgre, self)._initialize_(do_connect)
        ruri = self.uri.split('://', 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group('user'))
        password = self.credential_decoder(m.group('password'))
        if password is None:
            password = ''
        host = m.group('host')
        db = m.group('db')
        port = m.group('port') or '5432'
        self.dsn = (
            'jdbc:postgresql://%s:%s/%s' % (host, port, db), user, password)
        if self.driver:
            self.__version__ = "%s %s" % (self.driver.__name__,
                                          self.driver.__version__)
        else:
            self.__version__ = None
        THREAD_LOCAL._pydal_last_insert_ = None
        self._mock_reconnect()

    def connector(self):
        return self.driver.connect(*self.dsn, **self.driver_args)

    def after_connection(self):
        self.connection.set_client_encoding('UTF8')
        self.execute('BEGIN;')
        self.execute("SET CLIENT_ENCODING TO 'UNICODE';")

    def _config_json(self):
        use_json = self.connection.dbversion >= "9.2.0"
        if use_json:
            self.dialect = self._get_json_dialect()(self)
