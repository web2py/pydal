import re
from .._compat import PY2, iteritems, integer_types, to_unicode, long
from .._globals import IDENTITY
from .base import SQLAdapter
from . import adapters, with_connection_or_raise

class Slicer(object):
    def rowslice(self, rows, minimum=0, maximum=None):
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]


class MSSQL(SQLAdapter):
    dbengine = 'mssql'
    drivers = ('pyodbc',)

    REGEX_DSN = re.compile('^(?P<dsn>.+)$')
    REGEX_URI = re.compile(
        '^(?P<user>[^:@]+)(\:(?P<password>[^@]*))?@(?P<host>\[[^/]+\]|' +
        '[^\:/]+)(\:(?P<port>[0-9]+))?/(?P<db>[^\?]+)(\?(?P<urlargs>.*))?$')
    REGEX_ARGPATTERN = re.compile('(?P<argkey>[^=]+)=(?P<argvalue>[^&]*)')

    def __init__(self, db, uri, pool_size=0, folder=None, db_codec='UTF-8',
                 credential_decoder=IDENTITY, driver_args={},
                 adapter_args={}, do_connect=True, srid=4326,
                 after_connection=None):
        self.srid = srid
        super(MSSQL, self).__init__(
            db, uri, pool_size, folder, db_codec, credential_decoder,
            driver_args, adapter_args, do_connect, after_connection)

    def _initialize_(self, do_connect):
        super(MSSQL, self)._initialize_(do_connect)
        ruri = self.uri.split('://', 1)[1]
        if '@' not in ruri:
            try:
                m = self.REGEX_DSN.match(ruri)
                if not m:
                    raise SyntaxError(
                        'Parsing uri string(%s) has no result' % self.uri)
                dsn = m.group('dsn')
                if not dsn:
                    raise SyntaxError('DSN required')
            except SyntaxError as e:
                self.db.logger.error('NdGpatch error')
                raise e
            self.cnxn = dsn
        else:
            m = self.REGEX_URI.match(ruri)
            if not m:
                raise SyntaxError(
                    "Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group('user'))
            if not user:
                raise SyntaxError('User required')
            password = self.credential_decoder(m.group('password'))
            if not password:
                password = ''
            host = m.group('host')
            if not host:
                raise SyntaxError('Host name required')
            db = m.group('db')
            if not db:
                raise SyntaxError('Database name required')
            port = m.group('port') or '1433'
            # Parse the optional url name-value arg pairs after the '?'
            # (in the form of arg1=value1&arg2=value2&...)
            # (drivers like FreeTDS insist on uppercase parameter keys)
            argsdict = {'DRIVER': '{SQL Server}'}
            urlargs = m.group('urlargs') or ''
            for argmatch in self.REGEX_ARGPATTERN.finditer(urlargs):
                argsdict[str(argmatch.group('argkey')).upper()] = \
                    argmatch.group('argvalue')
            urlargs = ';'.join([
                '%s=%s' % (ak, av) for (ak, av) in iteritems(argsdict)])
            self.cnxn = 'SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;%s' \
                % (host, port, db, user, password, urlargs)

    def connector(self):
        return self.driver.connect(self.cnxn, **self.driver_args)

    def lastrowid(self, table):
        self.execute('SELECT SCOPE_IDENTITY();')
        return long(self.cursor.fetchone()[0])


@adapters.register_for('mssql')
class MSSQL1(MSSQL, Slicer):
    pass


@adapters.register_for('mssql3')
class MSSQL3(MSSQL):
    pass


@adapters.register_for('mssql4')
class MSSQL4(MSSQL):
    pass


class MSSQLN(MSSQL):
    def represent(self, obj, field_type):
        rv = super(MSSQLN, self).represent(obj, field_type)
        if field_type in ('string', 'text', 'json') and rv[:1] == "'":
            rv = 'N' + rv
        return rv

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        if PY2:
            args = list(args)
            args[0] = to_unicode(args[0])
        return super(MSSQLN, self).execute(*args, **kwargs)


@adapters.register_for('mssqln', 'mssql2')
class MSSQL1N(MSSQLN, Slicer):
    pass


@adapters.register_for('mssql3n')
class MSSQL3N(MSSQLN):
    pass


@adapters.register_for('mssql4n')
class MSSQL4N(MSSQLN):
    pass


@adapters.register_for('vertica')
class Vertica(MSSQL1):
    def lastrowid(self, table):
        self.execute('SELECT SCOPE_IDENTITY();')
        return long(self.cursor.fetchone()[0])


@adapters.register_for('sybase')
class Sybase(MSSQL1):
    dbengine = 'sybase'

    def _initialize_(self, do_connect):
        super(MSSQL, self)._initialize_(do_connect)
        ruri = self.uri.split('://', 1)[1]
        if '@' not in ruri:
            try:
                m = self.REGEX_DSN.match(ruri)
                if not m:
                    raise SyntaxError(
                        'Parsing uri string(%s) has no result' % self.uri)
                dsn = m.group('dsn')
                if not dsn:
                    raise SyntaxError('DSN required')
            except SyntaxError as e:
                self.db.logger.error('NdGpatch error')
                raise e
            self.cnxn = dsn
        else:
            m = self.REGEX_URI.match(ruri)
            if not m:
                raise SyntaxError(
                    "Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group('user'))
            if not user:
                raise SyntaxError('User required')
            password = self.credential_decoder(m.group('password'))
            if not password:
                password = ''
            host = m.group('host')
            if not host:
                raise SyntaxError('Host name required')
            db = m.group('db')
            if not db:
                raise SyntaxError('Database name required')
            port = m.group('port') or '1433'
            self.dsn = 'sybase:host=%s:%s;dbname=%s' % (host, port, db)
            self.driver_args.update(
                user=self.credential_decoder(user),
                passwd=self.credential_decoder(password))

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)
