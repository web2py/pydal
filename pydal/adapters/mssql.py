import re
from .._compat import PY2, iteritems, integer_types, to_unicode, long
from .._globals import IDENTITY
from .base import SQLAdapter
from ..utils import split_uri_args
from . import adapters, with_connection_or_raise

class Slicer(object):
    def rowslice(self, rows, minimum=0, maximum=None):
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]


class MSSQL(SQLAdapter):
    dbengine = 'mssql'
    drivers = ('pyodbc',)

    REGEX_DSN = '^.+$'
    REGEX_URI = \
         '^(?P<user>[^:@]+)(:(?P<password>[^@]*))?' \
        r'@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?' \
         '/(?P<db>[^?]+)' \
        r'(\?(?P<uriargs>.*))?$'

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
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            self.dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError(
                    "Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group('user'))
            password = self.credential_decoder(m.group('password'))
            if password is None:
                password = ''
            host = m.group('host')
            db = m.group('db')
            port = m.group('port') or '1433'
            # Parse the optional uri name-value arg pairs after the '?'
            # (in the form of arg1=value1&arg2=value2&...)
            # (drivers like FreeTDS insist on uppercase parameter keys)
            argsdict = {'DRIVER': '{SQL Server}'}
            uriargs = m.group('uriargs')
            if uriargs:
                for argkey, argvalue in split_uri_args(
                        uriargs, separators='&', need_equal=True).items():
                    argsdict[argkey.upper()] = argvalue
            uriargs = ';'.join([
                '%s=%s' % (ak, av) for (ak, av) in iteritems(argsdict)])
            self.dsn = 'SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;%s' \
                % (host, port, db, user, password, uriargs)

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)

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
        if field_type in ('string', 'text', 'json') and rv.startswith("'"):
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
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError(
                    "Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group('user'))
            password = self.credential_decoder(m.group('password'))
            if password is None:
                password = ''
            host = m.group('host')
            db = m.group('db')
            port = m.group('port') or '1433'
            self.dsn = 'sybase:host=%s:%s;dbname=%s' % (host, port, db)
            self.driver_args.update(
                user=self.credential_decoder(user),
                passwd=self.credential_decoder(password))
