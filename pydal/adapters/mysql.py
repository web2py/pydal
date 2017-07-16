import re
from .base import SQLAdapter
from . import adapters, with_connection


@adapters.register_for('mysql')
class MySQL(SQLAdapter):
    dbengine = 'mysql'
    drivers = ('MySQLdb', 'pymysql', 'mysqlconnector')
    commit_on_alter_table = True
    support_distributed_transaction = True

    REGEX_URI = re.compile(
        '^(?P<user>[^:@]+)(\:(?P<password>[^@]*))?@(?P<host>\[[^/]+\]|' +
        '[^\:/]*)(\:(?P<port>[0-9]+))?/(?P<db>[^?]+)(\?set_encoding=' +
        '(?P<charset>\w+))?(\?unix_socket=(?P<socket>.+))?$')

    def _initialize_(self, do_connect):
        super(MySQL, self)._initialize_(do_connect)
        ruri = self.uri.split('://', 1)[1]
        m = self.REGEX_URI.match(ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group('user'))
        if not user:
            raise SyntaxError('User required')
        password = self.credential_decoder(m.group('password'))
        if not password:
            password = ''
        host = m.group('host')
        socket = m.group('socket')
        if not host and not socket:
            raise SyntaxError('Host name required')
        db = m.group('db')
        if not db and not socket:
            raise SyntaxError('Database name required')
        port = int(m.group('port') or '3306')
        charset = m.group('charset') or 'utf8'
        if socket:
            self.driver_args.update(
                unix_socket=socket,
                user=user, passwd=password,
                charset=charset)
            if db:
                self.driver_args.update(db=db)
        else:
            self.driver_args.update(
                db=db, user=user, passwd=password, host=host, port=port,
                charset=charset)

    def connector(self):
        return self.driver.connect(**self.driver_args)

    def after_connection(self):
        self.execute("SET FOREIGN_KEY_CHECKS=1;")
        self.execute("SET sql_mode='NO_BACKSLASH_ESCAPES';")

    def distributed_transaction_begin(self, key):
        self.execute("XA START;")

    @with_connection
    def prepare(self, key):
        self.execute("XA END;")
        self.execute("XA PREPARE;")

    @with_connection
    def commit_prepared(self, key):
        self.execute("XA COMMIT;")

    @with_connection
    def rollback_prepared(self, key):
        self.execute("XA ROLLBACK;")


@adapters.register_for('cubrid')
class Cubrid(MySQL):
    dbengine = "cubrid"
    drivers = ('cubriddb',)

    def _initialize_(self, do_connect):
        super(Cubrid, self)._initialize_(do_connect)
        del self.driver_args['charset']
