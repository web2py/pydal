import re
from .base import SQLAdapter
from ..utils import split_uri_args
from . import adapters, with_connection


@adapters.register_for('mysql')
class MySQL(SQLAdapter):
    dbengine = 'mysql'
    drivers = ('MySQLdb', 'pymysql', 'mysqlconnector')
    commit_on_alter_table = True
    support_distributed_transaction = True

    REGEX_URI = \
         '^(?P<user>[^:@]+)(:(?P<password>[^@]*))?' \
        r'@(?P<host>[^:/]*|\[[^\]]+\])(:(?P<port>\d+))?' \
         '/(?P<db>[^?]+)' \
        r'(\?(?P<uriargs>.*))?$'  # set_encoding and unix_socket

    def _initialize_(self, do_connect):
        super(MySQL, self)._initialize_(do_connect)
        ruri = self.uri.split('://', 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group('user'))
        password = self.credential_decoder(m.group('password'))
        host = m.group('host')
        uriargs = m.group('uriargs')
        if uriargs:
            uri_args = split_uri_args(uriargs, need_equal=True)
            charset = uri_args.get('set_encoding') or 'utf8'
            socket = uri_args.get('unix_socket')
        else:
            charset = 'utf8'
            socket = None
        # NOTE:
        # MySQLdb (see http://mysql-python.sourceforge.net/MySQLdb.html)
        # use UNIX sockets and named pipes by default if no host is given
        # or host is 'localhost'; as opposed to
        # pymysql (see https://pymysql.readthedocs.io/en/latest/modules/connections.html)
        # or mysqlconnector (see https://dev.mysql.com/doc/connectors/en/connector-python-connectargs.html)
        # driver, where you have to specify the socket explicitly.
        if not host and not socket:
            raise SyntaxError('Host or UNIX socket name required')
        db = m.group('db')
        port = int(m.group('port') or '3306')
        self.driver_args.update(user=user, db=db, charset=charset)
        if password is not None:
            self.driver_args['passwd'] = password
        if socket:
            self.driver_args['unix_socket'] = socket
        else:
            self.driver_args.update(host=host, port=port)

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
