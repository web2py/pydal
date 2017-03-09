import re
from .._compat import long
from .base import SQLAdapter
from . import adapters


@adapters.register_for('firebird')
class FireBird(SQLAdapter):
    dbengine = "firebird"
    drivers = ('kinterbasdb', 'firebirdsql', 'fdb', 'pyodbc')

    support_distributed_transaction = True
    commit_on_alter_table = True

    REGEX_URI = re.compile(
        '^(?P<user>[^:@]+)(\:(?P<password>[^@]*))?@(?P<host>\[[^/]+\]|' +
        '[^\:/]+)(\:(?P<port>[0-9]+))?/(?P<db>.+?)' +
        '(\?set_encoding=(?P<charset>\w+))?$')

    def _initialize_(self, do_connect):
        super(FireBird, self)._initialize_(do_connect)
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
        if not host:
            raise SyntaxError('Host name required')
        db = m.group('db')
        if not db:
            raise SyntaxError('Database name required')
        port = int(m.group('port') or 3050)
        charset = m.group('charset') or 'UTF8'
        self.driver_args.update(
            dsn='%s/%s:%s' % (host, port, db),
            user=self.credential_decoder(user),
            password=self.credential_decoder(password),
            charset=charset)

    def connector(self):
        return self.driver.connect(**self.driver_args)

    def lastrowid(self, table):
        sequence_name = table._sequence_name
        self.execute('SELECT gen_id(%s, 0) FROM rdb$database' % sequence_name)
        return long(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        tablename = table._rname
        sequence_name = table._sequence_name
        trigger_name = table._trigger_name
        self.execute(query)
        self.execute('create generator %s;' % sequence_name)
        self.execute('set generator %s to 0;' % sequence_name)
        self.execute(
            'create trigger %s for %s active before insert position 0 ' +
            'as\nbegin\nif(new."id" is null) then\nbegin\n' +
            'new."id" = gen_id(%s, 1);\nend\nend;' % (
                trigger_name, tablename, sequence_name))


@adapters.register_for('firebird_embedded')
class FireBirdEmbedded(FireBird):
    REGEX_URI = re.compile(
        '^(?P<user>[^:@]+)(\:(?P<password>[^@]*))?@(?P<path>[^\?]+)' +
        '(\?set_encoding=(?P<charset>\w+))?$')

    def _initialize_(self, do_connect):
        super(FireBird, self)._initialize_(do_connect)
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
        pathdb = m.group('path')
        if not pathdb:
            raise SyntaxError('Path required')
        charset = m.group('charset') or 'UTF8'
        self.driver_args.update(
            host='', database=pathdb, user=self.credential_decoder(user),
            password=self.credential_decoder(password), charset=charset)
