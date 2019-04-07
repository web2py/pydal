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

    REGEX_URI = \
         '^(?P<user>[^:@]+)(:(?P<password>[^@]*))?' \
        r'@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?' \
         '/(?P<db>[^?]+)' \
        r'(\?set_encoding=(?P<charset>\w+))?$'

    def _initialize_(self, do_connect):
        super(FireBird, self)._initialize_(do_connect)
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
        port = int(m.group('port') or 3050)
        charset = m.group('charset') or 'UTF8'
        self.driver_args.update(
            dsn='%s/%s:%s' % (host, port, db),
            user=user, password=password, charset=charset)

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
        qid = self.dialect.quote_template % 'id'
        self.execute(
            'create trigger %s for %s active before insert position 0 as\n' \
            'begin\nif(new.%s is null) then new.%s = gen_id(%s, 1);\n' \
            'end;' % (trigger_name, tablename, qid, qid, sequence_name))


@adapters.register_for('firebird_embedded')
class FireBirdEmbedded(FireBird):
    REGEX_URI = \
         '^(?P<user>[^:@]+)(:(?P<password>[^@]*))?' \
        r'@(?P<path>[^?]+)(\?set_encoding=(?P<charset>\w+))?$'

    def _initialize_(self, do_connect):
        super(FireBird, self)._initialize_(do_connect)
        ruri = self.uri.split('://', 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group('user'))
        password = self.credential_decoder(m.group('password'))
        if password is None:
            password = ''
        pathdb = m.group('path')
        charset = m.group('charset') or 'UTF8'
        self.driver_args.update(
            host='', database=pathdb,
            user=user, password=password, charset=charset)
