import re
from .._compat import integer_types, long
from .base import SQLAdapter
from . import adapters

@adapters.register_for('sapdb')
class SAPDB(SQLAdapter):
    dbengine = 'sapdb'
    drivers = ('sapdb',)

    REGEX_URI = re.compile(
        '^(?P<user>[^:@]+)(\:(?P<password>[^@]*))?@(?P<host>\[[^/]+\]|' +
        '[^\:@]+)(\:(?P<port>[0-9]+))?/(?P<db>[^\?]+)' +
        '(\?sslmode=(?P<sslmode>.+))?$')

    def _initialize_(self, do_connect):
        super(SAPDB, self)._initialize_(do_connect)
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
        self.driver_args.update(
            user=user, password=password, database=db, host=host)

    def connector(self):
        self.driver.connect(**self.driver_args)

    def lastrowid(self, table):
        self.execute("select %s.NEXTVAL from dual" % table._sequence_name)
        return long(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        self.execute('CREATE SEQUENCE %s;' % table._sequence_name)
        self.execute(
            "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s');" %
            (table._rname, table._id._rname, table._sequence_name))
        self.execute(query)
