from .._compat import integer_types, long
from .base import SQLAdapter
from . import adapters, with_connection_or_raise


class DB2(SQLAdapter):
    dbengine = "db2"

    def _initialize_(self, do_connect):
        super(DB2, self)._initialize_(do_connect)
        self.ruri = self.uri.split('://', 1)[1]

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        command = self.filter_sql_command(args[0])
        if command[-1:] == ';':
            command = command[:-1]
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        if kwargs.get('placeholders'):
            args.append(kwargs['placeholders'])
            del kwargs['placeholders']
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def lastrowid(self, table):
        self.execute("SELECT DISTINCT IDENTITY_VAL_LOCAL() FROM %s;" % table)
        return long(self.cursor.fetchone()[0])

    def rowslice(self, rows, minimum=0, maximum=None):
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]


@adapters.register_for('db2:ibm_db_dbi')
class DB2IBM(DB2):
    drivers = ('ibm_db_dbi',)

    def connector(self):
        uriparts = self.ruri.split(";")
        cnxn = {}
        for var in uriparts:
            v = var.split('=')
            cnxn[v[0].lower()] = v[1]
        return self.driver.connect(
            cnxn['dsn'], cnxn['uid'], cnxn['pwd'], **self.driver_args)


@adapters.register_for('db2:pyodbc')
class DB2Pyodbc(DB2):
    drivers = ('pyodbc',)

    def connector(self):
        return self.driver.connect(self.ruri, **self.driver_args)
