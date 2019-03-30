import re
import sys
from .._compat import integer_types, long
from ..helpers.classes import Reference
from .base import SQLAdapter
from . import adapters, with_connection_or_raise

@adapters.register_for('oracle')
class Oracle(SQLAdapter):
    dbengine = 'oracle'
    drivers = ('cx_Oracle',)

    REGEX_CLOB = r"[^']*('[^']*'[^']*)*:(?P<clob>CLOB\('([^']+|'')*'\))"

    def _initialize_(self, do_connect):
        super(Oracle, self)._initialize_(do_connect)
        self.ruri = self.uri.split('://', 1)[1]
        if 'threaded' not in self.driver_args:
            self.driver_args['threaded'] = True

    def connector(self):
        return self.driver.connect(self.ruri, **self.driver_args)

    def after_connection(self):
        self.execute(
            "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';")
        self.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = " +
            "'YYYY-MM-DD HH24:MI:SS';")

    def test_connection(self):
        self.execute('SELECT 1 FROM DUAL;')

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        command = self.filter_sql_command(args[0])
        i = 1
        while True:
            m = re.match(self.REGEX_CLOB, command)
            if not m:
                break
            command = command[:m.start('clob')] + str(i) + \
                command[m.end('clob'):]
            args.append(m.group('clob')[6:-2].replace("''", "'"))
            i += 1
        if command[-1:] == ';':
            command = command[:-1]
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def lastrowid(self, table):
        sequence_name = table._sequence_name
        self.execute('SELECT %s.currval FROM dual;' % sequence_name)
        return long(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        tablename = table._rname
        id_name = table._id._rname
        sequence_name = table._sequence_name
        trigger_name = table._trigger_name
        self.execute(query)
        self.execute(
            'CREATE SEQUENCE %s START WITH 1 INCREMENT BY 1 NOMAXVALUE MINVALUE -1;' 
            % sequence_name)
        self.execute(_trigger_sql % dict(
            trigger_name=trigger_name, tablename=tablename,
            sequence_name=sequence_name,
            id=id_name)
        )

    def _select_aux_execute(self, sql):
        self.execute(sql)
        return self.fetchall()

    def fetchall(self):
        from ..drivers import cx_Oracle
        if any(x[1] == cx_Oracle.LOB or x[1] == cx_Oracle.CLOB
               for x in self.cursor.description):
            return [tuple(
                [(c.read() if type(c) == cx_Oracle.LOB else c) for c in r]
            ) for r in self.cursor]
        else:
            return self.cursor.fetchall()

    def sqlsafe_table(self, tablename, original_tablename=None):
        if original_tablename is not None:
            return (
                self.dialect.quote_template + ' ' +
                self.dialect.quote_template
            ) % (original_tablename, tablename)
        return self.dialect.quote(tablename)

    def _build_value_for_insert(self, field, value, r_values):
        if field.type is 'text':
            r_values[':' + field._rname] = self.expand(value, field.type)
            return ':' + field._rname
        return self.expand(value, field.type)

    def _insert(self, table, fields):
        if fields:
            r_values = {}
            return self.dialect.insert(
                table._rname,
                ','.join(el[0]._rname for el in fields),
                ','.join(
                    self._build_value_for_insert(f, v, r_values)
                    for f, v in fields)
                ), r_values
        return self.dialect.insert_empty(table._rname), None

    def insert(self, table, fields):
        query, values = self._insert(table, fields)
        try:
            if not values:
                self.execute(query)
            else:
                self.execute(query, *values)
        except:
            e = sys.exc_info()[1]
            if hasattr(table, '_on_insert_error'):
                return table._on_insert_error(table, fields, e)
            raise e
        if hasattr(table, '_primarykey'):
            pkdict = dict([
                (k[0].name, k[1]) for k in fields
                if k[0].name in table._primarykey])
            if pkdict:
                return pkdict
        id = self.lastrowid(table)
        if hasattr(table, '_primarykey') and len(table._primarykey) == 1:
            id = {table._primarykey[0]: id}
        if not isinstance(id, integer_types):
            return id
        rid = Reference(id)
        (rid._table, rid._record) = (table, None)
        return rid

_trigger_sql = """
CREATE OR REPLACE TRIGGER %(trigger_name)s BEFORE INSERT ON %(tablename)s FOR EACH ROW
DECLARE
    curr_val NUMBER;
    diff_val NUMBER;
    PRAGMA autonomous_transaction;
BEGIN
    IF :NEW.%(id)s IS NOT NULL THEN
        EXECUTE IMMEDIATE 'SELECT %(sequence_name)s.nextval FROM dual' INTO curr_val;
        diff_val := :NEW.%(id)s - curr_val - 1;
        IF diff_val != 0 THEN
          EXECUTE IMMEDIATE 'alter sequence %(sequence_name)s increment by '|| diff_val;
          EXECUTE IMMEDIATE 'SELECT %(sequence_name)s.nextval FROM dual' INTO curr_val;
          EXECUTE IMMEDIATE 'alter sequence %(sequence_name)s increment by 1';
        END IF;
    END IF;
    SELECT %(sequence_name)s.nextval INTO :NEW.%(id)s FROM DUAL;
END;"""
