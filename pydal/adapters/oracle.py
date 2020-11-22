import re
import sys
import os
from .._compat import integer_types, long
from ..helpers.classes import Reference
from ..helpers.methods import use_common_filters
from .base import SQLAdapter
from ..objects import Table, Field, Expression, Query
from . import adapters, with_connection, with_connection_or_raise


@adapters.register_for("oracle")
class Oracle(SQLAdapter):
    dbengine = "oracle"
    drivers = ("cx_Oracle",)

    cmd_fix = re.compile("[^']*('[^']*'[^']*)*\:(?P<clob>(C|B)LOB\('([^']+|'')*'\))")

    def _initialize_(self, do_connect):
        super(Oracle, self)._initialize_(do_connect)
        self.ruri = self.uri.split("://", 1)[1]
        if "threaded" not in self.driver_args:
            self.driver_args["threaded"] = True
        # set character encoding defaults
        if "encoding" not in self.driver_args:
            self.driver_args["encoding"] = "UTF-8"
        if "nencoding" not in self.driver_args:
            self.driver_args["nencoding"] = "UTF-8"

    def connector(self):
        return self.driver.connect(self.ruri, **self.driver_args)

    def after_connection(self):
        self.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';")
        self.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = " + "'YYYY-MM-DD HH24:MI:SS';"
        )

    def test_connection(self):
        self.execute("SELECT 1 FROM DUAL;")

    @with_connection
    def close_connection(self):
        self.connection = None

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        command = self.filter_sql_command(args[0])
        i = 1
        while True:
            m = re.match(self.REGEX_CLOB, command)
            if not m:
                break
            command = command[: m.start("clob")] + str(i) + command[m.end("clob") :]
            args = args + (m.group("clob")[6:-2].replace("''", "'"),)
            i += 1
        if command[-1:] == ";":
            command = command[:-1]
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        if len(args) > 1:
            rv = self.cursor.execute(command, args[1:], **kwargs)
        else:
            rv = self.cursor.execute(command, **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def lastrowid(self, table):
        sequence_name = table._sequence_name
        self.execute("SELECT %s.currval FROM dual;" % sequence_name)
        return long(self.cursor.fetchone()[0])

    def sqlsafe_table(self, tablename, original_tablename=None):
        if original_tablename is not None:
            return self.dialect.alias(original_tablename, tablename)
        return self.dialect.quote(tablename)

    def create_sequence_and_triggers(self, query, table, **args):
        tablename = table._rname
        if not "_id" in table:
            return self.execute(query)
        id_name = table._id._rname
        sequence_name = table._sequence_name
        trigger_name = table._trigger_name
        self.execute(query)
        self.execute(
            """
            CREATE SEQUENCE %s START WITH 1 INCREMENT BY 1 NOMAXVALUE 
            MINVALUE -1;"""
            % sequence_name
        )
        self.execute(
            _trigger_sql
            % dict(
                trigger_name=self.dialect.quote(trigger_name),
                tablename=self.dialect.quote(tablename),
                sequence_name=self.dialect.quote(sequence_name),
                id=self.dialect.quote(id_name),
            )
        )

    def _select_aux_execute(self, sql):
        self.execute(sql)
        return self.fetchall()

    def fetchall(self):
        from ..drivers import cx_Oracle

        if any(
            x[1] == cx_Oracle.LOB or x[1] == cx_Oracle.CLOB
            for x in self.cursor.description
        ):
            return [
                tuple([(c.read() if type(c) == cx_Oracle.LOB else c) for c in r])
                for r in self.cursor
            ]
        else:
            return self.cursor.fetchall()

    def sqlsafe_table(self, tablename, original_tablename=None):
        if original_tablename is not None:
            return "%s %s" % (
                self.dialect.quote(original_tablename),
                self.dialect.quote(tablename),
            )
        return self.dialect.quote(tablename)

    def _expand(self, expression, field_type=None, colnames=False, query_env={}):
        # override default expand to ensure quoted fields
        if isinstance(expression, Field):
            if not colnames:
                rv = self.dialect.sqlsafe(expression)
            else:
                rv = self.dialect.longname(expression)
            if field_type == "string" and expression.type not in (
                "string",
                "text",
                "json",
                "password",
            ):
                rv = self.dialect.cast(rv, self.types["text"], query_env)
            return str(rv)
        else:
            return super(Oracle, self)._expand(
                expression, field_type, colnames, query_env
            )

    def expand(self, expression, field_type=None, colnames=False, query_env={}):
        return self._expand(expression, field_type, colnames, query_env)

    def _build_value_for_insert(self, field, value, r_values):
        if field.type == "text":
            _rname = (field._rname[1] == '"') and field._rname[1:-1] or field._rname
            r_values[_rname] = value
            return ":" + _rname
        return self.expand(value, field.type)

    def _update(self, table, query, fields):
        sql_q = ""
        query_env = dict(current_scope=[table._tablename])
        if query:
            if use_common_filters(query):
                query = self.common_filter(query, [table])
            sql_q = self.expand(query, query_env=query_env)
        sql_v = ",".join(
            [
                "%s=%s"
                % (
                    self.dialect.quote(field._rname),
                    self.expand(value, field.type, query_env=query_env),
                )
                for (field, value) in fields
            ]
        )
        return self.dialect.update(table, sql_v, sql_q)

    def _insert(self, table, fields):
        if fields:
            r_values = {}
            return (
                self.dialect.insert(
                    table._rname,
                    ",".join(self.dialect.quote(el[0]._rname) for el in fields),
                    ",".join(
                        self._build_value_for_insert(f, v, r_values) for f, v in fields
                    ),
                ),
                r_values,
            )
        return self.dialect.insert_empty(table._rname), None

    def insert(self, table, fields):
        query, values = self._insert(table, fields)
        try:
            if not values:
                self.execute(query)
            else:
                if type(values) == dict:
                    self.execute(query, **values)
                else:
                    self.execute(query, values)
        except:
            e = sys.exc_info()[1]
            if hasattr(table, "_on_insert_error"):
                return table._on_insert_error(table, fields, e)
            raise e
        if hasattr(table, "_primarykey"):
            pkdict = dict(
                [(k[0].name, k[1]) for k in fields if k[0].name in table._primarykey]
            )
            if pkdict:
                return pkdict
        id = self.lastrowid(table)
        if hasattr(table, "_primarykey") and len(table._primarykey) == 1:
            id = {table._primarykey[0]: id}
        if not isinstance(id, integer_types):
            return id
        rid = Reference(id)
        (rid._table, rid._record) = (table, None)
        return rid

    def _regex_select_as_parser(self, colname):
        return re.compile('\s+"(\S+)"').search(colname)

    def parse(self, rows, fields, colnames, blob_decode=True, cacheable=False):
        if len(rows) and len(rows[0]) == len(fields) + 1 and type(rows[0][-1]) == int:
            # paging has added a trailing rownum column to be discarded
            rows = [row[:-1] for row in rows]
        return super(Oracle, self).parse(rows, fields, colnames, blob_decode, cacheable)


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
    END;
"""
