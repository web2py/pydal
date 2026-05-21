"""Backend module for oracle."""

# ============================================================
# Adapter
# ============================================================

import os
import re
import sys

from ..helpers.classes import Reference
from ..helpers.methods import use_common_filters
from ..objects import Expression, Field, Query, Table
from ..backend_base import adapters, with_connection, with_connection_or_raise
from ..backend_base import SQLAdapter


@adapters.register_for("oracle")
class Oracle(SQLAdapter):
    """
    Oracle adapter via ``cx_Oracle``.

    Distinctive features:

    * Forces ``threaded=True`` and UTF-8 encodings on the connection.
    * Sets ANSI-friendly date/timestamp formats via ``ALTER SESSION``
      so values round-trip cleanly through the dialect.
    * Pulls CLOB/BLOB literals out of the statement and binds them
      separately (cx_Oracle won't accept them inline beyond a small
      size).
    * ``execute`` parses inline CLOBs out of the query and rebinds
      them as parameters.
    """

    dbengine = "oracle"
    drivers = ("cx_Oracle",)

    def _initialize_(self):
        super(Oracle, self)._initialize_()
        self.ruri = self.uri.split("://", 1)[1]
        self.REGEX_CLOB = re.compile(
            r"[^']*('[^']*'[^']*)*\:(?P<clob>(C|B)LOB\('([^']|'')*'\))"
        )
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
        return int(self.cursor.fetchone()[0])

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
                if isinstance(values, dict):
                    self.execute(query, **values)
                else:
                    self.execute(query, values)
        except Exception:
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
        if not isinstance(id, int):
            return id
        rid = Reference(id)
        (rid._table, rid._record) = (table, None)
        return rid

    def _regex_select_as_parser(self, colname):
        return re.compile(r'\s+"(\S+)"').search(colname)

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

# ============================================================
# Dialect
# ============================================================

import re

from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(Oracle)
class OracleDialect(SQLDialect):
    """
    Oracle dialect.

    Distinctive features:

    * No native ``BOOLEAN`` — booleans render as ``1=1`` / ``1=0``.
    * Strings use ``VARCHAR2``.
    * ``regexp`` maps to ``REGEXP_LIKE``.
    * Synthetic IDs come from per-table sequences plus BEFORE-INSERT
      triggers; ``lastrowid`` reads ``seq.CURRVAL``.
    * Pagination uses a ``ROWNUM`` nested-select wrapper (works on
      Oracle 8–11; newer releases also accept ``OFFSET ... ROWS``).
    """

    false_exp = "1=0"
    true_exp = "1=1"

    @sqltype_for("string")
    def type_string(self):
        return "VARCHAR2(%(length)s)"

    @sqltype_for("text")
    def type_text(self):
        return "CLOB"

    @sqltype_for("integer")
    def type_integer(self):
        return "INT"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "NUMBER"

    @sqltype_for("double")
    def type_double(self):
        return "BINARY_DOUBLE"

    @sqltype_for("time")
    def type_time(self):
        return "TIME(8)"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "DATE"

    @sqltype_for("id")
    def type_id(self):
        return "NUMBER PRIMARY KEY"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "NUMBER, CONSTRAINT %(constraint_name)s FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_key)s ON DELETE "
            + "%(on_delete_action)s"
        )

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ", CONSTRAINT FK_%(constraint_name)s FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            " CONSTRAINT FK_%(constraint_name)s_PK FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_table)s"
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s"
        )

    def left_join(self, val, query_env={}):
        if not isinstance(val, str):
            val = self.expand(val, query_env=query_env)
        return "LEFT OUTER JOIN %s" % val

    @property
    def random(self):
        return "dbms_random.value"

    def cast(self, first, second, query_env={}):
        if second == "CLOB":
            return "TO_CHAR(%s)" % self.expand(first, query_env=query_env)
        return "CAST(%s)" % self._as(first, second, query_env)

    def mod(self, first, second, query_env={}):
        return "MOD(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def extract(self, first, what, query_env={}):
        if what == "hour":
            return "TO_CHAR(%s, 'HH24')" % self.expand(first, query_env=query_env)
        if what == "minute":
            return "TO_CHAR(%s, 'MI')" % self.expand(first, query_env=query_env)
        if what == "second":
            return "TO_CHAR(%s, 'SS')" % self.expand(first, query_env=query_env)
        return "EXTRACT(%s FROM %s)" % (what, self.expand(first, query_env=query_env))

    def epoch(self, val, query_env={}):
        return "(%s - DATE '1970-01-01')*24*60*60" % self.expand(
            val, query_env=query_env
        )

    def quote(self, val):
        if not (val[0] == '"' and val[-1] == '"'):
            return self.quote_template % val
        return val

    def _as(self, first, second, query_env={}):
        return "%s %s" % (self.expand(first, query_env), self.quote(second))

    def alias(self, original, new):
        return "%s %s" % (original, self.quote(new))

    def writing_alias(self, table):
        return self.sql_fullref(table)

    def sqlsafe(self, field):
        if field._table is None:
            raise SyntaxError("Field %s is not bound to any table" % field.name)
        return self.quote(field._table.sql_shortref) + "." + self.quote(field._rname)

    def longname(self, field):
        if field._table is None:
            raise SyntaxError("Field %s is not bound to any table" % field.name)
        return self.quote(field._table._tablename) + "." + self.quote(field.name)

    def sql_fullref(self, table):
        if table._tablename == table._dalname:
            return self.quote(table._rname)
        return self.adapter.sqlsafe_table(table._tablename, table._rname)

    def trigger_name(self, tablename):
        return "%s_trigger" % tablename

    def sequence_name(self, tablename):
        if tablename[0] == '"':
            # manually written quotes, typically in case-sensitive rname
            tablename = tablename[1:-1]
        # truncate to max length
        return self.quote(("%s_sequence" % tablename)[0:29])

    def constraint_name(self, table, fieldname):
        if table[0] == '"':
            # manually written quotes, typically in case-sensitive rname
            table = table[1:-1]
        constraint_name = super(OracleDialect, self).constraint_name(table, fieldname)
        if len(constraint_name) > 30:
            constraint_name = "%s_%s__constraint" % (table[:10], fieldname[:7])
        return constraint_name

    def primary_key(self, key):
        if len(re.split(r",\s*", key)) > 1:
            return "PRIMARY KEY(%s)" % ", ".join(
                [self.quote(k) for k in re.split(r",\s*", key)]
            )
        return "PRIMARY KEY(%s)" % key

    def not_null(self, default, field_type):
        return "DEFAULT %s NOT NULL" % self.adapter.represent(default, field_type)

    def not_null(self, default, field_type):
        return "NOT NULL DEFAULT %s" % self.adapter.represent(default, field_type)

    def eq(self, first, second=None, query_env={}):
        if (first.type == "text" or first.type[:4] == "list") and second:
            return "(TO_CHAR(%s) = %s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, first.type, query_env=query_env),
            )
        return super(OracleDialect, self).eq(first, second, query_env)

    def regexp(self, first, second, match_parameter, query_env={}):
        if match_parameter:
            _match_parameter = "," + self.expand(
                match_parameter, "string", query_env=query_env
            )
        else:
            _match_parameter = ""

        return "REGEXP_LIKE(%s, %s %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
            _match_parameter,
        )

    def insert(self, table, fields, values):
        return "INSERT INTO %s(%s) VALUES (%s);" % (self.quote(table), fields, values)

    def insert_empty(self, table):
        return "INSERT INTO %s VALUES (DEFAULT);" % table

    def _select_aux(self, sql, fields, attributes, colnames):
        return super._select_aux(sql, fields, attributes, colnames)

    def select(
        self,
        fields,
        tables,
        where=None,
        groupby=None,
        having=None,
        orderby=None,
        limitby=None,
        distinct=False,
        for_update=False,
        with_cte=None,
    ):
        dst, whr, grp, order, limit, offset, upd = "", "", "", "", "", "", ""
        if distinct is True:
            dst = " DISTINCT"
        elif distinct:
            dst = " DISTINCT ON (%s)" % distinct
        if where:
            whr = " %s" % self.where(where)
        if groupby:
            grp = " GROUP BY %s" % groupby
            if having:
                grp += " HAVING %s" % having
        if orderby:
            order = " ORDER BY %s" % orderby

        if with_cte:
            recursive, cte = with_cte
            recursive = " RECURSIVE" if recursive else ""
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

        if limitby:
            (lmin, lmax) = limitby
            if whr:
                whr2 = whr + " AND w_row > %i" % lmin
            else:
                whr2 = self.where("w_row > %i" % lmin)
            return """
                %sSELECT%s * FROM (
                    SELECT w_tmp.*, ROWNUM w_row FROM (
                        SELECT %s FROM %s%s%s%s
                    ) w_tmp
                ) WHERE w_row<=%i and w_row>%i
            """ % (
                with_cte,
                dst,
                fields,
                tables,
                whr,
                grp,
                order,
                lmax,
                lmin,
            )
        if for_update:
            upd = " FOR UPDATE"
        return "%sSELECT%s %s FROM %s%s%s%s%s%s%s;" % (
            with_cte,
            dst,
            fields,
            tables,
            whr,
            grp,
            order,
            limit,
            offset,
            upd,
        )

    def drop_table(self, table, mode):
        sequence_name = table._sequence_name
        if mode and mode.upper() == "CASCADE":
            mode = "CASCADE CONSTRAINTS"
        drops = [
            "DROP TABLE %s %s;" % (self.quote(table._rname), mode),
        ]
        if "_id" in table:
            drops.append("DROP SEQUENCE %s;" % sequence_name)
        return drops

# ============================================================
# Parser
# ============================================================

import json
from datetime import date, datetime

from ..backend_base import for_type, parsers
from ..backend_base import BasicParser, ListsParser  # noqa: F401  imports below stay near consumers


class OracleParser(BasicParser):
    """
    Oracle parser handling integer, text, CLOB, JSON, date, and
    list-of-reference columns. cx_Oracle returns these in shapes that
    need extra coercion compared to other drivers.
    """

    @for_type("integer")
    def _integer(self, value):
        """cx_Oracle returns Decimal for INTEGER; coerce to int."""
        return int(value)

    @for_type("text")
    def _text(self, value):
        """Pass text through unchanged (CLOB is handled separately)."""
        return value

    @for_type("clob")
    def _clob(self, value):
        """Pass CLOB content through unchanged."""
        return value

    @for_type("json")
    def _json(self, value):
        """Parse Oracle JSON columns (stored as CLOB) via ``json.loads``."""
        return json.loads(value)

    @for_type("date")
    def _date(self, value):
        """Convert Oracle's datetime-as-date back to a real ``date``."""
        if isinstance(value, datetime):
            return value.date()
        (y, m, d) = map(int, str(value)[:10].strip().split("-"))
        return date(y, m, d)

    @for_type("list:reference")
    def _list_references(self, value, field_type):
        """Delegate to the inherited list:reference handler."""
        return super(OracleParser, self)._list_references.f(self, value, field_type)


class OracleListsParser(ListsParser):
    """Oracle-flavoured list-of-integer and list-of-string parsers."""

    @for_type("list:integer")
    def _list_integers(self, value):
        """Delegate to the inherited list:integer handler."""
        return super(OracleListsParser, self)._list_integers.f(self, value)

    @for_type("list:string")
    def _list_strings(self, value):
        """Delegate to the inherited list:string handler."""
        return super(OracleListsParser, self)._list_strings.f(self, value)


@parsers.register_for(Oracle)
class OracleCommonparser(OracleParser, OracleListsParser):
    """Composite Oracle parser combining basic and list-type handlers."""

# ============================================================
# Representer
# ============================================================

from base64 import b64encode

from ..utils import to_bytes, to_native
from ..backend_base import representers
from ..backend_base import JSONRepresenter, SQLRepresenter


@representers.register_for(Oracle)
class OracleRepresenter(SQLRepresenter, JSONRepresenter):
    """Oracle-specific value rendering for blob, date, datetime."""

    def exceptions(self, obj, field_type):
        """Render BLOB via ``:CLOB('base64...')`` and dates via ``to_date(...)``."""
        if field_type == "blob":
            if not isinstance(obj, bytes):
                obj = to_bytes(obj)
            obj = to_native(b64encode(obj))
            return ":CLOB('%s')" % obj
        if field_type == "date":
            if isinstance(obj, (date, datetime)):
                obj = obj.isoformat()[:10]
            else:
                obj = str(obj)
            return "to_date('%s','yyyy-mm-dd')" % obj
        if field_type == "datetime":
            if isinstance(obj, datetime):
                obj = obj.isoformat()[:19].replace("T", " ")
            elif isinstance(obj, date):
                obj = obj.isoformat()[:10] + " 00:00:00"
            else:
                obj = str(obj)
            return "to_date('%s','yyyy-mm-dd hh24:mi:ss')" % obj
        return None

