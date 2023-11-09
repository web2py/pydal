import re

from .._compat import basestring, integer_types
from ..adapters.oracle import Oracle
from . import dialects, sqltype_for
from .base import SQLDialect


@dialects.register_for(Oracle)
class OracleDialect(SQLDialect):
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
        if not isinstance(val, basestring):
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
