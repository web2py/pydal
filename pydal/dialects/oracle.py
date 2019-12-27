from ..adapters.oracle import Oracle
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(Oracle)
class OracleDialect(SQLDialect):
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
    def type_float(self):
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

    def left_join(self, val):
        return "LEFT OUTER JOIN %s" % val

    @property
    def random(self):
        return "dbms_random.value"

    def trigger_name(self, tablename):
        return "%s_trigger" % tablename

    def constraint_name(self, table, fieldname):
        constraint_name = super(OracleDialect, self).constraint_name(table, fieldname)
        if len(constraint_name) > 30:
            constraint_name = "%s_%s__constraint" % (table[:10], fieldname[:7])
        return constraint_name

    def not_null(self, default, field_type):
        return "DEFAULT %s NOT NULL" % self.adapter.represent(default, field_type)

    def regexp(self, first, second, query_env={}):
        return "REGEXP_LIKE(%s, %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
        )

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
        if limitby:
            (lmin, lmax) = limitby
            if whr:
                whr2 = whr + " AND w_row > %i" % lmin
            else:
                whr2 = self.where("w_row > %i" % lmin)

            return (
                "SELECT%s %s FROM (SELECT w_tmp.*, ROWNUM w_row FROM "
                "(SELECT %s FROM %s%s%s%s) w_tmp WHERE ROWNUM<=%i) %s%s%s%s;"
            ) % (
                dst,
                fields,
                fields,
                tables,
                whr,
                grp,
                order,
                lmax,
                tables,
                whr2,
                grp,
                order,
            )
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s %s FROM %s%s%s%s%s%s%s;" % (
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
        return [
            "DROP TABLE %s %s;" % (table._rname, mode),
            "DROP SEQUENCE %s;" % sequence_name,
        ]
