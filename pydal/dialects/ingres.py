from .._compat import basestring
from ..adapters.ingres import Ingres, IngresUnicode
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(Ingres)
class IngresDialect(SQLDialect):
    SEQNAME = "ii***lineitemsequence"

    @sqltype_for("text")
    def type_text(self):
        return "CLOB"

    @sqltype_for("integer")
    def type_integer(self):
        return "INTEGER4"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_float(self):
        return "FLOAT8"

    @sqltype_for("date")
    def type_date(self):
        return "ANSIDATE"

    @sqltype_for("time")
    def type_time(self):
        return "TIME WITHOUT TIME ZONE"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "TIMESTAMP WITHOUT TIME ZONE"

    @sqltype_for("id")
    def type_id(self):
        return (
            "int not null unique with default next value for %s" % self.INGRES_SEQNAME
        )

    @sqltype_for("big-id")
    def type_big_id(self):
        return (
            "bigint not null unique with default next value for %s"
            % self.INGRES_SEQNAME
        )

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INT, FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT, FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
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
        # Left join must always have an ON clause
        if not isinstance(val, basestring):
            val = self.expand(val, query_env=query_env)
        return "LEFT OUTER JOIN %s" % val

    @property
    def random(self):
        return "RANDOM()"

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
            fetch_amt = lmax - lmin
            if fetch_amt:
                limit = " FIRST %i" % fetch_amt
            if lmin:
                offset = " OFFSET %i" % lmin
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s%S %s FROM %s%s%s%s%s%s;" % (
            dst,
            limit,
            fields,
            tables,
            whr,
            grp,
            order,
            offset,
            upd,
        )


@dialects.register_for(IngresUnicode)
class IngresUnicodeDialect(IngresDialect):
    @sqltype_for("string")
    def type_string(self):
        return "NVARCHAR(%(length)s)"

    @sqltype_for("text")
    def type_text(self):
        return "NCLOB"
