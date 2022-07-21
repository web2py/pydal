from ..adapters.sap import SAPDB
from . import dialects, sqltype_for
from .base import SQLDialect


@dialects.register_for(SAPDB)
class SAPDBDialect(SQLDialect):
    @sqltype_for("integer")
    def type_integer(self):
        return "INT"

    @sqltype_for("text")
    def type_text(self):
        return "LONG"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_double(self):
        return "DOUBLE PRECISION"

    @sqltype_for("decimal")
    def type_decimal(self):
        return "FIXED(%(precision)s,%(scale)s)"

    @sqltype_for("id")
    def type_id(self):
        return "INT PRIMARY KEY"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT PRIMARY KEY"

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

    def sequence_name(self, tablename):
        return self.quote("%s_id_Seq" % tablename)

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
                "SELECT%s %s FROM (SELECT w_tmp.*, ROWNO w_row FROM "
                + "(SELECT %s FROM %s%s%s%s) w_tmp WHERE ROWNO=%i) %s%s%s%s;"
                % (
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
            )
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s%s%s %s FROM %s%s%s%s%s;" % (
            dst,
            limit,
            offset,
            fields,
            tables,
            whr,
            grp,
            order,
            upd,
        )
