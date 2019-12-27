from ..adapters.informix import Informix, InformixSE
from .firebird import FireBirdDialect
from . import dialects, sqltype_for


@dialects.register_for(Informix)
class InformixDialect(FireBirdDialect):
    @sqltype_for("id")
    def type_id(self):
        return "SERIAL"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGSERIAL"

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            "REFERENCES %(foreign_key)s ON DELETE %(on_delete_action)s "
            + "CONSTRAINT FK_%(table_name)s_%(field_name)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            "FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_table)s"
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s "
            + "CONSTRAINT TFK_%(table_name)s_%(field_name)s"
        )

    @property
    def random(self):
        return "Random()"

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
            if lmin and self.adapter.dbms_version >= 10:
                offset = " SKIP %i" % lmin
            if fetch_amt and self.adapter.dbms_version >= 9:
                limit = " FIRST %i" % fetch_amt
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s%s%s %s FROM %s%s%s%s%s;" % (
            dst,
            offset,
            limit,
            fields,
            tables,
            whr,
            grp,
            order,
            upd,
        )


@dialects.register_for(InformixSE)
class InformixSEDialect(InformixDialect):
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
