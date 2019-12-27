from .._compat import basestring
from ..adapters.teradata import Teradata
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(Teradata)
class TeradataDialect(SQLDialect):
    @sqltype_for("integer")
    def type_integer(self):
        return "INT"

    @sqltype_for("text")
    def type_text(self):
        return "VARCHAR(2000)"

    @sqltype_for("json")
    def type_json(self):
        return "VARCHAR(4000)"

    @sqltype_for("float")
    def type_float(self):
        return "REAL"

    @sqltype_for("list:integer")
    def type_list_integer(self):
        return self.types["json"]

    @sqltype_for("list:string")
    def type_list_string(self):
        return self.types["json"]

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return self.types["json"]

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("id")
    def type_id(self):
        return "INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL"

    @sqltype_for("reference")
    def type_reference(self):
        return "INT"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return "BIGINT"

    @sqltype_for("geometry")
    def type_geometry(self):
        return "ST_GEOMETRY"

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return " REFERENCES %(foreign_key)s "

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            " FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_table)s"
            + " (%(foreign_key)s)"
        )

    def left_join(self, val, query_env={}):
        # Left join must always have an ON clause
        if not isinstance(val, basestring):
            val = self.expand(val, query_env=query_env)
        return "LEFT OUTER JOIN %s" % val

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
            limit = " TOP %i" % lmax
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s%s %s FROM %s%s%s%s%s%s;" % (
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

    def truncate(self, table, mode=""):
        return ["DELETE FROM %s ALL;" % table._rname]
