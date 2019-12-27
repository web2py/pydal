from .._compat import basestring
from ..adapters.db2 import DB2
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(DB2)
class DB2Dialect(SQLDialect):
    @sqltype_for("text")
    def type_text(self):
        return "CLOB"

    @sqltype_for("integer")
    def type_integer(self):
        return "INT"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("float")
    def type_float(self):
        return "REAL"

    @sqltype_for("id")
    def type_id(self):
        return "INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL"

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
        return "RAND()"

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
            limit = " FETCH FIRST %i ROWS ONLY" % lmax
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
