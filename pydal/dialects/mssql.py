from .._compat import basestring
from ..adapters.mssql import (
    MSSQL,
    MSSQLN,
    MSSQL3,
    MSSQL4,
    MSSQL3N,
    MSSQL4N,
    Vertica,
    Sybase,
)
from ..helpers.methods import varquote_aux
from ..objects import Expression
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(MSSQL)
class MSSQLDialect(SQLDialect):
    true = 1
    false = 0
    true_exp = "1=1"
    false_exp = "1=0"
    dt_sep = "T"

    @sqltype_for("boolean")
    def type_boolean(self):
        return "BIT"

    @sqltype_for("blob")
    def type_blob(self):
        return "IMAGE"

    @sqltype_for("integer")
    def type_integer(self):
        return "INT"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_double(self):
        return "FLOAT"

    @sqltype_for("date")
    def type_date(self):
        return "DATE"

    @sqltype_for("time")
    def type_time(self):
        return "CHAR(8)"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "DATETIME"

    @sqltype_for("id")
    def type_id(self):
        return "INT IDENTITY PRIMARY KEY"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INT%(null)s%(unique)s, CONSTRAINT %(constraint_name)s "
            + "FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_key)s ON "
            + "DELETE %(on_delete_action)s"
        )

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT IDENTITY PRIMARY KEY"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT%(null)s%(unique)s, CONSTRAINT %(constraint_name)s"
            + " FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ", CONSTRAINT FK_%(constraint_name)s FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_key)s ON DELETE "
            + "%(on_delete_action)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            " CONSTRAINT FK_%(constraint_name)s_PK FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_table)s "
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("geometry")
    def type_geometry(self):
        return "geometry"

    @sqltype_for("geography")
    def type_geography(self):
        return "geography"

    def varquote(self, val):
        return varquote_aux(val, "[%s]")

    def update(self, table, values, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "UPDATE %s SET %s FROM %s%s;" % (
            table.sql_shortref,
            values,
            tablename,
            whr,
        )

    def delete(self, table, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "DELETE %s FROM %s%s;" % (table.sql_shortref, tablename, whr)

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
        with_cte=None  # ['recursive' | '', sql]
    ):
        dst, whr, grp, order, limit, upd = "", "", "", "", "", ""
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

        if with_cte:
            recursive, cte = with_cte
            recursive = ' RECURSIVE' if recursive else ''
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

        return "%sSELECT%s%s %s FROM %s%s%s%s%s;" % (
            with_cte,
            dst,
            limit,
            fields,
            tables,
            whr,
            grp,
            order,
            upd,
        )

    def left_join(self, val, query_env={}):
        # Left join must always have an ON clause
        if not isinstance(val, basestring):
            val = self.expand(val, query_env=query_env)
        return "LEFT OUTER JOIN %s" % val

    def random(self):
        return "NEWID()"

    def cast(self, first, second, query_env={}):
        # apparently no cast necessary in MSSQL
        return first

    def _mssql_like_normalizer(self, term):
        term = term.replace("[", "[[]")
        return term

    def _like_escaper_default(self, term):
        if isinstance(term, Expression):
            return term
        return self._mssql_like_normalizer(
            super(MSSQLDialect, self)._like_escaper_default(term)
        )

    def concat(self, *items, **kwargs):
        query_env = kwargs.get("query_env", {})
        tmp = (self.expand(x, "string", query_env=query_env) for x in items)
        return "(%s)" % " + ".join(tmp)

    def regexp(self, first, second, query_env={}):
        second = self.expand(second, "string", query_env=query_env)
        second = second.replace("\\", "\\\\")
        second = second.replace(r"%", r"\%").replace("*", "%").replace(".", "_")
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first, query_env=query_env),
            second,
        )

    def extract(self, first, what, query_env={}):
        return "DATEPART(%s,%s)" % (what, self.expand(first, query_env=query_env))

    def epoch(self, val, query_env={}):
        return "DATEDIFF(second, '1970-01-01 00:00:00', %s)" % self.expand(
            val, query_env=query_env
        )

    def length(self, val, query_env={}):
        return "LEN(%s)" % self.expand(val, query_env=query_env)

    def aggregate(self, first, what, query_env={}):
        if what == "LENGTH":
            what = "LEN"
        return super(MSSQLDialect, self).aggregate(first, what, query_env)

    @property
    def allow_null(self):
        return " NULL"

    def substring(self, field, parameters, query_env={}):
        return "SUBSTRING(%s,%s,%s)" % (
            self.expand(field, query_env=query_env),
            parameters[0],
            parameters[1],
        )

    def primary_key(self, key):
        return "PRIMARY KEY CLUSTERED (%s)" % key

    def concat_add(self, tablename):
        return "; ALTER TABLE %s ADD " % tablename

    def drop_index(self, name, table, if_exists = False):
        return "DROP INDEX %s ON %s;" % (self.quote(name), table._rname)

    def st_astext(self, first, query_env={}):
        return "%s.STAsText()" % self.expand(first, query_env=query_env)

    def st_contains(self, first, second, query_env={}):
        return "%s.STContains(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_distance(self, first, second, query_env={}):
        return "%s.STDistance(%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_equals(self, first, second, query_env={}):
        return "%s.STEquals(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_intersects(self, first, second, query_env={}):
        return "%s.STIntersects(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_overlaps(self, first, second, query_env={}):
        return "%s.STOverlaps(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_touches(self, first, second, query_env={}):
        return "%s.STTouches(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_within(self, first, second, query_env={}):
        return "%s.STWithin(%s)=1" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )


@dialects.register_for(MSSQLN)
class MSSQLNDialect(MSSQLDialect):
    @sqltype_for("string")
    def type_string(self):
        return "NVARCHAR(%(length)s)"

    @sqltype_for("text")
    def type_text(self):
        return "NTEXT"

    def ilike(self, first, second, escape=None, query_env={}):
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env).lower()
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        if second.startswith("n'"):
            second = "N'" + second[2:]
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.lower(first, query_env),
            second,
            escape,
        )


@dialects.register_for(MSSQL3)
class MSSQL3Dialect(MSSQLDialect):
    @sqltype_for("text")
    def type_text(self):
        return "VARCHAR(MAX)"

    @sqltype_for("time")
    def type_time(self):
        return "TIME(7)"

    def _rebuild_select_for_limit(
        self, fields, tables, dst, whr, grp, order, lmin, lmax
    ):
        f_outer = ["f_%s" % i for i in range(len(fields.split(",")))]
        f_inner = [field for field in fields.split(", ")]
        f_iproxy = ", ".join([self._as(o, n) for (o, n) in zip(f_inner, f_outer)])
        f_oproxy = ", ".join(f_outer)
        interp = (
            "SELECT%s %s FROM ("
            + "SELECT%s ROW_NUMBER() OVER (%s) AS w_row, %s FROM %s%s%s)"
            + " TMP WHERE w_row BETWEEN %i and %i;"
        )
        return interp % (
            dst,
            f_oproxy,
            dst,
            order,
            f_iproxy,
            tables,
            whr,
            grp,
            lmin,
            lmax,
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
        with_cte=None  # ['recursive' | '', sql]
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
            if lmin == 0:
                dst += " TOP %i" % lmax
            else:
                return self._rebuild_select_for_limit(
                    fields, tables, dst, whr, grp, order, lmin, lmax
                )
        if for_update:
            upd = " FOR UPDATE"

        if with_cte:
            recursive, cte = with_cte
            recursive = ' RECURSIVE' if recursive else ''
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

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


@dialects.register_for(MSSQL4)
class MSSQL4Dialect(MSSQL3Dialect):
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
        with_cte=None  # ['recursive' | '', sql]
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
            if lmin == 0:
                dst += " TOP %i" % lmax
            else:
                if not order:
                    order = " ORDER BY %s" % self.random
                offset = " OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" % (
                    lmin,
                    (lmax - lmin),
                )
        if for_update:
            upd = " FOR UPDATE"

        if with_cte:
            recursive, cte = with_cte
            recursive = ' RECURSIVE' if recursive else ''
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

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


@dialects.register_for(MSSQL3N)
class MSSQL3NDialect(MSSQLNDialect, MSSQL3Dialect):
    @sqltype_for("text")
    def type_text(self):
        return "NVARCHAR(MAX)"


@dialects.register_for(MSSQL4N)
class MSSQL4NDialect(MSSQLNDialect, MSSQL4Dialect):
    @sqltype_for("text")
    def type_text(self):
        return "NVARCHAR(MAX)"


@dialects.register_for(Vertica)
class VerticaDialect(MSSQLDialect):
    dt_sep = " "

    @sqltype_for("boolean")
    def type_boolean(self):
        return "BOOLEAN"

    @sqltype_for("text")
    def type_text(self):
        return "BYTEA"

    @sqltype_for("json")
    def type_json(self):
        return self.types["string"]

    @sqltype_for("blob")
    def type_blob(self):
        return "BYTEA"

    @sqltype_for("double")
    def type_double(self):
        return "DOUBLE PRECISION"

    @sqltype_for("time")
    def type_time(self):
        return "TIME"

    @sqltype_for("id")
    def type_id(self):
        return "IDENTITY"

    @sqltype_for("reference")
    def type_reference(self):
        return "INT REFERENCES %(foreign_key)s ON DELETE %(on_delete_action)s"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return "BIGINT REFERENCES %(foreign_key)s ON DELETE" + " %(on_delete_action)s"

    def extract(self, first, what, query_env={}):
        return "DATE_PART('%s', TIMESTAMP %s)" % (
            what,
            self.expand(first, query_env=query_env),
        )

    def truncate(self, table, mode=""):
        if mode:
            mode = " %s" % mode
        return ["TRUNCATE %s%s;" % (table._rname, mode)]

    def select(self, *args, **kwargs):
        return SQLDialect.select(self, *args, **kwargs)


@dialects.register_for(Sybase)
class SybaseDialect(MSSQLDialect):
    @sqltype_for("string")
    def type_string(self):
        return "CHAR VARYING(%(length)s)"

    @sqltype_for("date")
    def type_date(self):
        return "DATETIME"
