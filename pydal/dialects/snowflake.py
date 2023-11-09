from .._compat import basestring, integer_types
from ..adapters.snowflake import Snowflake
from ..helpers.methods import varquote_aux
from ..objects import Expression
from . import dialects, register_expression, sqltype_for
from .base import SQLDialect


@dialects.register_for(Snowflake)
class SnowflakeDialect(SQLDialect):
    true_exp = "TRUE"
    false_exp = "FALSE"
    quote_template = " %s "

    @sqltype_for("blob")
    def type_blob(self):
        return "BINARY"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "NUMBER"

    @sqltype_for("double")
    def type_double(self):
        return "FLOAT8"

    @sqltype_for("id")
    def type_id(self):
        return "NUMBER PRIMARY KEY AUTOINCREMENT"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "NUMBER PRIMARY KEY AUTOINCREMENT"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "NUMBER REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s %(null)s %(unique)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            ' CONSTRAINT "FK_%(constraint_name)s_PK" FOREIGN KEY '
            + "(%(field_name)s) REFERENCES %(foreign_table)s"
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("geometry")
    def type_geometry(self):
        return "GEOMETRY"

    @sqltype_for("geography")
    def type_geography(self):
        return "GEOGRAPHY"

    def varquote(self, val):
        return varquote_aux(val, '"%s"')

    def sequence_name(self, tablename):
        return "%s_id_seq" % tablename

    def insert(self, table, fields, values):
        return "INSERT INTO %s(%s) VALUES (%s);" % (table, fields, values)

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
        with_cte=False,
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

        return "SELECT%s%s%s %s FROM %s%s%s%s;" % (
            dst,
            limit,
            offset,
            fields,
            tables,
            whr,
            grp,
            order,
        )

    def delete(self, table, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "DELETE FROM %s %s;" % (tablename, whr)

    def update(self, table, values, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "UPDATE %s SET %s%s;" % (
            tablename,
            values,
            whr,
        )

    @property
    def random(self):
        return "RANDOM()"

    def add(self, first, second, query_env={}):
        t = first.type
        if t in ("text", "string", "password", "json", "jsonb", "upload", "blob"):
            return "(%s || %s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, first.type, query_env=query_env),
            )
        else:
            return "(%s + %s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, first.type, query_env=query_env),
            )

    def regexp(self, first, second, match_parameter=None, query_env={}):
        return "(%s ~ %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
        )

    def like(self, first, second, escape=None, query_env={}):
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env)
            if escape is None:
                escape = r"\\ "
                # second = second.replace(escape, escape * 2)
            check = r"\ "
            check = check.strip()
            if escape == check:
                escape = r"\\ "
                escape = escape.strip()
        if first.type not in ("string", "text", "json", "jsonb"):
            return "(%s LIKE %s ESCAPE '%s')" % (
                self.cast(
                    self.expand(first, query_env=query_env), "CHAR(%s)" % first.length
                ),
                second,
                escape,
            )
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.expand(first, query_env=query_env),
            second,
            escape,
        )

    def ilike(self, first, second, escape=None, query_env={}):
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env)
            if escape is None:
                escape = r"\\ "
                escape = escape.strip()
                # second = second.replace(escape, escape * 2)
            check = r"\ "
            check = check.strip()
            if escape == check:
                escape = r"\\ "
                escape = escape.strip()
        if first.type not in ("string", "text", "json", "jsonb", "list:string"):
            return "(%s ILIKE %s ESCAPE '%s')" % (
                self.cast(
                    self.expand(first, query_env=query_env), "CHAR(%s)" % first.length
                ),
                second,
                escape,
            )
        return "(%s ILIKE %s ESCAPE '%s')" % (
            self.expand(first, query_env=query_env),
            second,
            escape,
        )

    def drop_table(self, table, mode):
        if mode not in ["restrict", "cascade", ""]:
            raise ValueError("Invalid mode: %s" % mode)
        return ["DROP TABLE " + table._rname + " " + mode + ";"]

    def create_index(self, name, table, expressions, unique=False, where=None):
        uniq = " UNIQUE" if unique else ""
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        with self.adapter.index_expander():
            rv = "CREATE%s INDEX %s ON %s (%s)%s;" % (
                uniq,
                name,
                table._rname,
                ",".join(self.expand(field) for field in expressions),
                whr,
            )
        return rv

    def st_asgeojson(self, first, second, query_env={}):
        return "ST_AsGeoJSON(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            second["precision"],
            second["options"],
        )

    def unquote(self, val):
        if val[0] == '"' and val[-1] == '"':
            val = val.replace('"', "")
        return val

    def st_astext(self, first, query_env={}):
        return "ST_AsText(%s)" % self.expand(first, query_env=query_env)

    def st_aswkb(self, first, query_env={}):
        return "%s" % self.expand(first, query_env=query_env)

    def st_x(self, first, query_env={}):
        return "ST_X(%s)" % (self.expand(first, query_env=query_env))

    def st_y(self, first, query_env={}):
        return "ST_Y(%s)" % (self.expand(first, query_env=query_env))

    def st_contains(self, first, second, query_env={}):
        return "ST_Contains(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_distance(self, first, second, query_env={}):
        return "ST_Distance(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_equals(self, first, second, query_env={}):
        return "ST_Equals(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_intersects(self, first, second, query_env={}):
        return "ST_Intersects(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_overlaps(self, first, second, query_env={}):
        return "ST_Overlaps(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_simplify(self, first, second, query_env={}):
        return "ST_Simplify(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "double", query_env=query_env),
        )

    def st_simplifypreservetopology(self, first, second, query_env={}):
        return "ST_SimplifyPreserveTopology(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "double", query_env=query_env),
        )

    def st_touches(self, first, second, query_env={}):
        return "ST_Touches(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_within(self, first, second, query_env={}):
        return "ST_Within(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_dwithin(self, first, tup, query_env={}):
        return "ST_DWithin(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(tup[0], first.type, query_env=query_env),
            self.expand(tup[1], "double", query_env=query_env),
        )

    def st_transform(self, first, second, query_env={}):
        # The SRID argument can be provided as an integer SRID or a Proj4 string
        if isinstance(second, int):
            return "ST_Transform(%s,%s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, "integer", query_env=query_env),
            )
        else:
            return "ST_Transform(%s,%s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, "string", query_env=query_env),
            )

    @register_expression("doy")
    def extract_doy(self, expr):
        return Expression(expr.db, self.extract, expr, "doy", "integer")

    @register_expression("dow")
    def extract_dow(self, expr):
        return Expression(expr.db, self.extract, expr, "dow", "integer")

    @register_expression("isodow")
    def extract_isodow(self, expr):
        return Expression(expr.db, self.extract, expr, "isodow", "integer")

    @register_expression("isoyear")
    def extract_isoyear(self, expr):
        return Expression(expr.db, self.extract, expr, "isoyear", "integer")

    @register_expression("quarter")
    def extract_quarter(self, expr):
        return Expression(expr.db, self.extract, expr, "quarter", "integer")

    @register_expression("week")
    def extract_week(self, expr):
        return Expression(expr.db, self.extract, expr, "week", "integer")

    @register_expression("decade")
    def extract_decade(self, expr):
        return Expression(expr.db, self.extract, expr, "decade", "integer")

    @register_expression("century")
    def extract_century(self, expr):
        return Expression(expr.db, self.extract, expr, "century", "integer")

    @register_expression("millenium")
    def extract_millenium(self, expr):
        return Expression(expr.db, self.extract, expr, "millenium", "integer")
