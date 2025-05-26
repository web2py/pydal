from .._compat import basestring, integer_types
from ..adapters.postgres import Postgre, PostgreBoolean, PostgreNew
from ..helpers.methods import varquote_aux
from ..objects import Expression
from . import dialects, register_expression, sqltype_for
from .base import SQLDialect


@dialects.register_for(Postgre)
class PostgreDialect(SQLDialect):
    true_exp = "TRUE"
    false_exp = "FALSE"

    @sqltype_for("blob")
    def type_blob(self):
        return "BYTEA"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_double(self):
        return "FLOAT8"

    @sqltype_for("id")
    def type_id(self):
        return "SERIAL PRIMARY KEY"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGSERIAL PRIMARY KEY"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s ON UPDATE %(on_update_action)s %(null)s %(unique)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            ' CONSTRAINT "FK_%(constraint_name)s_PK" FOREIGN KEY '
            + "(%(field_name)s) REFERENCES %(foreign_table)s"
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s ON UPDATE %(on_update_action)s"
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
        return self.quote("%s_id_seq" % tablename)

    def insert(self, table, fields, values, returning=None):
        ret = ""
        if returning:
            ret = "RETURNING %s" % returning
        return "INSERT INTO %s(%s) VALUES (%s)%s;" % (table, fields, values, ret)

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

    def regexp(self, first, second, match_parameter=None, query_env={}, **kwargs):
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
                escape = "\\"
                second = second.replace(escape, escape * 2)
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
                escape = "\\"
                second = second.replace(escape, escape * 2)
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
                self.quote(name),
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


class PostgreDialectJSON(PostgreDialect):
    @sqltype_for("json")
    def type_json(self):
        return "JSON"

    @sqltype_for("jsonb")
    def type_jsonb(self):
        return "JSONB"

    def st_astext(self, first, query_env={}):
        return "ST_AsText(%s)" % self.expand(first, query_env=query_env)

    def st_asgeojson(self, first, second, query_env={}):
        return "ST_AsGeoJSON(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            second["precision"],
            second["options"],
        )

    def json_key(self, first, key, query_env=None):
        """Get the json in key which you can use for more queries"""
        if isinstance(key, basestring):
            key = self.expand(key, "string", query_env=query_env)
        elif not isinstance(key, integer_types):
            raise TypeError("Key must be a string or int")
        return "%s->%s" % (self.expand(first, query_env=query_env or {}), key)

    def json_key_value(self, first, key, query_env=None):
        """Get the value int or text in key"""
        if isinstance(key, basestring):
            key = self.expand(key, "string", query_env=query_env)
        elif isinstance(key, integer_types):
            key = self.expand(key, "integer", query_env=query_env)
        else:
            raise TypeError("Key must be a string or int")
        return "%s->>%s" % (self.expand(first, query_env=query_env or {}), key)

    def json_path(self, first, path, query_env=None):
        """Get the json in path which you can use for more queries"""
        return "%s#>'%s'" % (self.expand(first, query_env=query_env or {}), path)

    def json_path_value(self, first, path, query_env=None):
        """Get the json in path which you can use for more queries"""
        return "%s#>>'%s'" % (self.expand(first, query_env=query_env or {}), path)

    # JSON Queries
    def json_contains(self, first, jsonvalue, query_env=None):
        # requires jsonb, value is json e.g. '{"country": "Peru"}'
        return "%s::jsonb@>'%s'::jsonb" % (
            self.expand(first, query_env=query_env or {}),
            jsonvalue,
        )


@dialects.register_for(PostgreNew)
class PostgreDialectArrays(PostgreDialect):
    @sqltype_for("list:integer")
    def type_list_integer(self):
        return "BIGINT[]"

    @sqltype_for("list:string")
    def type_list_string(self):
        return "TEXT[]"

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return "BIGINT[]"

    def any(self, val, query_env={}):
        return "ANY(%s)" % self.expand(val, query_env=query_env)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        if first.type.startswith("list:"):
            f = self.expand(
                second,
                "string" if first.type == "list:string" else "integer",
                query_env=query_env,
            )
            s = self.any(first, query_env)
            if not case_sensitive and first.type == "list:string":
                return self.ilike(f, s, escape="\\", query_env=query_env)
            return self.eq(f, s)
        return super(PostgreDialectArrays, self).contains(
            first, second, case_sensitive=case_sensitive, query_env=query_env
        )

    def ilike(self, first, second, escape=None, query_env={}):
        if first and "type" not in first:
            args = (first, self.expand(second, query_env=query_env))
            return "(%s ILIKE %s)" % args
        return super(PostgreDialectArrays, self).ilike(
            first, second, escape=escape, query_env=query_env
        )

    def eq(self, first, second=None, query_env={}):
        if first and "type" not in first:
            return "(%s = %s)" % (first, self.expand(second, query_env=query_env))
        return super(PostgreDialectArrays, self).eq(first, second, query_env)


class PostgreDialectArraysJSON(PostgreDialectArrays, PostgreDialectJSON):
    pass


@dialects.register_for(PostgreBoolean)
class PostgreDialectBoolean(PostgreDialectArrays):
    @sqltype_for("boolean")
    def type_boolean(self):
        return "BOOLEAN"


class PostgreDialectBooleanJSON(PostgreDialectBoolean, PostgreDialectJSON):
    pass
