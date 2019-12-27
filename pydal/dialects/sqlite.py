from ..adapters.sqlite import SQLite, Spatialite
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(SQLite)
class SQLiteDialect(SQLDialect):
    @sqltype_for("string")
    def type_string(self):
        return "CHAR(%(length)s)"

    @sqltype_for("float")
    def type_float(self):
        return "DOUBLE"

    @sqltype_for("double")
    def type_double(self):
        return self.types["float"]

    @sqltype_for("decimal")
    def type_decimal(self):
        return self.types["float"]

    def extract(self, field, what, query_env={}):
        return "web2py_extract('%s', %s)" % (
            what,
            self.expand(field, query_env=query_env),
        )

    def regexp(self, first, second, query_env={}):
        return "(%s REGEXP %s)" % (
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
        if distinct and distinct is not True:
            raise SyntaxError("DISTINCT ON is not supported by SQLite")
        return super(SQLiteDialect, self).select(
            fields,
            tables,
            where,
            groupby,
            having,
            orderby,
            limitby,
            distinct,
            for_update,
        )

    def truncate(self, table, mode=""):
        tablename = self.adapter.expand(table._raw_rname, "string")
        return [
            self.delete(table),
            "DELETE FROM sqlite_sequence WHERE name=%s" % tablename,
        ]

    def writing_alias(self, table):
        if table._dalname != table._tablename:
            raise SyntaxError("SQLite does not support UPDATE/DELETE on aliased table")
        return table._rname


@dialects.register_for(Spatialite)
class SpatialiteDialect(SQLiteDialect):
    @sqltype_for("geometry")
    def type_geometry(self):
        return "GEOMETRY"

    def st_asgeojson(self, first, second, query_env={}):
        return "AsGeoJSON(%s,%s,%s)" % (
            self.expand(first, query_env=query_env),
            second["precision"],
            second["options"],
        )

    def st_astext(self, first, query_env={}):
        return "AsText(%s)" % self.expand(first, query_env=query_env)

    def st_contains(self, first, second, query_env={}):
        return "Contains(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_distance(self, first, second, query_env={}):
        return "Distance(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_equals(self, first, second, query_env={}):
        return "Equals(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_intersects(self, first, second, query_env={}):
        return "Intersects(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_overlaps(self, first, second, query_env={}):
        return "Overlaps(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_simplify(self, first, second, query_env={}):
        return "Simplify(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "double", query_env=query_env),
        )

    def st_touches(self, first, second, query_env={}):
        return "Touches(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def st_within(self, first, second, query_env={}):
        return "Within(%s,%s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )
