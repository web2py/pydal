from ..adapters.sqlite import SQLite, Spatialite
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(SQLite)
class SQLiteDialect(SQLDialect):
    @sqltype_for('string')
    def type_string(self):
        return 'CHAR(%(length)s)'

    @sqltype_for('float')
    def type_float(self):
        return 'DOUBLE'

    @sqltype_for('double')
    def type_double(self):
        return self.types['float']

    @sqltype_for('decimal')
    def type_decimal(self):
        return self.types['float']

    def extract(self, field, what):
        return "web2py_extract('%s', %s)" % (what, self.expand(field))

    def regexp(self, first, second):
        return '(%s REGEXP %s)' % (
            self.expand(first), self.expand(second, 'string'))

    def select(self, fields, tables, where=None, groupby=None, having=None,
               orderby=None, limitby=None, distinct=False, for_update=False):
        if distinct and distinct is not True:
            raise SyntaxError(
                'DISTINCT ON is not supported by SQLite')
        return super(SQLiteDialect, self).select(
            fields, tables, where, groupby, having, orderby, limitby, distinct,
            for_update)

    def truncate(self, table, mode=''):
        tablename = table._tablename
        return [
            self.delete(tablename),
            self.delete('sqlite_sequence', "name='%s'" % tablename)]


@dialects.register_for(Spatialite)
class SpatialiteDialect(SQLiteDialect):
    @sqltype_for('geometry')
    def type_geometry(self):
        return 'GEOMETRY'

    def st_asgeojson(self, first, second):
        return 'AsGeoJSON(%s,%s,%s)' % (
            self.expand(first), second['precision'], second['options'])

    def st_astext(self, first):
        return 'AsText(%s)' % self.expand(first)

    def st_contains(self, first, second):
        return 'Contains(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_distance(self, first, second):
        return 'Distance(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_equals(self, first, second):
        return 'Equals(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_intersects(self, first, second):
        return 'Intersects(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_overlaps(self, first, second):
        return 'Overlaps(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_simplify(self, first, second):
        return 'Simplify(%s,%s)' % (
            self.expand(first), self.expand(second, 'double'))

    def st_touches(self, first, second):
        return 'Touches(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_within(self, first, second):
        return 'Within(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))
