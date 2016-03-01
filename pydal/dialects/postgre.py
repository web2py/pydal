from ..adapters.postgres import Postgre
from ..helpers.methods import varquote_aux
from ..objects import Expression
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(Postgre)
class PostgreDialect(SQLDialect):
    true_exp = "TRUE"
    false_exp = "FALSE"

    @sqltype_for('string')
    def type_string(self):
        return 'VARCHAR(%(length)s)'

    @sqltype_for('boolean')
    def type_boolean(self):
        return 'CHAR(1)'

    @sqltype_for('blob')
    def type_blob(self):
        return 'BYTEA'

    @sqltype_for('bigint')
    def type_bigint(self):
        return 'BIGINT'

    @sqltype_for('float')
    def type_float(self):
        return 'FLOAT'

    @sqltype_for('double')
    def type_double(self):
        return 'FLOAT8'

    @sqltype_for('decimal')
    def type_decimal(self):
        return 'NUMERIC(%(precision)s,%(scale)s)'

    @sqltype_for('id')
    def type_id(self):
        return 'SERIAL PRIMARY KEY'

    @sqltype_for('big-id')
    def type_big_id(self):
        return 'BIGSERIAL PRIMARY KEY'

    @sqltype_for('big-reference')
    def type_big_reference(self):
        return 'BIGINT REFERENCES %(foreign_key)s ' + \
            'ON DELETE %(on_delete_action)s %(null)s %(unique)s'

    @sqltype_for('reference TFK')
    def type_reference_tfk(self):
        return ' CONSTRAINT "FK_%(constraint_name)s_PK" FOREIGN KEY ' + \
            '(%(field_name)s) REFERENCES %(foreign_table)s' + \
            '(%(foreign_key)s) ON DELETE %(on_delete_action)s'

    @sqltype_for('geometry')
    def type_geometry(self):
        return 'GEOMETRY'

    @sqltype_for('geography')
    def type_geography(self):
        return 'GEOGRAPHY'

    def varquote(self, val):
        return varquote_aux(val, '"%s"')

    def sequence_name(self, tablename):
        return self.quote('%s_id_seq' % tablename)

    def insert(self, table, fields, values, returning=None):
        ret = ''
        if returning:
            ret = 'RETURNING %s' % self.quote(returning)
        return 'INSERT INTO %s(%s) VALUES (%s)%s;' % (
            table, fields, values, ret)

    @property
    def random(self):
        return 'RANDOM()'

    def add(self, first, second):
        t = first.type
        if t in ('text', 'string', 'password', 'json', 'upload', 'blob'):
            return '(%s || %s)' % (
                self.expand(first), self.expand(second, first.type))
        else:
            return '(%s + %s)' % (
                self.expand(first), self.expand(second, first.type))

    def regexp(self, first, second):
        return '(%s ~ %s)' % (
            self.expand(first), self.expand(second, 'string'))

    def like(self, first, second, escape=None):
        if isinstance(second, Expression):
            second = self.expand(second, 'string')
        else:
            second = self.expand(second, 'string')
            if escape is None:
                escape = '\\'
                second = second.replace(escape, escape * 2)
        if first.type not in ('string', 'text', 'json'):
            return "(%s LIKE %s ESCAPE '%s')" % (
                self.cast(self.expand(first), 'CHAR(%s)' % first.length),
                second, escape)
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.expand(first), second, escape)

    def ilike(self, first, second, escape=None):
        if isinstance(second, Expression):
            second = self.expand(second, 'string')
        else:
            second = self.expand(second, 'string')
            if escape is None:
                escape = '\\'
                second = second.replace(escape, escape * 2)
        if first.type not in ('string', 'text', 'json', 'list:string'):
            return "(%s ILIKE %s ESCAPE '%s')" % (
                self.cast(self.expand(first), 'CHAR(%s)' % first.length),
                second, escape)
        return "(%s ILIKE %s ESCAPE '%s')" % (
            self.expand(first), second, escape)

    def drop(self, table, mode):
        if mode not in ['restrict', 'cascade', '']:
            raise ValueError('Invalid mode: %s' % mode)
        return ['DROP TABLE ' + table.sqlsafe + ' ' + mode + ';']

    def st_asgeojson(self, first, second):
        return 'ST_AsGeoJSON(%s,%s,%s,%s)' % (
            second['version'], self.expand(first), second['precision'],
            second['options'])

    def st_astext(self, first):
        return 'ST_AsText(%s)' % self.expand(first)

    def st_x(self, first):
        return 'ST_X(%s)' % (self.expand(first))

    def st_y(self, first):
        return 'ST_Y(%s)' % (self.expand(first))

    def st_contains(self, first, second):
        return 'ST_Contains(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_distance(self, first, second):
        return 'ST_Distance(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_equals(self, first, second):
        return 'ST_Equals(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_intersects(self, first, second):
        return 'ST_Intersects(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_overlaps(self, first, second):
        return 'ST_Overlaps(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_simplify(self, first, second):
        return 'ST_Simplify(%s,%s)' % (
            self.expand(first), self.expand(second, 'double'))

    def st_simplifypreservetopology(self, first, second):
        return 'ST_SimplifyPreserveTopology(%s,%s)' % (
            self.expand(first), self.expand(second, 'double'))

    def st_touches(self, first, second):
        return 'ST_Touches(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_within(self, first, second):
        return 'ST_Within(%s,%s)' % (
            self.expand(first), self.expand(second, first.type))

    def st_dwithin(self, first, tup):
        return 'ST_DWithin(%s,%s,%s)' % (
            self.expand(first), self.expand(tup[0], first.type),
            self.expand(tup[1], 'double'))


class PostgreDialectJSON(PostgreDialect):
    @sqltype_for('json')
    def type_json(self):
        return 'JSON'
