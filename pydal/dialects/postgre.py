from ..adapters.postgres import Postgre, PostgreNew
from ..helpers.methods import varquote_aux
from ..objects import Expression
from .base import SQLDialect
from . import dialects, sqltype_for, register_expression


@dialects.register_for(Postgre)
class PostgreDialect(SQLDialect):
    true_exp = "TRUE"
    false_exp = "FALSE"

    @sqltype_for('blob')
    def type_blob(self):
        return 'BYTEA'

    @sqltype_for('bigint')
    def type_bigint(self):
        return 'BIGINT'

    @sqltype_for('double')
    def type_double(self):
        return 'FLOAT8'

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

    def drop_table(self, table, mode):
        if mode not in ['restrict', 'cascade', '']:
            raise ValueError('Invalid mode: %s' % mode)
        return ['DROP TABLE ' + table.sqlsafe + ' ' + mode + ';']

    def create_index(self, name, table, expressions, unique=False, where=None):
        uniq = ' UNIQUE' if unique else ''
        whr = ''
        if where:
            whr = ' %s' % self.where(where)
        with self.adapter.index_expander():
            rv = 'CREATE%s INDEX %s ON %s (%s)%s;' % (
                uniq, self.quote(name), table.sqlsafe, ','.join(
                    self.expand(field) for field in expressions), whr)
        return rv

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

    @register_expression('doy')
    def extract_doy(self, expr):
        return Expression(expr.db, self.extract, expr, 'doy', 'integer')

    @register_expression('dow')
    def extract_dow(self, expr):
        return Expression(expr.db, self.extract, expr, 'dow', 'integer')

    @register_expression('isodow')
    def extract_isodow(self, expr):
        return Expression(expr.db, self.extract, expr, 'isodow', 'integer')

    @register_expression('isoyear')
    def extract_isoyear(self, expr):
        return Expression(expr.db, self.extract, expr, 'isoyear', 'integer')

    @register_expression('quarter')
    def extract_quarter(self, expr):
        return Expression(expr.db, self.extract, expr, 'quarter', 'integer')

    @register_expression('week')
    def extract_week(self, expr):
        return Expression(expr.db, self.extract, expr, 'week', 'integer')

    @register_expression('decade')
    def extract_decade(self, expr):
        return Expression(expr.db, self.extract, expr, 'decade', 'integer')

    @register_expression('century')
    def extract_century(self, expr):
        return Expression(expr.db, self.extract, expr, 'century', 'integer')

    @register_expression('millenium')
    def extract_millenium(self, expr):
        return Expression(expr.db, self.extract, expr, 'millenium', 'integer')


class PostgreDialectJSON(PostgreDialect):
    @sqltype_for('json')
    def type_json(self):
        return 'JSON'


@dialects.register_for(PostgreNew)
class PostgreDialectArrays(PostgreDialect):
    @sqltype_for('list:integer')
    def type_list_integer(self):
        return 'BIGINT[]'

    @sqltype_for('list:string')
    def type_list_string(self):
        return 'TEXT[]'

    @sqltype_for('list:reference')
    def type_list_reference(self):
        return 'BIGINT[]'

    def any(self, val):
        return "ANY(%s)" % self.expand(val)

    def contains(self, first, second, case_sensitive=True):
        if first.type.startswith('list:'):
            f = self.expand(second, 'string')
            s = self.any(first)
            if case_sensitive is True:
                return self.eq(f, s)
            return self.ilike(f, s, escape='\\')
        return super(PostgreDialectArrays, self).contains(
            first, second, case_sensitive=case_sensitive)

    def ilike(self, first, second, escape=None):
        if first and 'type' not in first:
            args = (first, self.expand(second))
            return '(%s ILIKE %s)' % args
        return super(PostgreDialectArrays, self).ilike(
            first, second, escape=escape)

    def EQ(self, first, second=None):
        if first and 'type' not in first:
            return '(%s = %s)' % (first, self.expand(second))
        return super(PostgreDialectArrays, self).eq(first, second)


class PostgreDialectArraysJSON(PostgreDialectArrays):
    @sqltype_for('json')
    def type_json(self):
        return 'JSON'
