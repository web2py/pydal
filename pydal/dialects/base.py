import datetime
from .._compat import integer_types
from ..adapters.base import SQLAdapter
from ..helpers.methods import use_common_filters
from ..objects import Expression, Field
from . import Dialect, dialects, sqltype_for

long = integer_types[-1]


class CommonDialect(Dialect):
    def _force_bigints(self):
        if 'big-id' in self.types and 'reference' in self.types:
            self.types['id'] = self.types['big-id']
            self.types['reference'] = self.types['big-reference']

    def quote(self, val):
        return self.quote_template % val

    def varquote(self, val):
        return val

    def sequence_name(self, tablename):
        return self.quote('%s_sequence' % tablename)

    def trigger_name(self, tablename):
        return '%s_sequence' % tablename

    def coalesce_zero(self, val):
        return self.coalesce(val, [0])


@dialects.register_for(SQLAdapter)
class SQLDialect(CommonDialect):
    quote_template = '"%s"'
    true = "T"
    false = "F"
    true_exp = "1"
    false_exp = "0"
    dt_sep = " "

    @sqltype_for('string')
    def type_string(self):
        return 'VARCHAR(%(length)s)'

    @sqltype_for('boolean')
    def type_boolean(self):
        return 'CHAR(1)'

    @sqltype_for('text')
    def type_text(self):
        return 'TEXT'

    @sqltype_for('json')
    def type_json(self):
        return self.types['text']

    @sqltype_for('password')
    def type_password(self):
        return self.types['string']

    @sqltype_for('blob')
    def type_blob(self):
        return 'BLOB'

    @sqltype_for('upload')
    def type_upload(self):
        return self.types['string']

    @sqltype_for('integer')
    def type_integer(self):
        return 'INTEGER'

    @sqltype_for('bigint')
    def type_bigint(self):
        return self.types['integer']

    @sqltype_for('float')
    def type_float(self):
        return 'FLOAT'

    @sqltype_for('double')
    def type_double(self):
        return 'DOUBLE'

    @sqltype_for('decimal')
    def type_decimal(self):
        return 'NUMERIC(%(precision)s,%(scale)s)'

    @sqltype_for('date')
    def type_date(self):
        return 'DATE'

    @sqltype_for('time')
    def type_time(self):
        return 'TIME'

    @sqltype_for('datetime')
    def type_datetime(self):
        return 'TIMESTAMP'

    @sqltype_for('id')
    def type_id(self):
        return 'INTEGER PRIMARY KEY AUTOINCREMENT'

    @sqltype_for('reference')
    def type_reference(self):
        return 'INTEGER REFERENCES %(foreign_key)s ' + \
            'ON DELETE %(on_delete_action)s %(null)s %(unique)s'

    @sqltype_for('list:integer')
    def type_list_integer(self):
        return self.types['text']

    @sqltype_for('list:string')
    def type_list_string(self):
        return self.types['text']

    @sqltype_for('list:reference')
    def type_list_reference(self):
        return self.types['text']

    @sqltype_for('big-id')
    def type_big_id(self):
        return self.types['id']

    @sqltype_for('big-reference')
    def type_big_reference(self):
        return self.types['reference']

    @sqltype_for('reference FK')
    def type_reference_fk(self):
        return ', CONSTRAINT  "FK_%(constraint_name)s" FOREIGN KEY ' + \
            '(%(field_name)s) REFERENCES %(foreign_key)s ' + \
            'ON DELETE %(on_delete_action)s'

    def alias(self, original, new):
        return ('%s AS ' + self.quote_template) % (original, new)

    def insert(self, table, fields, values):
        return 'INSERT INTO %s(%s) VALUES (%s);' % (table, fields, values)

    def insert_empty(self, table):
        return 'INSERT INTO %s DEFAULT VALUES;' % table

    def where(self, query):
        return 'WHERE %s' % query

    def update(self, tablename, values, where=None):
        whr = ''
        if where:
            whr = ' %s' % self.where(where)
        return 'UPDATE %s SET %s%s;' % (tablename, values, whr)

    def delete(self, tablename, where=None):
        whr = ''
        if where:
            whr = ' %s' % self.where(where)
        return 'DELETE FROM %s%s;' % (tablename, whr)

    def select(self, fields, tables, where=None, groupby=None, having=None,
               orderby=None, limitby=None, distinct=False, for_update=False):
        dst, whr, grp, order, limit, offset, upd = '', '', '', '', '', '', ''
        if distinct is True:
            dst = ' DISTINCT'
        elif distinct:
            dst = ' DISTINCT ON (%s)' % distinct
        if where:
            whr = ' %s' % self.where(where)
        if groupby:
            grp = ' GROUP BY %s' % groupby
            if having:
                grp += ' HAVING %s' % having
        if orderby:
            order = ' ORDER BY %s' % orderby
        if limitby:
            (lmin, lmax) = limitby
            limit = ' LIMIT %i' % (lmax - lmin)
            offset = ' OFFSET %i' % lmin
        if for_update:
            upd = ' FOR UPDATE'
        return 'SELECT%s %s FROM %s%s%s%s%s%s%s;' % (
            dst, fields, tables, whr, grp, order, limit, offset, upd)

    def count(self, val, distinct=None):
        return ('count(%s)' if not distinct else 'count(DISTINCT %s)') % \
            self.expand(val)

    def join(self, val):
        return 'JOIN %s' % val

    def left_join(self, val):
        return 'LEFT JOIN %s' % val

    def cross_join(self, val):
        return 'CROSS JOIN %s' % val

    @property
    def random(self):
        return 'Random()'

    def _as(self, first, second):
        return '%s AS %s' % (self.expand(first), second)

    def cast(self, first, second):
        return 'CAST(%s)' % self._as(first, second)

    def _not(self, val):
        return '(NOT %s)' % self.expand(val)

    def _and(self, first, second):
        return '(%s AND %s)' % (self.expand(first), self.expand(second))

    def _or(self, first, second):
        return '(%s OR %s)' % (self.expand(first), self.expand(second))

    def belongs(self, first, second):
        if isinstance(second, str):
            return '(%s IN (%s))' % (self.expand(first), second[:-1])
        if not second:
            return '(1=0)'
        items = ','.join(self.expand(item, first.type) for item in second)
        return '(%s IN (%s))' % (self.expand(first), items)

    # def regexp(self, first, second):
    #     raise NotImplementedError

    def lower(self, val):
        return 'LOWER(%s)' % self.expand(val)

    def upper(self, first):
        return 'UPPER(%s)' % self.expand(first)

    def like(self, first, second, escape=None):
        """Case sensitive like operator"""
        if isinstance(second, Expression):
            second = self.expand(second, 'string')
        else:
            second = self.expand(second, 'string')
            if escape is None:
                escape = '\\'
                second = second.replace(escape, escape * 2)
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.expand(first), second, escape)

    def ilike(self, first, second, escape=None):
        """Case insensitive like operator"""
        if isinstance(second, Expression):
            second = self.expand(second, 'string')
        else:
            second = self.expand(second, 'string').lower()
            if escape is None:
                escape = '\\'
                second = second.replace(escape, escape*2)
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.lower(first), second, escape)

    def _like_escaper_default(self, term):
        if isinstance(term, Expression):
            return term
        term = term.replace('\\', '\\\\')
        term = term.replace('%', '\%').replace('_', '\_')
        return term

    def startswith(self, first, second):
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first),
            self.expand(self._like_escaper_default(second)+'%', 'string'))

    def endswith(self, first, second):
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first),
            self.expand('%'+self._like_escaper_default(second), 'string'))

    def replace(self, first, tup):
        second, third = tup
        return 'REPLACE(%s,%s,%s)' % (
            self.expand(first, 'string'), self.expand(second, 'string'),
            self.expand(third, 'string'))

    def concat(self, *items):
        return '(%s)' % ' || '.join(self.expand(x, 'string') for x in items)

    def contains(self, first, second, case_sensitive=True):
        if first.type in ('string', 'text', 'json'):
            if isinstance(second, Expression):
                second = Expression(
                    second.db,
                    self.concat('%', Expression(
                        second.db, self.replace(second, ('%', '\%'))), '%'))
            else:
                second = '%'+self._like_escaper_default(str(second))+'%'
        elif first.type.startswith('list:'):
            if isinstance(second, Expression):
                second = Expression(
                    second.db, self.concat('%|', Expression(
                        second.db, self.replace(Expression(
                            second.db, self.replace(
                                second, ('%', '\%'))), ('|', '||'))), '|%'))
            else:
                second = str(second).replace('|', '||')
                second = '%|'+self._like_escaper_default(second)+'|%'
        op = case_sensitive and self.like or self.ilike
        return op(first, second, escape='\\')

    def eq(self, first, second=None):
        if second is None:
            return '(%s IS NULL)' % self.expand(first)
        return '(%s = %s)' % (
            self.expand(first), self.expand(second, first.type))

    def ne(self, first, second=None):
        if second is None:
            return '(%s IS NOT NULL)' % self.expand(first)
        return '(%s <> %s)' % (
            self.expand(first), self.expand(second, first.type))

    def lt(self, first, second=None):
        if second is None:
            raise RuntimeError("Cannot compare %s < None" % first)
        return '(%s < %s)' % (
            self.expand(first), self.expand(second, first.type))

    def lte(self, first, second=None):
        if second is None:
            raise RuntimeError("Cannot compare %s <= None" % first)
        return '(%s <= %s)' % (
            self.expand(first), self.expand(second, first.type))

    def gt(self, first, second=None):
        if second is None:
            raise RuntimeError("Cannot compare %s > None" % first)
        return '(%s > %s)' % (
            self.expand(first), self.expand(second, first.type))

    def gte(self, first, second=None):
        if second is None:
            raise RuntimeError("Cannot compare %s >= None" % first)
        return '(%s >= %s)' % (
            self.expand(first), self.expand(second, first.type))

    def _is_numerical(self, field_type):
        return field_type in ('integer', 'boolean', 'double', 'bigint') or \
            field_type.startswith('decimal')

    def add(self, first, second):
        if self._is_numerical(first.type) or isinstance(first.type, Field):
            return '(%s + %s)' % (
                self.expand(first), self.expand(second, first.type))
        else:
            return self.concat(first, second)

    def sub(self, first, second):
        return '(%s - %s)' % (
            self.expand(first), self.expand(second, first.type))

    def mul(self, first, second):
        return '(%s * %s)' % (
            self.expand(first), self.expand(second, first.type))

    def div(self, first, second):
        return '(%s / %s)' % (
            self.expand(first), self.expand(second, first.type))

    def mod(self, first, second):
        return '(%s %% %s)' % (
            self.expand(first), self.expand(second, first.type))

    def on(self, first, second):
        table_rname = self.adapter.table_alias(first)
        if use_common_filters(second):
            second = self.adapter.common_filter(second, [first._tablename])
        return ('%s ON %s') % (self.expand(table_rname), self.expand(second))

    def invert(self, first):
        return '%s DESC' % self.expand(first)

    def comma(self, first, second):
        return '%s, %s' % (self.expand(first), self.expand(second))

    def extract(self, first, what):
        return "EXTRACT(%s FROM %s)" % (what, self.expand(first))

    def epoch(self, val):
        return self.extract(val, 'epoch')

    def length(self, val):
        return "LENGTH(%s)" % self.expand(val)

    def aggregate(self, first, what):
        return "%s(%s)" % (what, self.expand(first))

    def not_null(self, default, field_type):
        return 'NOT NULL DEFAULT %s' % \
            self.adapter.represent(default, field_type)

    @property
    def allow_null(self):
        return ''

    def coalesce(self, first, second):
        expressions = [self.expand(first)] + \
            [self.expand(val, first.type) for val in second]
        return 'COALESCE(%s)' % ','.join(expressions)

    def raw(self, val):
        return val

    def substring(self, field, parameters):
        return 'SUBSTR(%s,%s,%s)' % (
            self.expand(field), parameters[0], parameters[1])

    def case(self, query, true_false):
        _types = {bool: 'boolean', int: 'integer', float: 'double'}
        return 'CASE WHEN %s THEN %s ELSE %s END' % (
            self.expand(query),
            self.adapter.represent(
                true_false[0], _types.get(type(true_false[0]), 'string')),
            self.adapter.represent(
                true_false[1], _types.get(type(true_false[1]), 'string')))

    def primary_key(self, key):
        return 'PRIMARY KEY(%s)' % key

    def drop_table(self, table, mode):
        return ['DROP TABLE %s;' % table.sqlsafe]

    def truncate(self, table, mode=''):
        if mode:
            mode = " %s" % mode
        return ['TRUNCATE TABLE %s%s;' % (table.sqlsafe, mode)]

    def create_index(self, name, table, expressions, unique=False):
        uniq = ' UNIQUE' if unique else ''
        with self.adapter.index_expander():
            rv = 'CREATE%s INDEX %s ON %s (%s);' % (
                uniq, self.quote(name), table.sqlsafe, ','.join(
                    self.expand(field) for field in expressions))
        return rv

    def drop_index(self, name, table):
        return 'DROP INDEX %s;' % self.quote(name)

    def constraint_name(self, table, fieldname):
        return '%s_%s__constraint' % (table, fieldname)

    def concat_add(self, tablename):
        return ', ADD '


class NoSQLDialect(CommonDialect):
    quote_template = '%s'

    @sqltype_for('string')
    def type_string(self):
        return str

    @sqltype_for('boolean')
    def type_boolean(self):
        return bool

    @sqltype_for('text')
    def type_text(self):
        return str

    @sqltype_for('json')
    def type_json(self):
        return self.types['text']

    @sqltype_for('password')
    def type_password(self):
        return self.types['string']

    @sqltype_for('blob')
    def type_blob(self):
        return self.types['text']

    @sqltype_for('upload')
    def type_upload(self):
        return self.types['string']

    @sqltype_for('integer')
    def type_integer(self):
        return long

    @sqltype_for('bigint')
    def type_bigint(self):
        return self.types['integer']

    @sqltype_for('float')
    def type_float(self):
        return float

    @sqltype_for('double')
    def type_double(self):
        return self.types['float']

    @sqltype_for('date')
    def type_date(self):
        return datetime.date

    @sqltype_for('time')
    def type_time(self):
        return datetime.time

    @sqltype_for('datetime')
    def type_datetime(self):
        return datetime.datetime

    @sqltype_for('id')
    def type_id(self):
        return long

    @sqltype_for('reference')
    def type_reference(self):
        return long

    @sqltype_for('list:integer')
    def type_list_integer(self):
        return list

    @sqltype_for('list:string')
    def type_list_string(self):
        return list

    @sqltype_for('list:reference')
    def type_list_reference(self):
        return list

    def quote(self, val):
        return val
