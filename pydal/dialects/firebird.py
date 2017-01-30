from ..adapters.firebird import FireBird
from ..objects import Expression
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(FireBird)
class FireBirdDialect(SQLDialect):
    @sqltype_for('text')
    def type_text(self):
        return 'BLOB SUB_TYPE 1'

    @sqltype_for('bigint')
    def type_bigint(self):
        return 'BIGINT'

    @sqltype_for('double')
    def type_double(self):
        return 'DOUBLE PRECISION'

    @sqltype_for('decimal')
    def type_decimal(self):
        return 'DECIMAL(%(precision)s,%(scale)s)'

    @sqltype_for('blob')
    def type_blob(self):
        return 'BLOB SUB_TYPE 0'

    @sqltype_for('id')
    def type_id(self):
        return 'INTEGER PRIMARY KEY'

    @sqltype_for('big-id')
    def type_big_id(self):
        return 'BIGINT PRIMARY KEY'

    @sqltype_for('reference')
    def type_reference(self):
        return 'INTEGER REFERENCES %(foreign_key)s ' + \
            'ON DELETE %(on_delete_action)s'

    @sqltype_for('big-reference')
    def type_big_reference(self):
        return 'BIGINT REFERENCES %(foreign_key)s ' + \
            'ON DELETE %(on_delete_action)s'

    def sequence_name(self, tablename):
        return self.quote('genid_%s' % tablename)

    def trigger_name(self, tablename):
        return 'trg_id_%s' % tablename

    @property
    def random(self):
        return 'RAND()'

    def not_null(self, default, field_type):
        return 'DEFAULT %s NOT NULL' % \
            self.adapter.represent(default, field_type)

    def epoch(self, val, query_env={}):
        return "DATEDIFF(second, '1970-01-01 00:00:00', %s)" % \
            self.expand(val, query_env=query_env)

    def substring(self, field, parameters, query_env={}):
        return 'SUBSTRING(%s from %s for %s)' % (
            self.expand(field, query_env=query_env), parameters[0],
            parameters[1])

    def length(self, val, query_env={}):
        return "CHAR_LENGTH(%s)" % self.expand(val, query_env=query_env)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        if first.type.startswith('list:'):
            second = Expression(None, self.concat('|', Expression(
                None, self.replace(second, ('|', '||'), query_env)), '|'))
        return '(%s CONTAINING %s)' % (
            self.expand(first, query_env=query_env),
            self.expand(second, 'string', query_env=query_env))

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
            limit = ' FIRST %i' % (lmax - lmin)
            offset = ' SKIP %i' % lmin
        if for_update:
            upd = ' FOR UPDATE'
        return 'SELECT%s%s%s %s FROM %s%s%s%s%s;' % (
            dst, limit, offset, fields, tables, whr, grp, order, upd)

    def drop_table(self, table, mode):
        sequence_name = table._sequence_name
        return [
            'DROP TABLE %s %s;' % (table._rname, mode),
            'DROP GENERATOR %s;' % sequence_name]

    def truncate(self, table, mode=''):
        return [
            'DELETE FROM %s;' % table._rname,
            'SET GENERATOR %s TO 0;' % table._sequence_name]
