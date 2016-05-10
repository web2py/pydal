from ..adapters.mysql import MySQL
from ..helpers.methods import varquote_aux
from .base import SQLDialect
from . import dialects, sqltype_for


@dialects.register_for(MySQL)
class MySQLDialect(SQLDialect):
    quote_template = '`%s`'

    @sqltype_for('text')
    def type_text(self):
        return 'LONGTEXT'

    @sqltype_for('blob')
    def type_blob(self):
        return 'LONGBLOB'

    @sqltype_for('bigint')
    def type_bigint(self):
        return 'BIGINT'

    @sqltype_for('id')
    def type_id(self):
        return 'INT AUTO_INCREMENT NOT NULL'

    @sqltype_for('big-id')
    def type_big_id(self):
        return 'BIGINT AUTO_INCREMENT NOT NULL'

    @sqltype_for('reference')
    def type_reference(self):
        return 'INT %(null)s %(unique)s, INDEX %(index_name)s ' + \
            '(%(field_name)s), FOREIGN KEY (%(field_name)s) REFERENCES ' + \
            '%(foreign_key)s ON DELETE %(on_delete_action)s'

    @sqltype_for('big-reference')
    def type_big_reference(self):
        return 'BIGINT %(null)s %(unique)s, INDEX %(index_name)s ' + \
            '(%(field_name)s), FOREIGN KEY (%(field_name)s) REFERENCES ' + \
            '%(foreign_key)s ON DELETE %(on_delete_action)s'

    @sqltype_for('reference FK')
    def type_reference_fk(self):
        return ', CONSTRAINT `FK_%(constraint_name)s` FOREIGN KEY ' + \
            '(%(field_name)s) REFERENCES %(foreign_key)s ON DELETE ' + \
            '%(on_delete_action)s'

    def varquote(self, val):
        return varquote_aux(val, '`%s`')

    def insert_empty(self, table):
        return 'INSERT INTO %s VALUES (DEFAULT);' % table

    @property
    def random(self):
        return 'RAND()'

    def substring(self, field, parameters):
        return 'SUBSTRING(%s,%s,%s)' % (
            self.expand(field), parameters[0], parameters[1])

    def epoch(self, first):
        return "UNIX_TIMESTAMP(%s)" % self.expand(first)

    def concat(self, *items):
        return 'CONCAT(%s)' % ','.join(self.expand(x, 'string') for x in items)

    def regexp(self, first, second):
        return '(%s REGEXP %s)' % (
            self.expand(first), self.expand(second, 'string'))

    def cast(self, first, second):
        if second == 'LONGTEXT':
            second = 'CHAR'
        return 'CAST(%s AS %s)' % (first, second)

    def drop_table(self, table, mode):
        # breaks db integrity but without this mysql does not drop table
        return [
            'SET FOREIGN_KEY_CHECKS=0;', 'DROP TABLE %s;' % table.sqlsafe,
            'SET FOREIGN_KEY_CHECKS=1;']

    def drop_index(self, name, table):
        return 'DROP INDEX %s ON %s;' % (self.quote(name), table.sqlsafe)
