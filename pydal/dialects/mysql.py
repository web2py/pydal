from ..adapters.mysql import MySQL
from ..helpers.methods import varquote_aux
from . import dialects, sqltype_for
from .base import SQLDialect


@dialects.register_for(MySQL)
class MySQLDialect(SQLDialect):
    quote_template = "`%s`"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "DATETIME"

    @sqltype_for("text")
    def type_text(self):
        return "LONGTEXT"

    @sqltype_for("blob")
    def type_blob(self):
        return "LONGBLOB"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("id")
    def type_id(self):
        return "INT AUTO_INCREMENT NOT NULL"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT AUTO_INCREMENT NOT NULL"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INT %(null)s %(unique)s, INDEX %(index_name)s "
            + "(%(field_name)s), FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT %(null)s %(unique)s, INDEX %(index_name)s "
            + "(%(field_name)s), FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ", CONSTRAINT `FK_%(constraint_name)s` FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_key)s ON DELETE "
            + "%(on_delete_action)s"
        )

    def varquote(self, val):
        return varquote_aux(val, "`%s`")

    def insert_empty(self, table):
        return "INSERT INTO %s VALUES (DEFAULT);" % table

    def delete(self, table, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "DELETE %s FROM %s%s;" % (table.sql_shortref, tablename, whr)

    @property
    def random(self):
        return "RAND()"

    def substring(self, field, parameters, query_env={}):
        return "SUBSTRING(%s,%s,%s)" % (
            self.expand(field, query_env=query_env),
            parameters[0],
            parameters[1],
        )

    def epoch(self, first, query_env={}):
        return "UNIX_TIMESTAMP(%s)" % self.expand(first, query_env=query_env)

    def concat(self, *items, **kwargs):
        query_env = kwargs.get("query_env", {})
        tmp = (self.expand(x, "string", query_env=query_env) for x in items)
        return "CONCAT(%s)" % ",".join(tmp)

    def regexp(self, first, second, match_parameter=None, query_env={}):
        return "(%s REGEXP %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
        )

    def cast(self, first, second, query_env={}):
        if second == "LONGTEXT":
            second = "CHAR"
        return "CAST(%s AS %s)" % (first, second)

    def drop_table(self, table, mode):
        # breaks db integrity but without this mysql does not drop table
        return [
            "SET FOREIGN_KEY_CHECKS=0;",
            "DROP TABLE %s;" % table._rname,
            "SET FOREIGN_KEY_CHECKS=1;",
        ]

    def drop_index(self, name, table, if_exists=False):
        return "DROP INDEX %s ON %s;" % (self.quote(name), table._rname)
