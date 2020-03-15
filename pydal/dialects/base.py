import datetime
from .._compat import integer_types, basestring, string_types
from ..adapters.base import SQLAdapter
from ..helpers.methods import use_common_filters
from ..objects import Expression, Field, Table, Select
from . import Dialect, dialects, sqltype_for

long = integer_types[-1]


class CommonDialect(Dialect):
    quote_template = "%s"

    def _force_bigints(self):
        if "big-id" in self.types and "reference" in self.types:
            self.types["id"] = self.types["big-id"]
            self.types["reference"] = self.types["big-reference"]

    def quote(self, val):
        return self.quote_template % val

    def varquote(self, val):
        return val

    def sequence_name(self, tablename):
        return self.quote("%s_sequence" % tablename)

    def trigger_name(self, tablename):
        return "%s_sequence" % tablename

    def coalesce_zero(self, val, query_env={}):
        return self.coalesce(val, [0], query_env)


@dialects.register_for(SQLAdapter)
class SQLDialect(CommonDialect):
    quote_template = '"%s"'
    true = "T"
    false = "F"
    true_exp = "1"
    false_exp = "0"
    dt_sep = " "

    @sqltype_for("string")
    def type_string(self):
        return "VARCHAR(%(length)s)"

    @sqltype_for("boolean")
    def type_boolean(self):
        return "CHAR(1)"

    @sqltype_for("text")
    def type_text(self):
        return "TEXT"

    @sqltype_for("json")
    def type_json(self):
        return self.types["text"]

    @sqltype_for("password")
    def type_password(self):
        return self.types["string"]

    @sqltype_for("blob")
    def type_blob(self):
        return "BLOB"

    @sqltype_for("upload")
    def type_upload(self):
        return self.types["string"]

    @sqltype_for("integer")
    def type_integer(self):
        return "INTEGER"

    @sqltype_for("bigint")
    def type_bigint(self):
        return self.types["integer"]

    @sqltype_for("float")
    def type_float(self):
        return "FLOAT"

    @sqltype_for("double")
    def type_double(self):
        return "DOUBLE"

    @sqltype_for("decimal")
    def type_decimal(self):
        return "NUMERIC(%(precision)s,%(scale)s)"

    @sqltype_for("date")
    def type_date(self):
        return "DATE"

    @sqltype_for("time")
    def type_time(self):
        return "TIME"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "TIMESTAMP"

    @sqltype_for("id")
    def type_id(self):
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INTEGER REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s %(null)s %(unique)s"
        )

    @sqltype_for("list:integer")
    def type_list_integer(self):
        return self.types["text"]

    @sqltype_for("list:string")
    def type_list_string(self):
        return self.types["text"]

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return self.types["text"]

    @sqltype_for("big-id")
    def type_big_id(self):
        return self.types["id"]

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return self.types["reference"]

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ', CONSTRAINT  "FK_%(constraint_name)s" FOREIGN KEY '
            + "(%(field_name)s) REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s"
        )

    def alias(self, original, new):
        return ("%s AS " + self.quote_template) % (original, new)

    def insert(self, table, fields, values):
        return "INSERT INTO %s(%s) VALUES (%s);" % (table, fields, values)

    def insert_empty(self, table):
        return "INSERT INTO %s DEFAULT VALUES;" % table

    def where(self, query):
        return "WHERE %s" % query

    def update(self, table, values, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "UPDATE %s SET %s%s;" % (tablename, values, whr)

    def delete(self, table, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "DELETE FROM %s%s;" % (tablename, whr)

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
            limit = " LIMIT %i" % (lmax - lmin)
            offset = " OFFSET %i" % lmin
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s %s FROM %s%s%s%s%s%s%s;" % (
            dst,
            fields,
            tables,
            whr,
            grp,
            order,
            limit,
            offset,
            upd,
        )

    def count(self, val, distinct=None, query_env={}):
        return ("COUNT(%s)" if not distinct else "COUNT(DISTINCT %s)") % self.expand(
            val, query_env=query_env
        )

    def join(self, val, query_env={}):
        if isinstance(val, (Table, Select)):
            val = val.query_name(query_env.get("parent_scope", []))
        elif not isinstance(val, basestring):
            val = self.expand(val, query_env=query_env)
        return "JOIN %s" % val

    def left_join(self, val, query_env={}):
        # Left join must always have an ON clause
        if not isinstance(val, basestring):
            val = self.expand(val, query_env=query_env)
        return "LEFT JOIN %s" % val

    def cross_join(self, val, query_env={}):
        if isinstance(val, (Table, Select)):
            val = val.query_name(query_env.get("parent_scope", []))
        elif not isinstance(val, basestring):
            val = self.expand(val, query_env=query_env)
        return "CROSS JOIN %s" % val

    @property
    def random(self):
        return "Random()"

    def _as(self, first, second, query_env={}):
        return "%s AS %s" % (self.expand(first, query_env=query_env), second)

    def cast(self, first, second, query_env={}):
        return "CAST(%s)" % self._as(first, second, query_env)

    def _not(self, val, query_env={}):
        return "(NOT %s)" % self.expand(val, query_env=query_env)

    def _and(self, first, second, query_env={}):
        return "(%s AND %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def _or(self, first, second, query_env={}):
        return "(%s OR %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def belongs(self, first, second, query_env={}):
        ftype = first.type
        first = self.expand(first, query_env=query_env)
        if isinstance(second, str):
            return "(%s IN (%s))" % (first, second[:-1])
        elif isinstance(second, Select):
            if len(second._qfields) != 1:
                raise ValueError("Subquery in belongs() must select exactly 1 column")
            sub = second._compile(query_env.get("current_scope", []))[1][:-1]
            return "(%s IN (%s))" % (first, sub)
        if not second:
            return "(1=0)"
        items = ",".join(
            self.expand(item, ftype, query_env=query_env) for item in second
        )
        return "(%s IN (%s))" % (first, items)

    # def regexp(self, first, second):
    #     raise NotImplementedError

    def lower(self, val, query_env={}):
        return "LOWER(%s)" % self.expand(val, query_env=query_env)

    def upper(self, first, query_env={}):
        return "UPPER(%s)" % self.expand(first, query_env=query_env)

    def like(self, first, second, escape=None, query_env={}):
        """Case sensitive like operator"""
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env)
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.expand(first, query_env=query_env),
            second,
            escape,
        )

    def ilike(self, first, second, escape=None, query_env={}):
        """Case insensitive like operator"""
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env).lower()
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.lower(first, query_env=query_env),
            second,
            escape,
        )

    def _like_escaper_default(self, term):
        if isinstance(term, Expression):
            return term
        term = term.replace("\\", "\\\\")
        term = term.replace(r"%", r"\%").replace("_", r"\_")
        return term

    def startswith(self, first, second, query_env={}):
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first, query_env=query_env),
            self.expand(
                self._like_escaper_default(second) + "%", "string", query_env=query_env
            ),
        )

    def endswith(self, first, second, query_env={}):
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first, query_env=query_env),
            self.expand(
                "%" + self._like_escaper_default(second), "string", query_env=query_env
            ),
        )

    def replace(self, first, tup, query_env={}):
        second, third = tup
        return "REPLACE(%s,%s,%s)" % (
            self.expand(first, "string", query_env=query_env),
            self.expand(second, "string", query_env=query_env),
            self.expand(third, "string", query_env=query_env),
        )

    def concat(self, *items, **kwargs):
        query_env = kwargs.get("query_env", {})
        tmp = (self.expand(x, "string", query_env=query_env) for x in items)
        return "(%s)" % " || ".join(tmp)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        if first.type in ("string", "text", "json", "jsonb"):
            if isinstance(second, Expression):
                second = Expression(
                    second.db,
                    self.concat(
                        "%",
                        Expression(
                            second.db,
                            self.replace(second, (r"%", r"\%"), query_env=query_env),
                        ),
                        r"%",
                    ),
                )
            else:
                second = "%" + self._like_escaper_default(str(second)) + r"%"
        elif first.type.startswith("list:"):
            if isinstance(second, Expression):
                second = Expression(
                    second.db,
                    self.concat(
                        r"%|",
                        Expression(
                            second.db,
                            self.replace(
                                Expression(
                                    second.db,
                                    self.replace(second, (r"%", r"\%"), query_env),
                                ),
                                ("|", "||"),
                            ),
                        ),
                        r"|%",
                    ),
                )
            else:
                second = str(second).replace("|", "||")
                second = "%|" + self._like_escaper_default(second) + "|%"
        op = case_sensitive and self.like or self.ilike
        return op(first, second, escape="\\", query_env=query_env)

    def eq(self, first, second=None, query_env={}):
        if second is None:
            return "(%s IS NULL)" % self.expand(first, query_env=query_env)
        if first.type in ("json", "jsonb"):
            if isinstance(second, (string_types, int, float)):
                return "(%s = '%s')" % (
                    self.expand(first, query_env=query_env),
                    self.expand(second, query_env=query_env)
                )
        return "(%s = %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def ne(self, first, second=None, query_env={}):
        if second is None:
            return "(%s IS NOT NULL)" % self.expand(first, query_env=query_env)
        if first.type in ("json", "jsonb"):
            if isinstance(second, (string_types, int, float)):
                return "(%s <> '%s')" % (
                    self.expand(first, query_env=query_env),
                    self.expand(second, query_env=query_env)
                )
        return "(%s <> %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def lt(self, first, second=None, query_env={}):
        if second is None:
            raise RuntimeError("Cannot compare %s < None" % first)
        if first.type in ("json", "jsonb"):
            if isinstance(second, (string_types, int, float)):
                return "(%s < '%s')" % (
                    self.expand(first, query_env=query_env),
                    self.expand(second, query_env=query_env)
                )
        return "(%s < %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def lte(self, first, second=None, query_env={}):
        if second is None:
            raise RuntimeError("Cannot compare %s <= None" % first)
        if first.type in ("json", "jsonb"):
            if isinstance(second, (string_types, int, float)):
                return "(%s <= '%s')" % (
                    self.expand(first, query_env=query_env),
                    self.expand(second, query_env=query_env)
                )
        return "(%s <= %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def gt(self, first, second=None, query_env={}):
        if second is None:
            raise RuntimeError("Cannot compare %s > None" % first)
        if first.type in ("json", "jsonb"):
            if isinstance(second, (string_types, int, float)):
                return "(%s > '%s')" % (
                    self.expand(first, query_env=query_env),
                    self.expand(second, query_env=query_env)
                )
        return "(%s > %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def gte(self, first, second=None, query_env={}):
        if second is None:
            raise RuntimeError("Cannot compare %s >= None" % first)
        if first.type in ("json", "jsonb"):
            if isinstance(second, (string_types, int, float)):
                return "(%s >= '%s')" % (
                    self.expand(first, query_env=query_env),
                    self.expand(second, query_env=query_env)
                )
        return "(%s >= %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def _is_numerical(self, field_type):
        return field_type in (
            "integer",
            "float",
            "double",
            "bigint",
            "boolean",
        ) or field_type.startswith("decimal")

    def add(self, first, second, query_env={}):
        if self._is_numerical(first.type) or isinstance(first.type, Field):
            return "(%s + %s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, first.type, query_env=query_env),
            )
        else:
            return self.concat(first, second, query_env=query_env)

    def sub(self, first, second, query_env={}):
        return "(%s - %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def mul(self, first, second, query_env={}):
        return "(%s * %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def div(self, first, second, query_env={}):
        return "(%s / %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def mod(self, first, second, query_env={}):
        return "(%s %% %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def on(self, first, second, query_env={}):
        table_rname = first.query_name(query_env.get("parent_scope", []))[0]
        if use_common_filters(second):
            second = self.adapter.common_filter(second, [first])
        return ("%s ON %s") % (table_rname, self.expand(second, query_env=query_env))

    def invert(self, first, query_env={}):
        return "%s DESC" % self.expand(first, query_env=query_env)

    def comma(self, first, second, query_env={}):
        return "%s, %s" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def extract(self, first, what, query_env={}):
        return "EXTRACT(%s FROM %s)" % (what, self.expand(first, query_env=query_env))

    def epoch(self, val, query_env={}):
        return self.extract(val, "epoch", query_env)

    def length(self, val, query_env={}):
        return "LENGTH(%s)" % self.expand(val, query_env=query_env)

    def aggregate(self, first, what, query_env={}):
        return "%s(%s)" % (what, self.expand(first, query_env=query_env))

    def not_null(self, default, field_type):
        return "NOT NULL DEFAULT %s" % self.adapter.represent(default, field_type)

    @property
    def allow_null(self):
        return ""

    def coalesce(self, first, second, query_env={}):
        expressions = [self.expand(first, query_env=query_env)] + [
            self.expand(val, first.type, query_env=query_env) for val in second
        ]
        return "COALESCE(%s)" % ",".join(expressions)

    def raw(self, val, query_env={}):
        return val

    def substring(self, field, parameters, query_env={}):
        return "SUBSTR(%s,%s,%s)" % (
            self.expand(field, query_env=query_env),
            parameters[0],
            parameters[1],
        )

    def case(self, query, true_false, query_env={}):
        _types = {bool: "boolean", int: "integer", float: "double"}
        return "CASE WHEN %s THEN %s ELSE %s END" % (
            self.expand(query, query_env=query_env),
            self.adapter.represent(
                true_false[0], _types.get(type(true_false[0]), "string")
            ),
            self.adapter.represent(
                true_false[1], _types.get(type(true_false[1]), "string")
            ),
        )

    def primary_key(self, key):
        return "PRIMARY KEY(%s)" % key

    def drop_table(self, table, mode):
        return ["DROP TABLE %s;" % table._rname]

    def truncate(self, table, mode=""):
        if mode:
            mode = " %s" % mode
        return ["TRUNCATE TABLE %s%s;" % (table._rname, mode)]

    def create_index(self, name, table, expressions, unique=False):
        uniq = " UNIQUE" if unique else ""
        with self.adapter.index_expander():
            rv = "CREATE%s INDEX %s ON %s (%s);" % (
                uniq,
                self.quote(name),
                table._rname,
                ",".join(self.expand(field) for field in expressions),
            )
        return rv

    def drop_index(self, name, table):
        return "DROP INDEX %s;" % self.quote(name)

    def constraint_name(self, table, fieldname):
        return "%s_%s__constraint" % (table, fieldname)

    def concat_add(self, tablename):
        return ", ADD "

    def writing_alias(self, table):
        return table.sql_fullref


class NoSQLDialect(CommonDialect):
    @sqltype_for("string")
    def type_string(self):
        return str

    @sqltype_for("boolean")
    def type_boolean(self):
        return bool

    @sqltype_for("text")
    def type_text(self):
        return str

    @sqltype_for("json")
    def type_json(self):
        return self.types["text"]

    @sqltype_for("password")
    def type_password(self):
        return self.types["string"]

    @sqltype_for("blob")
    def type_blob(self):
        return self.types["text"]

    @sqltype_for("upload")
    def type_upload(self):
        return self.types["string"]

    @sqltype_for("integer")
    def type_integer(self):
        return long

    @sqltype_for("bigint")
    def type_bigint(self):
        return self.types["integer"]

    @sqltype_for("float")
    def type_float(self):
        return float

    @sqltype_for("double")
    def type_double(self):
        return self.types["float"]

    @sqltype_for("date")
    def type_date(self):
        return datetime.date

    @sqltype_for("time")
    def type_time(self):
        return datetime.time

    @sqltype_for("datetime")
    def type_datetime(self):
        return datetime.datetime

    @sqltype_for("id")
    def type_id(self):
        return long

    @sqltype_for("reference")
    def type_reference(self):
        return long

    @sqltype_for("list:integer")
    def type_list_integer(self):
        return list

    @sqltype_for("list:string")
    def type_list_string(self):
        return list

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return list

    def quote(self, val):
        return val
