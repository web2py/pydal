from .._gae import ndb
from ..adapters.google import GoogleDatastore
from ..helpers.gae import NDBDecimalProperty
from .base import NoSQLDialect
from . import dialects, sqltype_for


@dialects.register_for(GoogleDatastore)
class GoogleDatastoreDialect(NoSQLDialect):
    FILTER_OPTIONS = {
        "=": lambda a, b: a == b,
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">=": lambda a, b: a >= b,
        "!=": lambda a, b: a != b,
        "in": lambda a, b: a.IN(b),
    }

    @sqltype_for("string")
    def type_string(self):
        return lambda **kwargs: ndb.StringProperty(**kwargs)

    @sqltype_for("boolean")
    def type_boolean(self):
        return ndb.BooleanProperty

    @sqltype_for("text")
    def type_text(self):
        return ndb.TextProperty

    @sqltype_for("json")
    def type_json(self):
        return self.types["text"]

    @sqltype_for("password")
    def type_password(self):
        return ndb.StringProperty

    @sqltype_for("blob")
    def type_blob(self):
        return ndb.BlobProperty

    @sqltype_for("upload")
    def type_upload(self):
        return self.types["password"]

    @sqltype_for("integer")
    def type_integer(self):
        return ndb.IntegerProperty

    @sqltype_for("bigint")
    def type_bigint(self):
        return self.types["integer"]

    @sqltype_for("float")
    def type_float(self):
        return ndb.FloatProperty

    @sqltype_for("double")
    def type_double(self):
        return self.types["float"]

    @sqltype_for("decimal")
    def type_decimal(self):
        return NDBDecimalProperty

    @sqltype_for("date")
    def type_date(self):
        return ndb.DateProperty

    @sqltype_for("time")
    def type_time(self):
        return ndb.TimeProperty

    @sqltype_for("datetime")
    def type_datetime(self):
        return ndb.DateTimeProperty

    @sqltype_for("id")
    def type_id(self):
        return None

    @sqltype_for("reference")
    def type_reference(self):
        return ndb.IntegerProperty

    @sqltype_for("list:integer")
    def type_list_integer(self):
        return lambda **kwargs: ndb.IntegerProperty(
            repeated=True, default=None, **kwargs
        )

    @sqltype_for("list:string")
    def type_list_string(self):
        return lambda **kwargs: ndb.StringProperty(
            repeated=True, default=None, **kwargs
        )

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return lambda **kwargs: ndb.IntegerProperty(
            repeated=True, default=None, **kwargs
        )

    def _and(self, first, second, query_env={}):
        first = self.expand(first, query_env=query_env)
        second = self.expand(second, query_env=query_env)
        # none means lack of query (true)
        if first == None:
            return second
        return ndb.AND(first, second)

    def _or(self, first, second, query_env={}):
        first = self.expand(first, query_env=query_env)
        second = self.expand(second, query_env=query_env)
        # none means lack of query (true)
        if first == None or second == None:
            return None
        return ndb.OR(first, second)

    def __gaef(self, first, op, second):
        name = first.name if first.name != "id" else "key"
        if name == "key" and op in (">", "!=") and second in (0, "0", None):
            return None
        field = getattr(first.table._tableobj, name)
        value = self.adapter.represent(second, first.type, first._tablename)
        return self.FILTER_OPTIONS[op](field, value)

    def eq(self, first, second=None, query_env={}):
        return self.__gaef(first, "=", second)

    def ne(self, first, second=None, query_env={}):
        return self.__gaef(first, "!=", second)

    def lt(self, first, second=None, query_env={}):
        return self.__gaef(first, "<", second)

    def lte(self, first, second=None, query_env={}):
        return self.__gaef(first, "<=", second)

    def gt(self, first, second=None, query_env={}):
        return self.__gaef(first, ">", second)

    def gte(self, first, second=None, query_env={}):
        return self.__gaef(first, ">=", second)

    def invert(self, first, query_env={}):
        return "-%s" % first.name

    def comma(self, first, second, query_env={}):
        return "%s,%s" % (first, second)

    def belongs(self, first, second, query_env={}):
        if not isinstance(second, (list, tuple, set)):
            raise SyntaxError("Not supported")
        if not isinstance(second, list):
            second = list(second)
        if len(second) == 0:
            # return a filter which will return a null set
            f = self.eq(first, 0)
            f.filter_all = True
            return f
        return self.__gaef(first, "in", second)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        # silently ignoring: GAE can only do case sensitive matches!
        if not first.type.startswith("list:"):
            raise SyntaxError("Not supported")
        return self.__gaef(first, "=", second)

    def _not(self, val, query_env={}):
        op, f, s = val.op, val.first, val.second
        if op in [self._or, self._and]:
            not_op = self._and if op == self._or else self._or
            rv = not_op(self._not(f), self._not(s))
        elif op == self.eq:
            rv = self.__gaef(f, "!=", s)
        elif op == self.ne:
            rv = self.__gaef(f, "=", s)
        elif op == self.lt:
            rv = self.__gaef(f, ">=", s)
        elif op == self.lte:
            rv = self.__gaef(f, ">", s)
        elif op == self.gt:
            rv = self.__gaef(f, "<=", s)
        elif op == self.gte:
            rv = self.__gaef(f, "<", s)
        else:
            # TODO the IN operator must be split into a sequence of
            # (field!=value) AND (field!=value) AND ...
            raise NotImplementedError
        return rv
