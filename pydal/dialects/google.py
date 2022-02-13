from ..adapters.google import GoogleDatastore
from .base import NoSQLDialect
from . import dialects, sqltype_for


@dialects.register_for(GoogleDatastore)
class GoogleDatastoreDialect(NoSQLDialect):
    def _and(self, first, second, query_env={}):
        return (first, "and", second)

    def _or(self, first, second, query_env={}):
        raise NotImplemented

    def eq(self, first, second=None, query_env={}):
        return (first, "=", second)

    def ne(self, first, second=None, query_env={}):
        return (first, "!=", second)

    def lt(self, first, second=None, query_env={}):
        return (first, "<", second)

    def lte(self, first, second=None, query_env={}):
        return (first, "<=", second)

    def gt(self, first, second=None, query_env={}):
        return (first, ">", second)

    def gte(self, first, second=None, query_env={}):
        return (first, ">=", second)

    def invert(self, first, second=None, query_env={}):
        return (None, "~", first)

    def comma(self, first, second, query_env={}):
        return (first, ",", second)

    def belongs(self, first, second, query_env={}):
        return (first, "in", second)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        raise NotImplemented

    def _not(self, val, query_env={}):
        raise NotImplemented

