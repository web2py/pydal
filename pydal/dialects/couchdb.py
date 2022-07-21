from ..adapters.couchdb import CouchDB
from . import dialects
from .base import NoSQLDialect


@dialects.register_for(CouchDB)
class CouchDBDialect(NoSQLDialect):
    def _and(self, first, second, query_env={}):
        return "(%s && %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def _or(self, first, second, query_env={}):
        return "(%s || %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def eq(self, first, second=None, query_env={}):
        if second is None:
            return "(%s == null)" % self.expand(first, query_env=query_env)
        return "(%s == %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def ne(self, first, second=None, query_env={}):
        if second is None:
            return "(%s != null)" % self.expand(first, query_env=query_env)
        return "(%s != %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def comma(self, first, second, query_env={}):
        return "%s + %s" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )
