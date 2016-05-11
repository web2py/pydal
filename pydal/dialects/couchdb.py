from ..adapters.couchdb import CouchDB
from .base import NoSQLDialect
from . import dialects


@dialects.register_for(CouchDB)
class CouchDBDialect(NoSQLDialect):
    def _and(self, first, second):
        return '(%s && %s)' % (self.expand(first), self.expand(second))

    def _or(self, first, second):
        return '(%s || %s)' % (self.expand(first), self.expand(second))

    def eq(self, first, second=None):
        if second is None:
            return '(%s == null)' % self.expand(first)
        return '(%s == %s)' % (
            self.expand(first), self.expand(second, first.type))

    def ne(self, first, second=None):
        if second is None:
            return '(%s != null)' % self.expand(first)
        return '(%s != %s)' % (
            self.expand(first), self.expand(second, first.type))

    def comma(self, first, second):
        return '%s + %s' % (self.expand(first), self.expand(second))
