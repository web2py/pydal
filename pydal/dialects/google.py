from ..adapters.google import Firestore
from . import dialects, sqltype_for
from .base import NoSQLDialect

try:
    from google.cloud import firestore
    from google.cloud.firestore_v1.base_query import (
        BaseCompositeFilter,
        FieldFilter,
        Or,
    )
except ImportError:
    pass


@dialects.register_for(Firestore)
class FirestoreDialect(NoSQLDialect):
    def _and(self, first, second, query_env={}):
        a = self.expand(first, query_env=query_env)
        b = self.expand(second, query_env=query_env)
        filters = (
            a.filters
            if isinstance(a, BaseCompositeFilter) and a.operator == "AND"
            else [a]
        )
        filters += (
            b.filters
            if isinstance(b, BaseCompositeFilter) and a.operator == "AND"
            else [b]
        )
        return BaseCompositeFilter("AND", filters)

    def _or(self, first, second, query_env={}):
        a = self.expand(first, query_env=query_env)
        b = self.expand(second, query_env=query_env)
        filters = (
            a.filters
            if isinstance(a, BaseCompositeFilter) and a.operator == "OR"
            else [a]
        )
        filters += (
            b.filters
            if isinstance(b, BaseCompositeFilter) and a.operator == "OR"
            else [b]
        )
        return BaseCompositeFilter("OR", filters)

    def eq(self, first, second=None, query_env={}):
        return FieldFilter(
            first.name, "==", self.expand(second, first.type, query_env=query_env)
        )

    def ne(self, first, second=None, query_env={}):
        return FieldFilter(
            first.name, "!=", self.expand(second, first.type, query_env=query_env)
        )

    def lt(self, first, second=None, query_env={}):
        return FieldFilter(
            first.name, "<", self.expand(second, first.type, query_env=query_env)
        )

    def lte(self, first, second=None, query_env={}):
        return FieldFilter(
            first.name, "<=", self.expand(second, first.type, query_env=query_env)
        )

    def gt(self, first, second=None, query_env={}):
        return FieldFilter(
            first.name, ">", self.expand(second, first.type, query_env=query_env)
        )

    def gte(self, first, second=None, query_env={}):
        return FieldFilter(
            first.name, ">=", self.expand(second, first.type, query_env=query_env)
        )

    def invert(self, first, query_env={}):
        raise NotImplementedError

    def comma(self, first, second, query_env={}):
        raise NotImplementedError

    def belongs(self, first, second, query_env={}):
        return FieldFilter(first.name, "in", second)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        raise NotImplementedError

    def _not(self, val, query_env={}):
        op, f, s = val.op, val.first, val.second
        if op in [self._or, self._and]:
            not_op = self._and if op == self._or else self._or
            rv = not_op(self._not(f), self._not(s))
        elif op == self.eq:
            rv = FieldFilter(f.name, "!=", s)
        elif op == self.ne:
            rv = FieldFilter(f.name, "==", s)
        elif op == self.lt:
            rv = FieldFilter(f.name, ">=", s)
        elif op == self.lte:
            rv = FieldFilter(f.name, ">", s)
        elif op == self.gt:
            rv = FieldFilter(f.name, "<=", s)
        elif op == self.gte:
            rv = FieldFilter(f.name, "<", s)
        elif op == self.belongs:
            rv = FieldFilter(f.name, "not-in", s)
        else:
            # TODO the IN operator must be split into a sequence of
            # (field!=value) AND (field!=value) AND ...
            raise NotImplementedError
        return rv
