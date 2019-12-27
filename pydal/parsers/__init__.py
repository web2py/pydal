from collections import defaultdict
from .._compat import with_metaclass, iteritems
from .._gae import gae
from ..helpers._internals import Dispatcher


parsers = Dispatcher("parser")


class for_type(object):
    def __init__(self, field_type):
        self.field_type = field_type

    def __call__(self, f):
        self.f = f
        return self


class before_parse(object):
    def __init__(self, field_type):
        self.field_type = field_type

    def __call__(self, f):
        self.f = f
        return self


class MetaParser(type):
    def __new__(cls, name, bases, attrs):
        new_class = type.__new__(cls, name, bases, attrs)
        if bases == (object,):
            return new_class
        #: collect declared attributes
        parsers = {}
        before = {}
        for key, value in list(attrs.items()):
            if isinstance(value, for_type):
                parsers[key] = value
            elif isinstance(value, before_parse):
                before[key] = value
        #: get super declared attributes
        declared_parsers = {}
        declared_before = {}
        for base in reversed(new_class.__mro__[1:]):
            if hasattr(base, "_declared_parsers_"):
                declared_parsers.update(base._declared_parsers_)
            if hasattr(base, "_declared_before_"):
                declared_before.update(base._declared_before_)
        #: set parsers
        declared_parsers.update(parsers)
        declared_before.update(before)
        new_class._declared_parsers_ = declared_parsers
        new_class._declared_before_ = declared_before
        return new_class


class ParserMethodWrapper(object):
    def __init__(self, parser, f, extra=None):
        self.parser = parser
        self.f = f
        if extra:
            self.extra = extra
            self.call = self._call_with_extras
        else:
            self.call = self._call

    def _call_with_extras(self, value, field_type):
        extras = self.extra(self.parser, field_type)
        return self.f(self.parser, value, **extras)

    def _call(self, value, field_type):
        return self.f(self.parser, value)

    def __call__(self, value, field_type):
        return self.call(value, field_type)


class Parser(with_metaclass(MetaParser)):
    def __init__(self, adapter):
        self.adapter = adapter
        self.dialect = adapter.dialect
        self._before_registry_ = {}
        for name, obj in iteritems(self._declared_before_):
            self._before_registry_[obj.field_type] = obj.f
        self.registered = defaultdict(lambda self=self: self._default)
        for name, obj in iteritems(self._declared_parsers_):
            if obj.field_type in self._before_registry_:
                self.registered[obj.field_type] = ParserMethodWrapper(
                    self, obj.f, self._before_registry_[obj.field_type]
                )
            else:
                self.registered[obj.field_type] = ParserMethodWrapper(self, obj.f)

    def _default(self, value, field_type):
        return value

    def parse(self, value, field_itype, field_type):
        return self.registered[field_itype](value, field_type)


from .base import BasicParser
from .sqlite import SQLiteParser
from .postgre import PostgreParser
from .mongo import MongoParser

if gae is not None:
    from .google import GoogleDatastoreParser
