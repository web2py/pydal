from .._compat import with_metaclass, iteritems
from ..helpers._internals import Dispatcher


dialects = Dispatcher("dialect")


class sqltype_for(object):
    _inst_count_ = 0

    def __init__(self, key):
        self.key = key
        self._inst_count_ = sqltype_for._inst_count_
        sqltype_for._inst_count_ += 1

    def __call__(self, f):
        self.f = f
        return self


class MetaDialect(type):
    def __new__(cls, name, bases, attrs):
        new_class = type.__new__(cls, name, bases, attrs)
        if bases == (object,):
            return new_class
        #: collect declared attributes
        sqltypes = {}
        for key, value in list(attrs.items()):
            if isinstance(value, sqltype_for):
                sqltypes[key] = value
        #: get super declared attributes
        declared_sqltypes = {}
        for base in reversed(new_class.__mro__[1:]):
            if hasattr(base, '_declared_sqltypes_'):
                declared_sqltypes.update(base._declared_sqltypes_)
        #: set sqltypes
        declared_sqltypes.update(sqltypes)
        new_class._declared_sqltypes_ = declared_sqltypes
        return new_class


class Dialect(with_metaclass(MetaDialect)):
    def __init__(self, adapter):
        self.adapter = adapter
        sorted_types = []
        for name, obj in iteritems(self._declared_sqltypes_):
            sorted_types.append(obj)
        sorted_types.sort(key=lambda x: x._inst_count_)
        self.types = {}
        for obj in sorted_types:
            self.types[obj.key] = obj.f(self)

    def expand(self, *args, **kwargs):
        return self.adapter.expand(*args, **kwargs)


from .base import SQLDialect
from .sqlite import SQLiteDialect, SpatialiteDialect
