from .._compat import with_metaclass, iteritems
from .._load import OrderedDict
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
        sqltypes = []
        for key, value in list(attrs.items()):
            if isinstance(value, sqltype_for):
                #sqltypes[key] = value
                sqltypes.append((key, value))
        sqltypes.sort(key=lambda x: x[1]._inst_count_)
        declared_sqltypes = OrderedDict()
        for key, val in sqltypes:
            declared_sqltypes[key] = val
        new_class._declared_sqltypes_ = declared_sqltypes
        #: get super declared attributes
        all_sqltypes = OrderedDict()
        for base in reversed(new_class.__mro__[1:]):
            if hasattr(base, '_declared_sqltypes_'):
                all_sqltypes.update(base._declared_sqltypes_)
        #: set sqltypes
        all_sqltypes.update(declared_sqltypes)
        new_class._all_sqltypes_ = all_sqltypes
        return new_class


class Dialect(with_metaclass(MetaDialect)):
    def __init__(self, adapter):
        self.adapter = adapter
        self.types = {}
        for name, obj in iteritems(self._all_sqltypes_):
            self.types[obj.key] = obj.f(self)

    def expand(self, *args, **kwargs):
        return self.adapter.expand(*args, **kwargs)


from .base import SQLDialect
from .sqlite import SQLiteDialect, SpatialiteDialect
from .postgre import PostgreDialect
from .mysql import MySQLDialect
from .mssql import MSSQLDialect
