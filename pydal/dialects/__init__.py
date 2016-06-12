from .._compat import with_metaclass, iteritems
from .._gae import gae
from .._load import OrderedDict
from ..helpers._internals import Dispatcher
from ..objects import Expression


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


class register_expression(object):
    _inst_count_ = 0

    def __init__(self, name):
        self.name = name
        self._inst_count_ = register_expression._inst_count_
        register_expression._inst_count_ += 1

    def __call__(self, f):
        self.f = f
        return self


class ExpressionMethodWrapper(object):
    def __init__(self, dialect, obj):
        self.dialect = dialect
        self.obj = obj

    def __call__(self, expression, *args, **kwargs):
        return self.obj.f(self.dialect, expression, *args, **kwargs)


class MetaDialect(type):
    def __new__(cls, name, bases, attrs):
        new_class = type.__new__(cls, name, bases, attrs)
        if bases == (object,):
            return new_class
        #: collect declared attributes
        sqltypes = []
        expressions = []
        for key, value in list(attrs.items()):
            if isinstance(value, sqltype_for):
                sqltypes.append((key, value))
            if isinstance(value, register_expression):
                expressions.append((key, value))
        sqltypes.sort(key=lambda x: x[1]._inst_count_)
        expressions.sort(key=lambda x: x[1]._inst_count_)
        declared_sqltypes = OrderedDict()
        declared_expressions = OrderedDict()
        for key, val in sqltypes:
            declared_sqltypes[key] = val
        new_class._declared_sqltypes_ = declared_sqltypes
        for key, val in expressions:
            declared_expressions[key] = val
        new_class._declared_expressions_ = declared_expressions
        #: get super declared attributes
        all_sqltypes = OrderedDict()
        all_expressions = OrderedDict()
        for base in reversed(new_class.__mro__[1:]):
            if hasattr(base, '_declared_sqltypes_'):
                all_sqltypes.update(base._declared_sqltypes_)
            if hasattr(base, '_declared_expressions_'):
                all_expressions.update(base._declared_expressions_)
        #: set re-constructed attributes
        all_sqltypes.update(declared_sqltypes)
        all_expressions.update(declared_expressions)
        new_class._all_sqltypes_ = all_sqltypes
        new_class._all_expressions_ = all_expressions
        return new_class


class Dialect(with_metaclass(MetaDialect)):
    def __init__(self, adapter):
        self.adapter = adapter
        self.types = {}
        for name, obj in iteritems(self._all_sqltypes_):
            self.types[obj.key] = obj.f(self)
        for name, obj in iteritems(self._all_expressions_):
            Expression._dialect_expressions_[obj.name] = \
                ExpressionMethodWrapper(self, obj)

    def expand(self, *args, **kwargs):
        return self.adapter.expand(*args, **kwargs)


from .base import SQLDialect
from .sqlite import SQLiteDialect, SpatialiteDialect
from .postgre import PostgreDialect
from .mysql import MySQLDialect
from .mssql import MSSQLDialect
from .mongo import MongoDialect
from .db2 import DB2Dialect
from .firebird import FireBirdDialect
from .informix import InformixDialect
from .ingres import IngresDialect
from .oracle import OracleDialect
from .sap import SAPDBDialect
from .teradata import TeradataDialect
from .couchdb import CouchDBDialect

if gae is not None:
    from .google import GoogleDatastoreDialect
