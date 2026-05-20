"""
Representer framework — declarative registration of Python value →
SQL literal encoders.

Decorators:

* ``@for_type("name", encode=False, adapt=True)`` — register the
  primary encoder for a pydal field type. ``encode=True`` means the
  result is already a string literal (no extra quoting); ``adapt=False``
  skips the adapter's quoting wrapper around the result.
* ``@before_type("name")`` — compute per-type context (e.g. SRID for
  PostGIS types) consumed by the matching ``for_type`` handler.
* ``@for_instance(type, repr_type=False)`` — dispatch on the value's
  Python type rather than the field type. Used for ``date`` / ``datetime``
  / ``Decimal`` etc. so unknown column types still serialize sensibly.
* ``@pre(is_breaking=...)`` — short-circuiting pre-pass that runs
  before instance/type dispatch. ``is_breaking=True`` returns
  ``(True, value)`` from the call site immediately.
"""

from collections import defaultdict

from ..helpers._internals import Dispatcher
from ..helpers.regex import REGEX_TYPE
from ..utils import to_unicode

representers: Dispatcher = Dispatcher("representer")


class for_type(object):
    """Decorator: primary representer for a pydal field type."""

    def __init__(self, field_type, encode=False, adapt=True):
        self.field_type = field_type
        self.encode = encode
        self.adapt = adapt

    def __call__(self, f):
        self.f = f
        return self


class before_type(object):
    """
    Decorator: compute per-type kwargs for the matching ``for_type``
    representer (e.g. extract SRID from a ``geometry(...)`` type).
    """

    def __init__(self, field_type):
        self.field_type = field_type

    def __call__(self, f):
        """Stash the decorated function for later metaclass binding."""
        self.f = f
        return self


class for_instance(object):
    """
    Decorator: dispatch on the value's Python type.

    Triggered when the field type doesn't have a registered
    representer; e.g. ``datetime`` falls through here even on tables
    without an explicit datetime column.
    """

    def __init__(self, inst_type, repr_type=False):
        self.inst_type = inst_type
        self.repr_type = repr_type

    def __call__(self, f):
        """Stash the decorated function for later metaclass binding."""
        self.f = f
        return self


class pre(object):
    """
    Decorator: short-circuiting pre-pass run before type/instance dispatch.

    With ``is_breaking=True`` the call site returns immediately on a
    non-None result; with ``is_breaking=False`` it always continues
    (lets you preprocess without ending the pipeline); ``None`` (the
    default) auto-breaks on non-None.
    """

    _inst_count_ = 0

    def __init__(self, is_breaking=None):
        self.breaking = is_breaking
        self._inst_count_ = pre._inst_count_
        pre._inst_count_ += 1

    def __call__(self, f):
        self.f = f
        return self


class MetaRepresenter(type):
    def __new__(cls, name, bases, attrs):
        new_class = type.__new__(cls, name, bases, attrs)
        if bases == (object,):
            return new_class
        #: collect declared attributes
        trepresenters = {}
        irepresenters = {}
        tbefore = {}
        pres = {}
        for key, value in list(attrs.items()):
            if isinstance(value, for_type):
                trepresenters[key] = value
            elif isinstance(value, before_type):
                tbefore[key] = value
            elif isinstance(value, for_instance):
                irepresenters[key] = value
            elif isinstance(value, pre):
                pres[key] = value
        #: get super declared attributes
        declared_trepresenters = {}
        declared_irepresenters = {}
        declared_tbefore = {}
        declared_pres = {}
        for base in reversed(new_class.__mro__[1:]):
            if hasattr(base, "_declared_trepresenters_"):
                declared_trepresenters.update(base._declared_trepresenters_)
            if hasattr(base, "_declared_irepresenters_"):
                declared_irepresenters.update(base._declared_irepresenters_)
            if hasattr(base, "_declared_tbefore_"):
                declared_tbefore.update(base._declared_tbefore_)
            if hasattr(base, "_declared_pres_"):
                declared_pres.update(base._declared_pres_)
        #: set trepresenters
        declared_trepresenters.update(trepresenters)
        declared_irepresenters.update(irepresenters)
        declared_tbefore.update(tbefore)
        declared_pres.update(pres)
        new_class._declared_trepresenters_ = declared_trepresenters
        new_class._declared_irepresenters_ = declared_irepresenters
        new_class._declared_tbefore_ = declared_tbefore
        new_class._declared_pres_ = declared_pres
        return new_class


class TReprMethodWrapper(object):
    def __init__(self, representer, obj, extra=None):
        self.representer = representer
        self.obj = obj
        if extra:
            self.extra = extra
            self.call = self._call_with_extras
        else:
            self.call = self._call
        self.inner_call = self._inner_call
        if self.obj.adapt:
            self.adapt = self._adapt
        else:
            self.adapt = self._no_adapt

    def _adapt(self, value):
        return self.representer.adapt(value)

    def _no_adapt(self, value):
        return value

    def _inner_call(self, value, **kwargs):
        return self.obj.f(self.representer, value, **kwargs)

    def _call_with_extras(self, value, field_type):
        extras = self.extra(self.representer, field_type)
        return self.inner_call(value, **extras)

    def _call(self, value, field_type):
        return self.inner_call(value)

    def __call__(self, value, field_type):
        return self.adapt(self.call(value, field_type))


class IReprMethodWrapper(object):
    def __init__(self, representer, obj):
        self.representer = representer
        self.obj = obj

    def __call__(self, value, field_type):
        rv = self.obj.f(self.representer, value, field_type)
        return self.obj.repr_type, rv


class PreMethodWrapper(object):
    def __init__(self, representer, obj):
        self.representer = representer
        self.obj = obj
        if self.obj.breaking is None:
            self.call = self._call_autobreak
        elif self.obj.breaking == True:
            self.call = self._call_break
        else:
            self.call = self._call_nobreak

    def _call_autobreak(self, value, field_type):
        rv = self.obj.f(self.representer, value, field_type)
        if rv is not None:
            return True, rv
        return False, value

    def _call_break(self, value, field_type):
        return self.obj.f(self.representer, value, field_type)

    def _call_nobreak(self, value, field_type):
        return False, self.obj.f(self.representer, value, field_type)

    def __call__(self, value, field_type):
        return self.call(value, field_type)


class Representer(metaclass=MetaRepresenter):
    def __init__(self, adapter):
        self.adapter = adapter
        self.dialect = adapter.dialect
        self._tbefore_registry_ = {}
        for name, obj in self._declared_tbefore_.items():
            self._tbefore_registry_[obj.field_type] = obj.f
        self.registered_t = defaultdict(lambda self=self: self._default)
        for name, obj in self._declared_trepresenters_.items():
            if obj.field_type in self._tbefore_registry_:
                self.registered_t[obj.field_type] = TReprMethodWrapper(
                    self, obj, self._tbefore_registry_[obj.field_type]
                )
            else:
                self.registered_t[obj.field_type] = TReprMethodWrapper(self, obj)
        self.registered_i = {}
        for name, obj in self._declared_irepresenters_.items():
            self.registered_i[obj.inst_type] = IReprMethodWrapper(self, obj)
        self._pre_registry_ = []
        pres = []
        for name, obj in self._declared_pres_.items():
            pres.append(obj)
        pres.sort(key=lambda x: x._inst_count_)
        for pre in pres:
            self._pre_registry_.append(PreMethodWrapper(self, pre))

    def _default(self, value, field_type):
        return self.adapt(value)

    def _default_instance(self, value, field_type):
        return True, value

    def get_representer_for_instance(self, value):
        for inst, representer in self.registered_i.items():
            if isinstance(value, inst):
                return representer
        return self._default_instance

    def get_representer_for_type(self, field_type):
        key = REGEX_TYPE.match(field_type).group(0)
        return self.registered_t[key]

    def adapt(self, value):
        value = to_unicode(value)
        return self.adapter.adapt(value)

    def exceptions(self, value, field_type):
        return None

    def represent(self, value, field_type):
        pre_end = False
        for pre in self._pre_registry_:
            pre_end, value = pre(value, field_type)
            if pre_end:
                break
        if pre_end:
            return value
        repr_type, rv = self.get_representer_for_instance(value)(value, field_type)
        if repr_type:
            rv = self.get_representer_for_type(field_type)(rv, field_type)
        return rv


from .base import BaseRepresenter, NoSQLRepresenter, SQLRepresenter
from .couchdb import CouchDBRepresenter
from .db2 import DB2Representer
from .google import FirestoreRepresenter
from .informix import InformixRepresenter
from .mongo import MongoRepresenter
from .mssql import MSSQLRepresenter
from .mysql import MySQLRepresenter
from .oracle import OracleRepresenter
from .postgre import PostgreRepresenter
from .sqlite import SpatialiteRepresenter, SQLiteRepresenter
