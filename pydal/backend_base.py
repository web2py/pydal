"""
Unified backend framework for pydal.

This module merges what used to live in four sibling packages:

* ``adapters``   — connection-owning classes (``BaseAdapter``, ``SQLAdapter``,
  ``NoSQLAdapter``, ``NullAdapter``) and the URI-keyed ``adapters`` dispatcher.
* ``dialects``   — operator-rendering classes (``CommonDialect``, ``SQLDialect``,
  ``NoSQLDialect``) and the MRO-keyed ``dialects`` dispatcher.
* ``parsers``    — driver-value-to-Python decoders (``BasicParser``, ``JSONParser``,
  ``ListsParser``, ...) and the ``parsers`` dispatcher.
* ``representers`` — Python-value-to-SQL encoders (``BaseRepresenter``,
  ``SQLRepresenter``, ``JSONRepresenter``, ``NoSQLRepresenter``) and the
  ``representers`` dispatcher.

Decorator naming: parsers expose ``for_type`` / ``before_parse``; representers
expose ``repr_for_type`` / ``before_type`` / ``for_instance`` / ``pre`` (the
``for_type`` rename avoids a name collision with the parsers' decorator);
dialects expose ``sqltype_for`` / ``register_expression``.

Per-backend code lives in ``pydal.backends.<name>``; each backend file imports
from this module and registers its concrete classes with the four dispatchers.
"""

import json
import re
import sys
import types
from base64 import b64decode, b64encode
from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from ._globals import IDENTITY
from .connection import ConnectionPool
from .exceptions import NotOnNOSQLError
from .helpers._internals import Dispatcher
from .helpers.classes import (
    SQLALL,
    ExecutionHandler,
    NullDriver,
    Reference,
    SQLCustomType,
)
from .helpers.methods import (
    bar_decode_integer,
    bar_decode_string,
    bar_encode,
    merge_tablemaps,
    use_common_filters,
    xorify,
)
from .helpers.regex import REGEX_SELECT_AS_PARSER, REGEX_TABLE_DOT_FIELD, REGEX_TYPE
from .helpers.serializers import serializers
from .migrator import Migrator
from .objects import (
    Expression,
    Field,
    IterRows,
    LazyReferenceGetter,
    LazySet,
    Query,
    Rows,
    Row,
    Select,
    Table,
    VirtualCommand,
)
from .utils import deprecated, hashlib_md5, to_bytes, to_native, to_unicode


NoneType = type(None)


CALLABLETYPES = (
    types.LambdaType,
    types.FunctionType,
    types.BuiltinFunctionType,
    types.MethodType,
    types.BuiltinMethodType,
)


# ============================================================
# Adapter dispatcher, meta, and connection decorators
# ============================================================

class Adapters(Dispatcher):
    """
    URI-keyed registry of adapter classes.

    Unlike the generic ``Dispatcher``, registration is by URI string
    (``"postgres"``, ``"postgres:psycopg2"``, ``"sqlite:memory"``,
    ...), and ``get_for`` takes the URI prefix rather than walking
    the class MRO.
    """

    def register_for(self, *uris):
        """Decorator: register a class for one or more URI prefixes."""

        def wrap(dispatch_class):
            for uri in uris:
                self._registry_[uri] = dispatch_class
            return dispatch_class

        return wrap

    def get_for(self, uri):
        """Look up an adapter class by URI prefix; raises ``SyntaxError`` if missing."""
        try:
            return self._registry_[uri]
        except KeyError:
            raise SyntaxError("Adapter not found for %s" % uri)


adapters: "Adapters" = Adapters("adapters")


class AdapterMeta(type):
    """
    Metaclass that intercepts adapter-level kwargs at construction.

    Currently picks up ``entity_quoting`` (disables identifier quoting
    when False) and ``uploads_in_blob`` (forces all uploads into the
    DB as BLOBs). Both are removed from kwargs before they reach the
    adapter's ``__init__``.
    """

    def __call__(cls, *args, **kwargs):
        uploads_in_blob = kwargs.get("adapter_args", {}).get(
            "uploads_in_blob", cls.uploads_in_blob
        )
        cls.uploads_in_blob = uploads_in_blob

        entity_quoting = kwargs.get("entity_quoting", True)
        if "entity_quoting" in kwargs:
            del kwargs["entity_quoting"]

        obj = super(AdapterMeta, cls).__call__(*args, **kwargs)

        regex_ent = r"(\w+)"
        if not entity_quoting:
            obj.dialect.quote_template = "%s"
        else:
            regex_ent = obj.dialect.quote_template % regex_ent
        # FIXME: this regex should NOT be compiled
        obj.REGEX_TABLE_DOT_FIELD = re.compile(r"^%s\.%s$" % (regex_ent, regex_ent))

        return obj


def with_connection(f):
    """
    Decorator: only run ``f`` when the adapter has a live connection.

    Returns ``None`` when there's no connection — used for cleanup
    paths that should silently no-op on a closed DAL.
    """

    def wrap(*args, **kwargs):
        if args[0].connection:
            return f(*args, **kwargs)
        return None

    return wrap


def with_connection_or_raise(f):
    """
    Decorator: raise instead of silently no-op'ing when no connection.

    The error is ``ValueError(args[1])`` when an extra positional arg
    is present (so callers can surface the failed statement), otherwise
    a generic ``RuntimeError``.
    """

    def wrap(*args, **kwargs):
        if not args[0].connection:
            if len(args) > 1:
                raise ValueError(args[1])
            raise RuntimeError("no connection available")
        return f(*args, **kwargs)

    return wrap

# ============================================================
# Dialect dispatcher, decorators, and metaclass
# ============================================================

dialects: Dispatcher = Dispatcher("dialect")


class sqltype_for(object):
    """Decorator: register a method as the SQL-type renderer for ``key``."""

    _inst_count_ = 0

    def __init__(self, key):
        self.key = key
        self._inst_count_ = sqltype_for._inst_count_
        sqltype_for._inst_count_ += 1

    def __call__(self, f):
        self.f = f
        return self


class register_expression(object):
    """
    Decorator: register a dialect method as a callable on every
    ``Expression``. The method appears as ``expr.<name>(args)``.
    """

    _inst_count_ = 0

    def __init__(self, name):
        self.name = name
        self._inst_count_ = register_expression._inst_count_
        register_expression._inst_count_ += 1

    def __call__(self, f):
        """Stash the decorated function for later metaclass binding."""
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
            if hasattr(base, "_declared_sqltypes_"):
                all_sqltypes.update(base._declared_sqltypes_)
            if hasattr(base, "_declared_expressions_"):
                all_expressions.update(base._declared_expressions_)
        #: set re-constructed attributes
        all_sqltypes.update(declared_sqltypes)
        all_expressions.update(declared_expressions)
        new_class._all_sqltypes_ = all_sqltypes
        new_class._all_expressions_ = all_expressions
        return new_class


class Dialect(metaclass=MetaDialect):
    def __init__(self, adapter):
        self.adapter = adapter
        self.types = {}
        for name, obj in self._all_sqltypes_.items():
            self.types[obj.key] = obj.f(self)
        for name, obj in self._all_expressions_.items():
            Expression._dialect_expressions_[obj.name] = ExpressionMethodWrapper(
                self, obj
            )

    def expand(self, *args, **kwargs):
        return self.adapter.expand(*args, **kwargs)

# ============================================================
# Parser dispatcher, decorators, and metaclass
# ============================================================

parsers: Dispatcher = Dispatcher("parser")


class for_type(object):
    """Decorator: register a method as the parser for ``field_type``."""

    def __init__(self, field_type):
        self.field_type = field_type

    def __call__(self, f):
        """Stash the decorated function for later metaclass binding."""
        self.f = f
        return self


class before_parse(object):
    """
    Decorator: register a method that returns extra kwargs for the
    main ``for_type`` parser of the same ``field_type``.

    Used e.g. by SQLite's decimal parser to extract the precision out
    of the column type string before the decoder runs.
    """

    def __init__(self, field_type):
        self.field_type = field_type

    def __call__(self, f):
        """Stash the decorated function for later metaclass binding."""
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


class Parser(metaclass=MetaParser):
    def __init__(self, adapter):
        self.adapter = adapter
        self.dialect = adapter.dialect
        self._before_registry_ = {}
        for name, obj in self._declared_before_.items():
            self._before_registry_[obj.field_type] = obj.f
        self.registered = defaultdict(lambda self=self: self._default)
        for name, obj in self._declared_parsers_.items():
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

# ============================================================
# Representer dispatcher, decorators, and metaclass
# (the parsers' ``for_type`` decorator is renamed to
# ``repr_for_type`` here to avoid a name collision.)
# ============================================================

representers: Dispatcher = Dispatcher("representer")


class repr_for_type(object):
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
    Decorator: compute per-type kwargs for the matching ``repr_for_type``
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
            if isinstance(value, repr_for_type):
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

# ============================================================
# Adapter base classes: BaseAdapter / SQLAdapter / NoSQLAdapter / NullAdapter
# ============================================================

class BaseAdapter(ConnectionPool, metaclass=AdapterMeta):
    """
    Common base for every adapter.

    Subclasses must override ``dbengine`` (string identifier),
    ``drivers`` (tuple of preferred driver module names in priority
    order), and ``connector`` (returns a fresh DB-API connection).

    Many class-level booleans (``uploads_in_blob``,
    ``support_distributed_transaction``, ``commit_on_alter_table``,
    ``can_select_for_update``, ...) parameterize feature support so
    higher layers can branch politely on backend capabilities.
    """

    dbengine = "None"
    drivers = ()
    uploads_in_blob = False
    support_distributed_transaction = False

    def __init__(
        self,
        db,
        uri,
        pool_size=0,
        folder=None,
        db_codec="UTF-8",
        credential_decoder=IDENTITY,
        driver_args={},
        adapter_args={},
        after_connection=None,
        entity_quoting=False,
    ):
        super(BaseAdapter, self).__init__()
        self._load_dependencies()
        self.db = db
        self.uri = uri
        self.pool_size = pool_size
        self.folder = folder
        self.db_codec = db_codec
        self.credential_decoder = credential_decoder
        self.driver_args = driver_args
        self.adapter_args = adapter_args
        self.expand = self._expand
        self._after_connection = after_connection
        self.set_connection(None)
        self.find_driver()
        self._initialize_()

    def _load_dependencies(self):
        from .driver import Driver

        self.dialect = dialects.get_for(self)
        self.parser = parsers.get_for(self)
        self.representer = representers.get_for(self)
        # Layer 4: a thin Driver owns cursor.execute + transactions.
        # SQLAdapter delegates its execute/commit/rollback/lastrowid
        # methods to it; everything above doesn't need to change.
        self.driver_io = Driver(self)
        # The AST compiler is optional: BaseAdapter has no SQL backing,
        # and not every SQL adapter has a registered compiler yet. The
        # five Set/Table entry points check for None and fall back to
        # the legacy dialect path when missing.
        try:
            from .compilers import compilers
            self.compiler = compilers.get_for(self)
        except (ImportError, ValueError):
            self.compiler = None

    def _initialize_(self):
        self._find_work_folder()

    @property
    def types(self):
        return self.dialect.types

    @property
    def _available_drivers(self):
        return [
            driver
            for driver in self.drivers
            if driver in self.db._drivers_available
        ]

    def _driver_from_uri(self):
        rv = None
        if self.uri:
            items = self.uri.split("://", 1)[0].split(":")
            rv = items[1] if len(items) > 1 else None
        return rv

    def find_driver(self):
        if getattr(self, "driver", None) is not None:
            return
        requested_driver = self._driver_from_uri() or self.adapter_args.get("driver")
        if requested_driver:
            if requested_driver in self._available_drivers:
                self.driver_name = requested_driver
                self.driver = self.db._drivers_available[requested_driver]
            else:
                raise RuntimeError("Driver %s is not available" % requested_driver)
        elif self._available_drivers:
            self.driver_name = self._available_drivers[0]
            self.driver = self.db._drivers_available[self.driver_name]
        else:
            raise RuntimeError(
                "No driver of supported ones %s is available" % str(self.drivers)
            )

    def connector(self):
        return self.driver.connect(self.driver_args)

    def test_connection(self):
        pass

    @with_connection
    def close_connection(self):
        rv = self.connection.close()
        self.set_connection(None)
        return rv

    def tables(self, *queries):
        tables = dict()
        for query in queries:
            if isinstance(query, Field):
                key = query.tablename
                if tables.get(key, query.table) is not query.table:
                    raise ValueError("Name conflict in table list: %s" % key)
                tables[key] = query.table
            elif isinstance(query, (Expression, Query)):
                tmp = [x for x in (query.first, query.second) if x is not None]
                tables = merge_tablemaps(tables, self.tables(*tmp))
        return tables

    def get_table(self, *queries):
        tablemap = self.tables(*queries)
        if len(tablemap) == 1:
            return tablemap.popitem()[1]
        elif len(tablemap) < 1:
            raise RuntimeError("No table selected")
        else:
            raise RuntimeError("Too many tables selected (%s)" % str(list(tablemap)))

    def common_filter(self, query, tablist):
        tenant_fieldname = self.db._request_tenant
        for table in tablist:
            if isinstance(table, str):
                table = self.db[table]
            # deal with user provided filters
            if table._common_filter is not None:
                query = query & table._common_filter(query)
            # deal with multi_tenant filters
            if tenant_fieldname in table:
                default = table[tenant_fieldname].default
                if default is not None:
                    newquery = table[tenant_fieldname] == default
                    if query is None:
                        query = newquery
                    else:
                        query = query & newquery
        return query

    def _expand(self, expression, field_type=None, colnames=False, query_env={}):
        return str(expression)

    def expand_all(self, fields, tabledict):
        new_fields = []
        append = new_fields.append
        for item in fields:
            if isinstance(item, SQLALL):
                new_fields += item._table
            elif isinstance(item, str):
                m = REGEX_TABLE_DOT_FIELD.match(item)
                if m:
                    tablename, fieldname = m.groups()
                    append(self.db[tablename][fieldname])
                else:
                    append(Expression(self.db, lambda item=item: item))
            else:
                append(item)
        # ## if no fields specified take them all from the requested tables
        if not new_fields:
            for table in tabledict.values():
                for field in table:
                    append(field)
        return new_fields

    def parse_value(self, value, field_itype, field_type, blob_decode=True):
        # [Note - gi0baro] I think next if block can be (should be?) avoided
        if field_type != "blob" and isinstance(value, str):
            try:
                value = value.decode(self.db._db_codec)
            except Exception:
                pass
        if isinstance(field_type, SQLCustomType):
            value = field_type.decoder(value)
        if not isinstance(field_type, str) or value is None:
            return value
        elif field_type == "blob" and not blob_decode:
            return value
        else:
            return self.parser.parse(value, field_itype, field_type)

    def _add_operators_to_parsed_row(self, rid, table, row):
        for key, record_operator in self.db.record_operators.items():
            setattr(row, key, record_operator(row, table, rid))
        if table._db._lazy_tables:
            row["__get_lazy_reference__"] = LazyReferenceGetter(table, rid)

    def _add_reference_sets_to_parsed_row(self, rid, table, tablename, row):
        for rfield in table._referenced_by:
            referee_link = self.db._referee_name and self.db._referee_name % dict(
                table=rfield.tablename, field=rfield.name
            )
            if referee_link and referee_link not in row and referee_link != tablename:
                row[referee_link] = LazySet(rfield, rid)

    def _regex_select_as_parser(self, colname):
        return re.search(REGEX_SELECT_AS_PARSER, colname)

    def _parse(
        self,
        row,
        tmps,
        fields,
        colnames,
        blob_decode,
        cacheable,
        fields_virtual,
        fields_lazy,
    ):
        new_row = defaultdict(self.db.Row)
        extras = self.db.Row()
        #: let's loop over columns
        for j, colname in enumerate(colnames):
            value = row[j]
            tmp = tmps[j]
            tablename = None
            #: do we have a real column?
            if tmp:
                (tablename, fieldname, table, field, ft, fit) = tmp
                colset = new_row[tablename]
                #: parse value
                value = self.parse_value(value, fit, ft, blob_decode)
                if field.filter_out:
                    value = field.filter_out(value)
                colset[fieldname] = value
                #! backward compatibility
                if ft == "id" and fieldname != "id" and "id" not in table.fields:
                    colset["id"] = value
                #: additional parsing for 'id' fields
                if ft == "id" and not cacheable:
                    self._add_operators_to_parsed_row(value, table, colset)
                    #: table may be 'nested_select' which doesn't have '_referenced_by'
                    if hasattr(table, "_referenced_by"):
                        self._add_reference_sets_to_parsed_row(
                            value, table, tablename, colset
                        )
            #: otherwise we set the value in extras
            else:
                #: fields[j] may be None if only 'colnames' was specified in db.executesql()
                field = fields[j]
                f_itype, ftype = field and [field._itype, field.type] or [None, None]
                value = self.parse_value(value, f_itype, ftype, blob_decode)
                # for aliased fields use the aliased name
                if isinstance(field, Expression) and field.op == self.dialect._as:
                    colname = field.second
                    # if the alias is a tablename.fieldname add the column to the table
                    if field.tablename:
                        if field.tablename not in new_row:
                            new_row[field.tablename] = self.db.Row()
                        new_row[field.tablename][colname] = value
                        continue
                extras[colname] = value
                if not fields[j]:
                    new_row[colname] = value
                else:
                    new_column_match = self._regex_select_as_parser(colname)
                    if new_column_match is not None:
                        new_column_name = new_column_match.group(1)
                        new_row[new_column_name] = value
        #: add extra if not empty
        if extras:
            new_row["_extra"] = extras
        #: add virtuals
        new_row = self.db.Row(**new_row)
        for tablename in fields_virtual.keys():
            for f, v in fields_virtual[tablename][1]:
                try:
                    new_row[tablename][f] = v.f(new_row)
                except (AttributeError, KeyError):
                    pass  # not enough fields to define virtual field
            for f, v in fields_lazy[tablename][1]:
                try:
                    new_row[tablename][f] = v.handler(v.f, new_row)
                except (AttributeError, KeyError):
                    pass  # not enough fields to define virtual field
        return new_row

    def _parse_expand_colnames(self, fieldlist):
        """
        - Expand a list of colnames into a list of
          (tablename, fieldname, table_obj, field_obj, field_type)
        - Create a list of table for virtual/lazy fields
        """
        fields_virtual = {}
        fields_lazy = {}
        tmps = []
        for field in fieldlist:
            if not isinstance(field, Field):
                tmps.append(None)
                continue
            table = field.table
            tablename, fieldname = table._tablename, field.name
            ft = field.type
            fit = field._itype
            tmps.append((tablename, fieldname, table, field, ft, fit))
            if tablename not in fields_virtual:
                fields_virtual[tablename] = (
                    table,
                    [(f.name, f) for f in table._virtual_fields],
                )
                fields_lazy[tablename] = (
                    table,
                    [(f.name, f) for f in table._virtual_methods],
                )
        return (fields_virtual, fields_lazy, tmps)

    def parse(self, rows, fields, colnames, blob_decode=True, cacheable=False):
        (fields_virtual, fields_lazy, tmps) = self._parse_expand_colnames(fields)
        new_rows = [
            self._parse(
                row,
                tmps,
                fields,
                colnames,
                blob_decode,
                cacheable,
                fields_virtual,
                fields_lazy,
            )
            for row in rows
        ]
        rowsobj = self.db.Rows(self.db, new_rows, colnames, rawrows=rows, fields=fields)
        # Old style virtual fields
        for tablename, tmp in fields_virtual.items():
            table = tmp[0]
            # ## old style virtual fields
            for item in table.virtualfields:
                try:
                    rowsobj = rowsobj.setvirtualfields(**{tablename: item})
                except (KeyError, AttributeError):
                    # to avoid breaking virtualfields when partial select
                    pass
        return rowsobj

    def iterparse(self, sql, fields, colnames, blob_decode=True, cacheable=False):
        """
        Iterator to parse one row at a time.
        It doesn't support the old style virtual fields
        """
        return IterRows(self.db, sql, fields, colnames, blob_decode, cacheable)

    def adapt(self, value):
        return value

    def represent(self, obj, field_type):
        if isinstance(obj, CALLABLETYPES):
            obj = obj()
        return self.representer.represent(obj, field_type)

    def _drop_table_cleanup(self, table):
        del self.db[table._tablename]
        del self.db.tables[self.db.tables.index(table._tablename)]
        self.db._remove_references_to(table)

    def drop_table(self, table, mode=""):
        self._drop_table_cleanup(table)

    def rowslice(self, rows, minimum=0, maximum=None):
        return rows

    def sqlsafe_table(self, tablename, original_tablename=None):
        return tablename

    def sqlsafe_field(self, fieldname):
        return fieldname


class DebugHandler(ExecutionHandler):
    """ExecutionHandler that logs every executed statement at DEBUG level."""

    def before_execute(self, command):
        """Log the SQL before sending it to the cursor."""
        self.adapter.db.logger.debug("SQL: %s" % command)


class SQLAdapter(BaseAdapter):
    """
    Base adapter for all SQL backends.

    Adds the SQL-flavored pipeline: expansion of Expressions through
    the dialect, statement-level entry points (``_select_wcols``,
    ``_insert``, ``_update``, ``_delete``), and the executable
    counterparts that issue them via the bound driver.

    The class-level ``execution_handlers`` list (per-class)
    accumulates ``ExecutionHandler`` instances called around every
    cursor.execute.
    """

    commit_on_alter_table = False
    can_select_for_update = True
    execution_handlers = []
    migrator_cls = Migrator

    def __init__(self, *args, **kwargs):
        super(SQLAdapter, self).__init__(*args, **kwargs)
        migrator_cls = self.adapter_args.get("migrator", self.migrator_cls)
        self.migrator = migrator_cls(self)
        self.execution_handlers = list(self.db.execution_handlers)
        if self.db._debug:
            self.execution_handlers.insert(0, DebugHandler)

    def test_connection(self):
        self.execute("SELECT 1;")

    def represent(self, obj, field_type):
        if isinstance(obj, (Expression, Field)):
            return str(obj)
        return super(SQLAdapter, self).represent(obj, field_type)

    def adapt(self, obj):
        return "'%s'" % obj.replace("'", "''")

    def smart_adapt(self, obj):
        if isinstance(obj, (int, float)):
            return str(obj)
        return self.adapt(str(obj))

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def _build_handlers_for_execution(self):
        rv = []
        for handler_class in self.execution_handlers:
            rv.append(handler_class(self))
        return rv

    def filter_sql_command(self, command):
        return command

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        # Delegated to the Driver (Layer 4). The Driver handles
        # ParamSQL forwarding and the before/after_execute handlers.
        return self.driver_io.execute(*args, **kwargs)

    def _expand(self, expression, field_type=None, colnames=False, query_env={}):
        if isinstance(expression, Field):
            if not colnames:
                rv = expression.sqlsafe
            else:
                rv = expression.longname
            if field_type == "string" and expression.type not in (
                "string",
                "text",
                "json",
                "jsonb",
                "password",
            ):
                rv = self.dialect.cast(rv, self.types["text"], query_env)
        elif isinstance(expression, (Expression, Query)):
            first = expression.first
            second = expression.second
            op = expression.op
            optional_args = expression.optional_args or {}
            optional_args["query_env"] = query_env
            if second is not None:
                rv = op(first, second, **optional_args)
            elif first is not None:
                rv = op(first, **optional_args)
            elif isinstance(op, str):
                if op.endswith(";"):
                    op = op[:-1]
                rv = "(%s)" % op
            else:
                rv = op()
        elif field_type:
            rv = self.represent(expression, field_type)
        elif isinstance(expression, (list, tuple)):
            rv = ",".join(self.represent(item, field_type) for item in expression)
        elif isinstance(expression, bool):
            rv = self.dialect.true_exp if expression else self.dialect.false_exp
        else:
            rv = expression
        return str(rv)

    def _expand_for_index(
        self, expression, field_type=None, colnames=False, query_env={}
    ):
        if isinstance(expression, Field):
            return expression._rname
        return self._expand(expression, field_type, colnames, query_env)

    @contextmanager
    def index_expander(self):
        self.expand = self._expand_for_index
        yield
        self.expand = self._expand

    def lastrowid(self, table):
        # Delegated to the Driver (Layer 4). ``table`` is unused but
        # kept on the signature for backward compatibility — some
        # adapter subclasses override it to do post-insert lookups.
        return self.driver_io.lastrowid()

    def _insert(self, table, fields):
        # Try AST pipeline first; fall back to legacy on unsupported shapes.
        if self.compiler is not None:
            try:
                from .ast_translate import table_to_insert
                return self.compiler.compile_insert(table_to_insert(table, fields))
            except NotImplementedError:
                pass
        if fields:
            return self.dialect.insert(
                table._rname,
                ",".join(el[0]._rname for el in fields),
                ",".join(self.expand(v, f.type) for f, v in fields),
            )
        return self.dialect.insert_empty(table._rname)

    def insert(self, table, fields):
        query = self._insert(table, fields)
        try:
            self.execute(query)
        except Exception:
            e = sys.exc_info()[1]
            if hasattr(table, "_on_insert_error"):
                return table._on_insert_error(table, fields, e)
            raise e
        if hasattr(table, "_primarykey"):
            pkdict = dict(
                [(k[0].name, k[1]) for k in fields if k[0].name in table._primarykey]
            )
            if pkdict:
                return pkdict
        id = self.lastrowid(table)
        if hasattr(table, "_primarykey") and len(table._primarykey) == 1:
            id = {table._primarykey[0]: id}
        if not isinstance(id, int):
            return id
        rid = Reference(id)
        (rid._table, rid._record) = (table, None)
        return rid

    def _update(self, table, query, fields):
        if self.compiler is not None:
            try:
                from .objects import Set
                from .ast_translate import set_to_update
                node = set_to_update(Set(self.db, query), fields)
                return self.compiler.compile_update(node)
            except NotImplementedError:
                pass
        sql_q = ""
        query_env = dict(current_scope=[table._tablename])
        if query:
            if use_common_filters(query):
                query = self.common_filter(query, [table])
            sql_q = self.expand(query, query_env=query_env)
        sql_v = ",".join(
            [
                "%s=%s"
                % (field._rname, self.expand(value, field.type, query_env=query_env))
                for (field, value) in fields
            ]
        )
        return self.dialect.update(table, sql_v, sql_q)

    def update(self, table, query, fields):
        sql = self._update(table, query, fields)
        try:
            self.execute(sql)
        except Exception:
            e = sys.exc_info()[1]
            if hasattr(table, "_on_update_error"):
                return table._on_update_error(table, query, fields, e)
            raise e
        try:
            return self.cursor.rowcount
        except AttributeError:
            return None

    def _delete(self, table, query):
        if self.compiler is not None:
            try:
                from .objects import Set
                from .ast_translate import set_to_delete
                node = set_to_delete(Set(self.db, query))
                return self.compiler.compile_delete(node)
            except NotImplementedError:
                pass
        sql_q = ""
        query_env = dict(current_scope=[table._tablename])
        if query:
            if use_common_filters(query):
                query = self.common_filter(query, [table])
            sql_q = self.expand(query, query_env=query_env)
        return self.dialect.delete(table, sql_q)

    def delete(self, table, query):
        sql = self._delete(table, query)
        self.execute(sql)
        try:
            return self.cursor.rowcount
        except AttributeError:
            return None

    def _colexpand(self, field, query_env):
        return self.expand(field, colnames=True, query_env=query_env)

    def _geoexpand(self, field, query_env):
        if (
            isinstance(field.type, str)
            and field.type.startswith("geo")
            and isinstance(field, Field)
        ):
            field = field.st_astext()
        return self.expand(field, query_env=query_env)

    def _build_joins_for_select(self, tablenames, param):
        if not isinstance(param, (tuple, list)):
            param = [param]
        tablemap = {}
        for item in param:
            if isinstance(item, Expression):
                item = item.first
            key = item._tablename
            if tablemap.get(key, item) is not item:
                raise ValueError("Name conflict in table list: %s" % key)
            tablemap[key] = item
        join_tables = [t._tablename for t in param if not isinstance(t, Expression)]
        join_on = [t for t in param if isinstance(t, Expression)]
        tables_to_merge = {}
        for t in join_on:
            tables_to_merge = merge_tablemaps(tables_to_merge, self.tables(t))
        join_on_tables = [t.first._tablename for t in join_on]
        for t in join_on_tables:
            if t in tables_to_merge:
                tables_to_merge.pop(t)
        important_tablenames = join_tables + join_on_tables + list(tables_to_merge)
        excluded = [t for t in tablenames if t not in important_tablenames]
        return (
            join_tables,
            join_on,
            tables_to_merge,
            join_on_tables,
            important_tablenames,
            excluded,
            tablemap,
        )

    def _ast_select_wcols(self, query, fields, attributes):
        """Try the AST pipeline. Return (colnames, sql) or None on
        NotImplementedError. Colnames are still computed the legacy
        way; only SQL generation flips to the new path.
        """
        if self.compiler is None:
            return None
        try:
            from .objects import Set
            from .ast_translate import set_to_select
            s = Set(self.db, query)
            node = set_to_select(s, fields, attributes)
            sql = self.compiler.compile_select(node)
        except NotImplementedError:
            return None
        # Replicate _select_wcols' colnames-side computation: discover
        # the tablemap, apply common filters, expand fields, compute
        # query_env, then map each field through ``_colexpand``.
        tablemap = self.tables(
            query,
            attributes.get("join", None),
            attributes.get("left", None),
            attributes.get("orderby", None),
            attributes.get("groupby", None),
        )
        if use_common_filters(query):
            query = self.common_filter(query, list(tablemap.values()))
        expanded = list(self.expand_all(fields, tablemap))
        tablemap = merge_tablemaps(tablemap, self.tables(*expanded))
        outer_scoped = attributes.get("outer_scoped", [])
        for item in outer_scoped:
            tablemap.pop(item, None)
        query_env = dict(
            current_scope=outer_scoped + list(tablemap),
            parent_scope=outer_scoped,
        )
        colnames = [self._colexpand(x, query_env) for x in expanded]
        return colnames, sql

    def _select_wcols(
        self,
        query,
        fields,
        left=False,
        join=False,
        distinct=False,
        orderby=False,
        groupby=False,
        having=False,
        limitby=False,
        orderby_on_limitby=True,
        for_update=False,
        outer_scoped=[],
        required=None,
        cache=None,
        cacheable=None,
        processor=None,
        cte_collector=None,
    ):
        # Layer 3: route SQL generation through the AST/Compiler pipeline
        # when it handles the requested shape. The legacy block below is
        # the fallback for joins/CTE/outer-scoped/unsupported corners.
        _attrs_for_ast = dict(
            left=left, join=join, distinct=distinct,
            orderby=orderby, groupby=groupby, having=having,
            limitby=limitby, orderby_on_limitby=orderby_on_limitby,
            for_update=for_update, outer_scoped=outer_scoped,
            cte_collector=cte_collector,
        )
        _ast_result = self._ast_select_wcols(query, fields, _attrs_for_ast)
        if _ast_result is not None:
            return _ast_result
        if cte_collector is None:
            cte_collector = dict(stack=[], seen=set(), is_recursive=False)
            is_toplevel = True
        else:
            is_toplevel = False

        #: parse tablemap
        tablemap = self.tables(query)
        #: apply common filters if needed
        if use_common_filters(query):
            query = self.common_filter(query, list(tablemap.values()))
        #: auto-adjust tables
        tablemap = merge_tablemaps(tablemap, self.tables(*fields))
        #: remove outer scoped tables if needed
        for item in outer_scoped:
            # FIXME: check for name conflicts
            tablemap.pop(item, None)
        if len(tablemap) < 1:
            raise SyntaxError("Set: no tables selected")

        query_tables = list(tablemap)
        #: check for_update argument
        # [Note - gi0baro] I think this should be removed since useless?
        #                  should affect only NoSQL?
        if self.can_select_for_update is False and for_update is True:
            raise SyntaxError("invalid select attribute: for_update")
        #: build joins (inner, left outer) and table names
        if join:
            (
                # FIXME? ijoin_tables is never used
                ijoin_tables,
                ijoin_on,
                itables_to_merge,
                ijoin_on_tables,
                iimportant_tablenames,
                iexcluded,
                itablemap,
            ) = self._build_joins_for_select(tablemap, join)
            tablemap = merge_tablemaps(tablemap, itables_to_merge)
            tablemap = merge_tablemaps(tablemap, itablemap)
        if left:
            (
                join_tables,
                join_on,
                tables_to_merge,
                join_on_tables,
                important_tablenames,
                excluded,
                jtablemap,
            ) = self._build_joins_for_select(tablemap, left)
            tablemap = merge_tablemaps(tablemap, tables_to_merge)
            tablemap = merge_tablemaps(tablemap, jtablemap)
        current_scope = outer_scoped + list(tablemap)
        query_env = dict(current_scope=current_scope, parent_scope=outer_scoped)
        #: prepare columns and expand fields
        colnames = [self._colexpand(x, query_env) for x in fields]
        sql_fields = ", ".join(self._geoexpand(x, query_env) for x in fields)
        table_alias = lambda name: tablemap[name].query_name(outer_scoped)[0]
        if join and not left:
            cross_joins = iexcluded + list(itables_to_merge)
            tokens = [table_alias(cross_joins[0])]
            tokens.extend(
                [
                    self.dialect.cross_join(table_alias(t), query_env)
                    for t in cross_joins[1:]
                ]
            )
            tokens.extend([self.dialect.join(t, query_env) for t in ijoin_on])
            sql_t = " ".join(tokens)
        elif not join and left:
            cross_joins = excluded + list(tables_to_merge)
            tokens = [table_alias(cross_joins[0])]
            tokens.extend(
                [
                    self.dialect.cross_join(table_alias(t), query_env)
                    for t in cross_joins[1:]
                ]
            )
            # FIXME: WTF? This is not correct syntax at least on PostgreSQL
            if join_tables:
                tokens.append(
                    self.dialect.left_join(
                        ",".join([table_alias(t) for t in join_tables]), query_env
                    )
                )
            tokens.extend([self.dialect.left_join(t, query_env) for t in join_on])
            sql_t = " ".join(tokens)
        elif join and left:
            all_tables_in_query = set(
                important_tablenames + iimportant_tablenames + query_tables
            )
            tables_in_joinon = set(join_on_tables + ijoin_on_tables)
            tables_not_in_joinon = list(
                all_tables_in_query.difference(tables_in_joinon)
            )
            tokens = [table_alias(tables_not_in_joinon[0])]
            tokens.extend(
                [
                    self.dialect.cross_join(table_alias(t), query_env)
                    for t in tables_not_in_joinon[1:]
                ]
            )
            tokens.extend([self.dialect.join(t, query_env) for t in ijoin_on])
            # FIXME: WTF? This is not correct syntax at least on PostgreSQL
            if join_tables:
                tokens.append(
                    self.dialect.left_join(
                        ",".join([table_alias(t) for t in join_tables]), query_env
                    )
                )
            tokens.extend([self.dialect.left_join(t, query_env) for t in join_on])
            sql_t = " ".join(tokens)
        else:
            sql_t = ", ".join(table_alias(t) for t in query_tables)
        #: expand query if needed
        if query:
            query = self.expand(query, query_env=query_env)
        if having:
            having = self.expand(having, query_env=query_env)
        #: groupby
        sql_grp = groupby
        if groupby:
            if isinstance(groupby, (list, tuple)):
                groupby = xorify(groupby)
            sql_grp = self.expand(groupby, query_env=query_env)
        #: orderby
        sql_ord = False
        if orderby:
            if isinstance(orderby, (list, tuple)):
                orderby = xorify(orderby)
            if str(orderby) == "<random>":
                sql_ord = self.dialect.random
            else:
                sql_ord = self.expand(orderby, query_env=query_env)
        #: set default orderby if missing
        if (
            limitby
            and not groupby
            and query_tables
            and orderby_on_limitby
            and not orderby
        ):
            sql_ord = ", ".join(
                [
                    tablemap[t][x].sqlsafe
                    for t in query_tables
                    if not isinstance(tablemap[t], Select)
                    for x in (getattr(tablemap[t], "_primarykey", None) or ["_id"])
                ]
            )

        #: build CTE
        [t.cte(cte_collector) for t in tablemap.values() if getattr(t, "is_cte", None)]
        if is_toplevel and cte_collector["stack"]:
            with_cte = [
                cte_collector["is_recursive"],
                ", ".join(cte_collector["stack"]),
            ]
        else:
            with_cte = None

        #: build sql using dialect
        return (
            colnames,
            self.dialect.select(
                sql_fields,
                sql_t,
                query,
                sql_grp,
                having,
                sql_ord,
                limitby,
                distinct,
                for_update and self.can_select_for_update,
                with_cte,
            ),
        )

    def _select(self, query, fields, attributes):
        return self._select_wcols(query, fields, **attributes)[1]

    def nested_select(self, query, fields, attributes):
        return Select(self.db, query, fields, attributes)

    def _select_aux_execute(self, sql):
        self.execute(sql)
        return self.cursor.fetchall()

    def _select_aux(self, sql, fields, attributes, colnames):
        cache = attributes.get("cache", None)
        if not cache:
            rows = self._select_aux_execute(sql)
        else:
            if isinstance(cache, dict):
                cache_model = cache["model"]
                time_expire = cache["expiration"]
                key = cache.get("key")
                if not key:
                    key = self.uri + "/" + sql + "/rows"
                    key = hashlib_md5(key).hexdigest()
            else:
                (cache_model, time_expire) = cache
                key = self.uri + "/" + sql + "/rows"
                key = hashlib_md5(key).hexdigest()
            rows = cache_model(
                key,
                lambda self=self, sql=sql: self._select_aux_execute(sql),
                time_expire,
            )
        if isinstance(rows, tuple):
            rows = list(rows)
        limitby = attributes.get("limitby", None) or (0,)
        rows = self.rowslice(rows, limitby[0], None)
        processor = attributes.get("processor", self.parse)
        cacheable = attributes.get("cacheable", False)
        return processor(rows, fields, colnames, cacheable=cacheable)

    def _cached_select(self, cache, sql, fields, attributes, colnames):
        del attributes["cache"]
        (cache_model, time_expire) = cache
        key = self.uri + "/" + sql
        key = hashlib_md5(key).hexdigest()
        args = (sql, fields, attributes, colnames)
        ret = cache_model(
            key, lambda self=self, args=args: self._select_aux(*args), time_expire
        )
        ret._restore_fields(fields)
        return ret

    def select(self, query, fields, attributes):
        colnames, sql = self._select_wcols(query, fields, **attributes)
        cache = attributes.get("cache", None)
        if cache and attributes.get("cacheable", False):
            return self._cached_select(cache, sql, fields, attributes, colnames)
        return self._select_aux(sql, fields, attributes, colnames)

    def iterselect(self, query, fields, attributes):
        colnames, sql = self._select_wcols(query, fields, **attributes)
        cacheable = attributes.get("cacheable", False)
        return self.iterparse(sql, fields, colnames, cacheable=cacheable)

    def _count(self, query, distinct=None):
        if self.compiler is not None:
            try:
                from .objects import Set
                from .ast_translate import set_to_count
                node = set_to_count(Set(self.db, query), distinct=distinct)
                return self.compiler.compile_count(node)
            except NotImplementedError:
                pass
        tablemap = self.tables(query)
        tablenames = list(tablemap)
        tables = list(tablemap.values())
        query_env = dict(current_scope=tablenames)
        sql_q = ""
        if query:
            if use_common_filters(query):
                query = self.common_filter(query, tables)
            sql_q = self.expand(query, query_env=query_env)
        sql_t = ",".join(self.table_alias(t, []) for t in tables)
        sql_fields = "*"
        if distinct:
            if isinstance(distinct, (list, tuple)):
                distinct = xorify(distinct)
            sql_fields = self.expand(distinct, query_env=query_env)
        return self.dialect.select(
            self.dialect.count(sql_fields, distinct), sql_t, sql_q
        )

    def count(self, query, distinct=None):
        self.execute(self._count(query, distinct))
        return self.cursor.fetchone()[0]

    def bulk_insert(self, table, items):
        return [self.insert(table, item) for item in items]

    def create_table(self, *args, **kwargs):
        return self.migrator.create_table(*args, **kwargs)

    def _drop_table_cleanup(self, table):
        super(SQLAdapter, self)._drop_table_cleanup(table)
        if table._dbt:
            self.migrator.file_delete(table._dbt)
            self.migrator.log("success!\n", table)

    def drop_table(self, table, mode=""):
        queries = self.dialect.drop_table(table, mode)
        for query in queries:
            if table._dbt:
                self.migrator.log(query + "\n", table)
            self.execute(query)
        self.commit()
        self._drop_table_cleanup(table)

    @deprecated("drop", "drop_table", "SQLAdapter")
    def drop(self, table, mode=""):
        return self.drop_table(table, mode="")

    def truncate(self, table, mode=""):
        # Prepare functions "write_to_logfile" and "close_logfile"
        try:
            queries = self.dialect.truncate(table, mode)
            for query in queries:
                self.migrator.log(query + "\n", table)
                self.execute(query)
            self.migrator.log("success!\n", table)
        finally:
            pass

    def create_index(self, table, index_name, *fields, **kwargs):
        expressions = [
            field._rname if isinstance(field, Field) else field for field in fields
        ]
        sql = self.dialect.create_index(index_name, table, expressions, **kwargs)
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            self.rollback()
            err = (
                "Error creating index %s\n  Driver error: %s\n"
                + "  SQL instruction: %s"
            )
            raise RuntimeError(err % (index_name, str(e), sql))
        return True

    def drop_index(self, table, index_name, if_exists=False):
        sql = self.dialect.drop_index(index_name, table, if_exists)
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            self.rollback()
            err = "Error dropping index %s\n  Driver error: %s"
            raise RuntimeError(err % (index_name, str(e)))
        return True

    def distributed_transaction_begin(self, key):
        pass

    @with_connection
    def commit(self):
        # Delegated to the Driver (Layer 4).
        return self.driver_io.commit()

    @with_connection
    def rollback(self):
        # Delegated to the Driver (Layer 4).
        return self.driver_io.rollback()

    @with_connection
    def prepare(self, key):
        self.connection.prepare()

    @with_connection
    def commit_prepared(self, key):
        self.connection.commit()

    @with_connection
    def rollback_prepared(self, key):
        self.connection.rollback()

    def create_sequence_and_triggers(self, query, table, **args):
        self.execute(query)

    def sqlsafe_table(self, tablename, original_tablename=None):
        if original_tablename is not None:
            return self.dialect.alias(original_tablename, tablename)
        return self.dialect.quote(tablename)

    def sqlsafe_field(self, fieldname):
        return self.dialect.quote(fieldname)

    def table_alias(self, tbl, current_scope=[]):
        if isinstance(tbl, str):
            tbl = self.db[tbl]
        return tbl.query_name(current_scope)[0]

    def id_query(self, table):
        pkeys = getattr(table, "_primarykey", None)
        if pkeys:
            return table[pkeys[0]] != None
        return table._id != None


class NoSQLAdapter(BaseAdapter):
    """
    Base adapter for NoSQL backends — MongoDB, Firestore, CouchDB.

    Drops the SQL-only entry points: nested ``_select`` (which would
    return a subquery string) and ``SELECT ... FOR UPDATE``. Migration
    work mostly degrades to bookkeeping (no DDL); transactions are
    typically no-ops since NoSQL drivers either auto-commit or have
    their own session model.
    """

    can_select_for_update = False

    def commit(self):
        """NoSQL backends typically auto-commit; this is a no-op."""

    def rollback(self):
        pass

    def prepare(self):
        pass

    def commit_prepared(self, key):
        pass

    def rollback_prepared(self, key):
        pass

    def id_query(self, table):
        return table._id > 0

    def create_table(self, table, migrate=True, fake_migrate=False):
        table._dbt = None
        table._notnulls = []
        for field_name in table.fields:
            if table[field_name].notnull:
                table._notnulls.append(field_name)
        table._uniques = []
        for field_name in table.fields:
            if table[field_name].unique:
                # this is unnecessary if the fields are indexed and unique
                table._uniques.append(field_name)

    def drop_table(self, table, mode=""):
        ctable = self.connection[table._tablename]
        ctable.drop()
        self._drop_table_cleanup(table)

    @deprecated("drop", "drop_table", "SQLAdapter")
    def drop(self, table, mode=""):
        return self.drop_table(table, mode="")

    def _select(self, *args, **kwargs):
        raise NotOnNOSQLError("Nested queries are not supported on NoSQL databases")

    def nested_select(self, *args, **kwargs):
        raise NotOnNOSQLError("Nested queries are not supported on NoSQL databases")


class NullAdapter(BaseAdapter):
    """
    Stub adapter used when ``DAL(None)`` is requested — no driver,
    no connection.

    Useful for tests and tooling that needs a DAL surface but won't
    actually issue any queries. ``connector`` returns a ``NullDriver``
    whose cursor silently returns empty results.
    """

    def _load_dependencies(self):
        """Install the bare ``CommonDialect`` — no representer/parser/compiler."""

        self.dialect = CommonDialect(self)

    def find_driver(self):
        pass

    def connector(self):
        return NullDriver()

# ============================================================
# Dialect base classes: CommonDialect / SQLDialect / NoSQLDialect
# ============================================================

class CommonDialect(Dialect):
    """
    Shared base for SQL and NoSQL dialects.

    Provides identifier quoting (``quote``/``varquote``/``constraint_name``),
    type-coercion plumbing (``coerce``), and the ``_force_bigints``
    helper used by ``DAL(bigint_id=True)``.
    """

    quote_template = "%s"

    def _force_bigints(self):
        if "big-id" in self.types and "reference" in self.types:
            self.types["id"] = self.types["big-id"]
            self.types["reference"] = self.types["big-reference"]

    def quote(self, val):
        return self.quote_template % val

    def varquote(self, val):
        return val

    def sequence_name(self, tablename):
        return self.quote("%s_sequence" % tablename)

    def trigger_name(self, tablename):
        return "%s_sequence" % tablename

    def coalesce_zero(self, val, query_env={}):
        return self.coalesce(val, [0], query_env)


@dialects.register_for(SQLAdapter)
class SQLDialect(CommonDialect):
    """
    Base SQL dialect — the default for every relational backend.

    Defines the ANSI-ish baseline: double-quoted identifiers,
    ``T``/``F`` for booleans (since not every backend has native
    BOOLEAN), comparison/arithmetic/string-match operators, JOIN
    builders, aggregates, date arithmetic, ``CASE WHEN``, and the
    statement constructors (``select``/``insert``/``update``/
    ``delete``/``drop_table``/``create_index``).

    Backend subclasses override only the operators that diverge.
    """

    quote_template = '"%s"'
    true = "T"
    false = "F"
    true_exp = "1"
    false_exp = "0"
    dt_sep = " "

    @sqltype_for("string")
    def type_string(self):
        return "VARCHAR(%(length)s)"

    @sqltype_for("boolean")
    def type_boolean(self):
        return "CHAR(1)"

    @sqltype_for("text")
    def type_text(self):
        return "TEXT"

    @sqltype_for("json")
    def type_json(self):
        return self.types["text"]

    @sqltype_for("password")
    def type_password(self):
        return self.types["string"]

    @sqltype_for("blob")
    def type_blob(self):
        return "BLOB"

    @sqltype_for("upload")
    def type_upload(self):
        return self.types["string"]

    @sqltype_for("integer")
    def type_integer(self):
        return "INTEGER"

    @sqltype_for("bigint")
    def type_bigint(self):
        return self.types["integer"]

    @sqltype_for("float")
    def type_float(self):
        return "FLOAT"

    @sqltype_for("double")
    def type_double(self):
        return "DOUBLE"

    @sqltype_for("decimal")
    def type_decimal(self):
        return "NUMERIC(%(precision)s,%(scale)s)"

    @sqltype_for("date")
    def type_date(self):
        return "DATE"

    @sqltype_for("time")
    def type_time(self):
        return "TIME"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "TIMESTAMP"

    @sqltype_for("id")
    def type_id(self):
        return "INTEGER PRIMARY KEY AUTOINCREMENT"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INTEGER REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s ON UPDATE %(on_update_action)s %(null)s %(unique)s"
        )

    @sqltype_for("list:integer")
    def type_list_integer(self):
        return self.types["text"]

    @sqltype_for("list:string")
    def type_list_string(self):
        return self.types["text"]

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return self.types["text"]

    @sqltype_for("big-id")
    def type_big_id(self):
        return self.types["id"]

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return self.types["reference"]

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ', CONSTRAINT  "FK_%(constraint_name)s" FOREIGN KEY '
            + "(%(field_name)s) REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s ON UPDATE %(on_update_action)s"
        )

    def alias(self, original, new):
        return ("%s AS " + self.quote_template) % (original, new)

    def insert(self, table, fields, values):
        return "INSERT INTO %s(%s) VALUES (%s);" % (table, fields, values)

    def insert_empty(self, table):
        return "INSERT INTO %s DEFAULT VALUES;" % table

    def where(self, query):
        return "WHERE %s" % query

    def update(self, table, values, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "UPDATE %s SET %s%s;" % (tablename, values, whr)

    def delete(self, table, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "DELETE FROM %s%s;" % (tablename, whr)

    def cte(self, tname, fields, sql, recursive=None):
        """
        recursive:list = [union_type, recursive_sql]
        """
        if recursive:
            r_sql_parts = ["%s %s" % (union, sql) for union, sql in recursive]
            recursive = " ".join(r_sql_parts)
            cte_select = "{select} {recursive}"
        else:
            cte_select = "{select}"

        return ("{tname}({fields}) AS (%s)" % cte_select).format(
            tname=tname, fields=fields, select=sql, recursive=recursive
        )

    def select(
        self,
        fields,
        tables,
        where=None,
        groupby=None,
        having=None,
        orderby=None,
        limitby=None,
        distinct=False,
        for_update=False,
        with_cte=None,  # ['recursive' | '', sql]
    ):
        dst, whr, grp, order, limit, offset, upd = "", "", "", "", "", "", ""
        if distinct is True:
            dst = " DISTINCT"
        elif distinct:
            dst = " DISTINCT ON (%s)" % distinct
        if where:
            whr = " %s" % self.where(where)
        if groupby:
            grp = " GROUP BY %s" % groupby
            if having:
                grp += " HAVING %s" % having
        if orderby:
            order = " ORDER BY %s" % orderby
        if limitby:
            (lmin, lmax) = limitby
            limit = " LIMIT %i" % (lmax - lmin)
            offset = " OFFSET %i" % lmin
        if for_update:
            upd = " FOR UPDATE"
        if with_cte:
            recursive, cte = with_cte
            recursive = " RECURSIVE" if recursive else ""
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""
        return "%sSELECT%s %s FROM %s%s%s%s%s%s%s;" % (
            with_cte,
            dst,
            fields,
            tables,
            whr,
            grp,
            order,
            limit,
            offset,
            upd,
        )

    def count(self, val, distinct=None, query_env={}):
        return ("COUNT(%s)" if not distinct else "COUNT(DISTINCT %s)") % self.expand(
            val, query_env=query_env
        )

    def join(self, val, query_env={}):
        if isinstance(val, (Table, Select)):
            val = val.query_name(query_env.get("parent_scope", []))
        elif not isinstance(val, str):
            val = self.expand(val, query_env=query_env)
        return "JOIN %s" % val

    def left_join(self, val, query_env={}):
        # Left join must always have an ON clause
        if not isinstance(val, str):
            val = self.expand(val, query_env=query_env)
        return "LEFT JOIN %s" % val

    def cross_join(self, val, query_env={}):
        if isinstance(val, (Table, Select)):
            val = val.query_name(query_env.get("parent_scope", []))
        elif not isinstance(val, str):
            val = self.expand(val, query_env=query_env)
        return "CROSS JOIN %s" % val

    @property
    def random(self):
        return "Random()"

    def _as(self, first, second, query_env={}):
        return "%s AS %s" % (self.expand(first, query_env=query_env), second)

    def cast(self, first, second, query_env={}):
        return "CAST(%s)" % self._as(first, second, query_env)

    def _not(self, val, query_env={}):
        return "(NOT %s)" % self.expand(val, query_env=query_env)

    def _and(self, first, second, query_env={}):
        return "(%s AND %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def _or(self, first, second, query_env={}):
        return "(%s OR %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def belongs(self, first, second, query_env={}):
        ftype = first.type
        first = self.expand(first, query_env=query_env)
        if isinstance(second, str):
            return "(%s IN (%s))" % (first, second[:-1])
        elif isinstance(second, Select):
            if len(second._qfields) != 1:
                raise ValueError("Subquery in belongs() must select exactly 1 column")
            sub = second._compile(query_env.get("current_scope", []))[1][:-1]
            return "(%s IN (%s))" % (first, sub)
        if not second:
            return "(1=0)"
        items = ",".join(
            self.expand(item, ftype, query_env=query_env) for item in second
        )
        return "(%s IN (%s))" % (first, items)

    # def regexp(self, first, second):
    #     raise NotImplementedError

    def lower(self, val, query_env={}):
        return "LOWER(%s)" % self.expand(val, query_env=query_env)

    def upper(self, first, query_env={}):
        return "UPPER(%s)" % self.expand(first, query_env=query_env)

    def like(self, first, second, escape=None, query_env={}):
        """Case sensitive like operator"""
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env)
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.expand(first, query_env=query_env),
            second,
            escape,
        )

    def ilike(self, first, second, escape=None, query_env={}):
        """Case insensitive like operator"""
        if isinstance(second, Expression):
            second = self.expand(second, "string", query_env=query_env)
        else:
            second = self.expand(second, "string", query_env=query_env).lower()
            if escape is None:
                escape = "\\"
                second = second.replace(escape, escape * 2)
        return "(%s LIKE %s ESCAPE '%s')" % (
            self.lower(first, query_env=query_env),
            second,
            escape,
        )

    def _like_escaper_default(self, term):
        if isinstance(term, Expression):
            return term
        term = term.replace("\\", "\\\\")
        term = term.replace(r"%", r"\%").replace("_", r"\_")
        return term

    def startswith(self, first, second, query_env={}):
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first, query_env=query_env),
            self.expand(
                self._like_escaper_default(second) + "%", "string", query_env=query_env
            ),
        )

    def endswith(self, first, second, query_env={}):
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.expand(first, query_env=query_env),
            self.expand(
                "%" + self._like_escaper_default(second), "string", query_env=query_env
            ),
        )

    def replace(self, first, tup, query_env={}):
        second, third = tup
        return "REPLACE(%s,%s,%s)" % (
            self.expand(first, "string", query_env=query_env),
            self.expand(second, "string", query_env=query_env),
            self.expand(third, "string", query_env=query_env),
        )

    def concat(self, *items, **kwargs):
        query_env = kwargs.get("query_env", {})
        tmp = (self.expand(x, "string", query_env=query_env) for x in items)
        return "(%s)" % " || ".join(tmp)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        if first.type in ("string", "text", "json", "jsonb"):
            if isinstance(second, Expression):
                second = Expression(
                    second.db,
                    self.concat(
                        "%",
                        Expression(
                            second.db,
                            self.replace(second, (r"%", r"\%"), query_env=query_env),
                        ),
                        r"%",
                    ),
                )
            else:
                second = "%" + self._like_escaper_default(str(second)) + r"%"
        elif first.type.startswith("list:"):
            if isinstance(second, Expression):
                second = Expression(
                    second.db,
                    self.concat(
                        r"%|",
                        Expression(
                            second.db,
                            self.replace(
                                Expression(
                                    second.db,
                                    self.replace(second, (r"%", r"\%"), query_env),
                                ),
                                ("|", "||"),
                            ),
                        ),
                        r"|%",
                    ),
                )
            else:
                second = str(second).replace("|", "||")
                second = "%|" + self._like_escaper_default(second) + "|%"
        op = case_sensitive and self.like or self.ilike
        return op(first, second, escape="\\", query_env=query_env)

    def _binary_cmp(self, op, first, second, null_form, query_env):
        """Shared SQL comparison render for eq/ne/lt/lte/gt/gte.

        null_form is the rendered SQL when second is None (e.g. ``"IS NULL"``),
        or None if comparison with NULL should raise.
        """
        if second is None:
            if null_form is None:
                raise RuntimeError("Cannot compare %s %s None" % (first, op))
            return "(%s %s)" % (
                self.expand(first, query_env=query_env),
                null_form,
            )
        if first.type in ("json", "jsonb") and isinstance(second, (str, int, float)):
            return "(%s %s '%s')" % (
                self.expand(first, query_env=query_env),
                op,
                self.expand(second, query_env=query_env),
            )
        return "(%s %s %s)" % (
            self.expand(first, query_env=query_env),
            op,
            self.expand(second, first.type, query_env=query_env),
        )

    def eq(self, first, second=None, query_env={}):
        return self._binary_cmp("=", first, second, "IS NULL", query_env)

    def ne(self, first, second=None, query_env={}):
        return self._binary_cmp("<>", first, second, "IS NOT NULL", query_env)

    def lt(self, first, second=None, query_env={}):
        return self._binary_cmp("<", first, second, None, query_env)

    def lte(self, first, second=None, query_env={}):
        return self._binary_cmp("<=", first, second, None, query_env)

    def gt(self, first, second=None, query_env={}):
        return self._binary_cmp(">", first, second, None, query_env)

    def gte(self, first, second=None, query_env={}):
        return self._binary_cmp(">=", first, second, None, query_env)

    def _is_numerical(self, field_type):
        return field_type in (
            "integer",
            "float",
            "double",
            "bigint",
            "boolean",
        ) or field_type.startswith("decimal")

    def add(self, first, second, query_env={}):
        if self._is_numerical(first.type) or isinstance(first.type, Field):
            return "(%s + %s)" % (
                self.expand(first, query_env=query_env),
                self.expand(second, first.type, query_env=query_env),
            )
        else:
            return self.concat(first, second, query_env=query_env)

    def sub(self, first, second, query_env={}):
        return "(%s - %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def mul(self, first, second, query_env={}):
        return "(%s * %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def div(self, first, second, query_env={}):
        return "(%s / %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def mod(self, first, second, query_env={}):
        return "(%s %% %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def on(self, first, second, query_env={}):
        table_rname = first.query_name(query_env.get("parent_scope", []))[0]
        if use_common_filters(second):
            second = self.adapter.common_filter(second, [first])
        return ("%s ON %s") % (table_rname, self.expand(second, query_env=query_env))

    def invert(self, first, query_env={}):
        return "%s DESC" % self.expand(first, query_env=query_env)

    def comma(self, first, second, query_env={}):
        return "%s, %s" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def extract(self, first, what, query_env={}):
        return "EXTRACT(%s FROM %s)" % (what, self.expand(first, query_env=query_env))

    def epoch(self, val, query_env={}):
        return self.extract(val, "epoch", query_env)

    def length(self, val, query_env={}):
        return "LENGTH(%s)" % self.expand(val, query_env=query_env)

    def aggregate(self, first, what, query_env={}):
        return "%s(%s)" % (what, self.expand(first, query_env=query_env))

    def not_null(self, default, field_type):
        return "NOT NULL DEFAULT %s" % self.adapter.represent(default, field_type)

    @property
    def allow_null(self):
        return ""

    def coalesce(self, first, second, query_env={}):
        expressions = [self.expand(first, query_env=query_env)] + [
            self.expand(val, first.type, query_env=query_env) for val in second
        ]
        return "COALESCE(%s)" % ",".join(expressions)

    def raw(self, val, query_env={}):
        return val

    def substring(self, field, parameters, query_env={}):
        return "SUBSTR(%s,%s,%s)" % (
            self.expand(field, query_env=query_env),
            parameters[0],
            parameters[1],
        )

    def case(self, query, true_false, query_env={}):
        _types = {bool: "boolean", int: "integer", float: "double"}
        return "CASE WHEN %s THEN %s ELSE %s END" % (
            self.expand(query, query_env=query_env),
            self.adapter.represent(
                true_false[0], _types.get(type(true_false[0]), "string")
            ),
            self.adapter.represent(
                true_false[1], _types.get(type(true_false[1]), "string")
            ),
        )

    def primary_key(self, key):
        return "PRIMARY KEY(%s)" % key

    def drop_table(self, table, mode):
        return ["DROP TABLE %s;" % table._rname]

    def truncate(self, table, mode=""):
        if mode:
            mode = " %s" % mode
        return ["TRUNCATE TABLE %s%s;" % (table._rname, mode)]

    def create_index(self, name, table, expressions, unique=False):
        uniq = " UNIQUE" if unique else ""
        with self.adapter.index_expander():
            rv = "CREATE%s INDEX %s ON %s (%s);" % (
                uniq,
                self.quote(name),
                table._rname,
                ",".join(self.expand(field) for field in expressions),
            )
        return rv

    def drop_index(self, name, table, if_exists=False):
        if_exists = "IF EXISTS " if if_exists else ""
        return "DROP INDEX %s%s;" % (if_exists, self.quote(name))

    def constraint_name(self, table, fieldname):
        return "%s_%s__constraint" % (table, fieldname)

    def concat_add(self, tablename):
        return ", ADD "

    def writing_alias(self, table):
        return table.sql_fullref


class NoSQLDialect(CommonDialect):
    """
    Base NoSQL dialect — shared by MongoDB, Firestore, CouchDB.

    ``type_*`` methods return Python types rather than SQL type names
    since NoSQL backends store native Python values. Operators that
    don't apply to NoSQL (joins, subselects, etc.) raise
    ``NotOnNOSQLError`` from the adapter rather than rendering SQL.
    """

    @sqltype_for("string")
    def type_string(self):
        """Python ``str`` is the NoSQL string type."""
        return str

    @sqltype_for("boolean")
    def type_boolean(self):
        return bool

    @sqltype_for("text")
    def type_text(self):
        return str

    @sqltype_for("json")
    def type_json(self):
        return self.types["text"]

    @sqltype_for("password")
    def type_password(self):
        return self.types["string"]

    @sqltype_for("blob")
    def type_blob(self):
        return self.types["text"]

    @sqltype_for("upload")
    def type_upload(self):
        return self.types["string"]

    @sqltype_for("integer")
    def type_integer(self):
        return int

    @sqltype_for("bigint")
    def type_bigint(self):
        return self.types["integer"]

    @sqltype_for("float")
    def type_float(self):
        return float

    @sqltype_for("double")
    def type_double(self):
        return self.types["float"]

    @sqltype_for("date")
    def type_date(self):
        return date

    @sqltype_for("time")
    def type_time(self):
        return time

    @sqltype_for("datetime")
    def type_datetime(self):
        return datetime

    @sqltype_for("id")
    def type_id(self):
        return int

    @sqltype_for("reference")
    def type_reference(self):
        return int

    @sqltype_for("list:integer")
    def type_list_integer(self):
        return list

    @sqltype_for("list:string")
    def type_list_string(self):
        return list

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return list

    def quote(self, val):
        return val

# ============================================================
# Parser base classes: BasicParser / *Parser mixins / Commonparser
# ============================================================

class BasicParser(Parser):
    """
    Default per-type parsers: id/integer/float/double/boolean/blob
    plus the reference-resolution path.

    Most SQL parsers compose this with date/time/json/lists mixins.
    """

    @for_type("id")
    def _id(self, value):
        """Coerce id columns to ``int``."""
        return int(value)

    @for_type("integer")
    def _integer(self, value):
        """Coerce integer columns to ``int``."""
        return int(value)

    @for_type("float")
    def _float(self, value):
        """Coerce float columns to ``float``."""
        return float(value)

    @for_type("double")
    def _double(self, value):
        """Double columns share the float decoder."""
        return self.registered["float"](value, "double")

    @for_type("boolean")
    def _boolean(self, value):
        """
        Coerce boolean columns to Python ``bool``.

        Matches the dialect's ``true`` token (typically ``"T"`` or ``1``)
        or any string starting with ``T`` / ``t``.
        """
        return value == self.dialect.true or str(value)[:1].lower() == "t"

    @for_type("blob")
    def _blob(self, value):
        """Base64-decode blob content; try to decode as text on the way out."""
        decoded = b64decode(to_bytes(value))
        try:
            decoded = to_native(decoded)
        except (UnicodeDecodeError, AttributeError):
            pass
        return decoded

    @before_parse("reference")
    def reference_extras(self, field_type):
        return {"referee": field_type[10:].strip()}

    @for_type("reference")
    def _reference(self, value, referee):
        if "." not in referee:
            value = Reference(value)
            value._table, value._record = self.adapter.db[referee], None
        return value

    @before_parse("list:reference")
    def referencelist_extras(self, field_type):
        return {"field_type": field_type}

    @for_type("list:reference")
    def _list_references(self, value, field_type):
        return [self.registered["reference"](el, field_type[5:]) for el in value]

    @for_type("bigint")
    def _bigint(self, value):
        return self.registered["integer"](value, "bigint")


class DateParser(Parser):
    """Decode ``date`` columns from ISO strings or driver-native datetimes."""

    @for_type("date")
    def _date(self, value):
        """Parse ``YYYY-MM-DD`` or take ``.date()`` of a datetime."""
        if isinstance(value, datetime):
            return value.date()
        (y, m, d) = map(int, str(value)[:10].strip().split("-"))
        return date(y, m, d)


class TimeParser(Parser):
    """Decode ``time`` columns from ``HH:MM:SS`` strings or datetimes."""

    @for_type("time")
    def _time(self, value):
        """Parse ``HH:MM:SS`` or take ``.time()`` of a datetime."""
        if isinstance(value, datetime):
            return value.time()
        time_items = list(map(int, str(value)[:8].strip().split(":")[:3]))
        if len(time_items) == 3:
            (h, mi, s) = time_items
        else:
            (h, mi, s) = time_items + [0]
        return time(h, mi, s)


class DateTimeParser(Parser):
    """
    Decode ``datetime`` columns from ISO strings.

    Recognizes optional fractional seconds and ``+HH:MM`` / ``-HH:MM``
    / ``Z`` timezone suffixes, normalizing to a naive datetime offset
    by the timezone delta.
    """

    @for_type("datetime")
    def _datetime(self, value):
        """Parse an ISO datetime string and normalize the timezone offset."""
        value = str(value)
        date_part, time_part, timezone = value[:10], value[11:19], value[19:]
        if "+" in timezone:
            ms, tz = timezone.split("+")
            h, m = tz.split(":")
            dt = timedelta(seconds=3600 * int(h) + 60 * int(m))
        elif "-" in timezone:
            ms, tz = timezone.split("-")
            h, m = tz.split(":")
            dt = -timedelta(seconds=3600 * int(h) + 60 * int(m))
        else:
            ms = timezone.upper().split("Z")[0]
            dt = None
        (y, m, d) = map(int, date_part.split("-"))
        time_parts = time_part and time_part.split(":")[:3] or (0, 0, 0)
        while len(time_parts) < 3:
            time_parts.append(0)
        time_items = map(int, time_parts)
        (h, mi, s) = time_items
        if ms and ms[0] == ".":
            ms = int(float("0" + ms) * 1000000)
        else:
            ms = 0
        value = datetime(y, m, d, h, mi, s, ms)
        if dt:
            value = value + dt
        return value


class DecimalParser(Parser):
    """Decode ``decimal`` columns to Python ``Decimal``."""

    @for_type("decimal")
    def _decimal(self, value):
        """Wrap the driver-supplied value in ``Decimal``."""
        return Decimal(value)


class JSONParser(Parser):
    """Decode ``json`` columns from JSON strings."""

    @for_type("json")
    def _json(self, value):
        """Parse a JSON string with the stdlib decoder."""
        if not isinstance(value, str):
            raise RuntimeError("json data not a string")
        return json.loads(value)


class ListsParser(BasicParser):
    """
    Decode ``list:*`` columns from the pydal pipe-delimited encoding.

    See ``helpers.methods.bar_encode`` / ``bar_decode_*`` for the
    wire-format details.
    """

    @for_type("list:integer")
    def _list_integers(self, value):
        """Decode ``|1|2|3|`` to ``[1, 2, 3]``."""
        return bar_decode_integer(value)

    @for_type("list:string")
    def _list_strings(self, value):
        """Decode ``|a|b|c|`` (with ``||`` escaping) to ``["a", "b", "c"]``."""
        return bar_decode_string(value)

    @for_type("list:reference")
    def _list_references(self, value, field_type):
        """Decode a list of foreign-key ids into a list of ``Reference``."""
        value = bar_decode_integer(value)
        return [self.registered["reference"](el, field_type[5:]) for el in value]


@parsers.register_for(SQLAdapter)
class Commonparser(
    ListsParser, DateParser, TimeParser, DateTimeParser, DecimalParser, JSONParser
):
    """Default SQL parser — composes every base mixin."""

# ============================================================
# Representer base classes: BaseRepresenter / JSONRepresenter
# SQLRepresenter / NoSQLRepresenter
# ============================================================

class BaseRepresenter(Representer):
    """
    Generic representer: id/integer/decimal/double/date/datetime/time
    plus the boolean ``T``/``F`` mapping via ``dialect.true`` /
    ``dialect.false``.
    """

    @repr_for_type("boolean", adapt=False)
    def _boolean(self, value):
        """Render booleans via the dialect's true/false tokens."""
        if value and not str(value)[:1].upper() in "0F":
            return self.adapter.smart_adapt(self.dialect.true)
        return self.adapter.smart_adapt(self.dialect.false)

    @repr_for_type("id", adapt=False)
    def _id(self, value):
        return str(int(value))

    @repr_for_type("integer", adapt=False)
    def _integer(self, value):
        return str(int(value))

    @repr_for_type("decimal", adapt=False)
    def _decimal(self, value):
        return str(value)

    @repr_for_type("double", adapt=False)
    def _double(self, value):
        return repr(float(value))

    @repr_for_type("date", encode=True)
    def _date(self, value):
        if isinstance(value, (date, datetime)):
            return value.isoformat()[:10]
        return str(value)

    @repr_for_type("time", encode=True)
    def _time(self, value):
        if isinstance(value, time):
            return value.isoformat()[:10]
        return str(value)

    @repr_for_type("datetime", encode=True)
    def _datetime(self, value):
        if isinstance(value, datetime):
            value = value.isoformat(self.dialect.dt_sep)[:19]
        elif isinstance(value, date):
            value = value.isoformat()[:10] + self.dialect.dt_sep + "00:00:00"
        else:
            value = str(value)
        return value

    def _ensure_list(self, value):
        if not value:
            value = []
        elif not isinstance(value, (list, tuple)):
            value = [value]
        return value

    def _listify_elements(self, elements):
        return bar_encode(elements)

    @repr_for_type("list:integer")
    def _list_integer(self, value):
        values = self._ensure_list(value)
        values = [int(val) for val in values if val != ""]
        return self._listify_elements(values)

    @repr_for_type("list:string")
    def _list_string(self, value):
        value = self._ensure_list(value)
        value = list(map(str, value))
        return self._listify_elements(value)

    @repr_for_type("list:reference", adapt=False)
    def _list_reference(self, value):
        return self.registered_t["list:integer"](value, "list:reference")


class JSONRepresenter(Representer):
    """JSON encoder mixin for backends with a ``json`` column type."""

    @repr_for_type("json", encode=True)
    def _json(self, value):
        """Serialize a Python value to a JSON string literal."""
        return serializers.json(value)


@representers.register_for(SQLAdapter)
class SQLRepresenter(BaseRepresenter):
    """
    Default SQL representer — handles strings, text, blobs, password,
    upload, list-of-* types, reference columns, and SQLCustomType.

    Strings are SQL-quoted via ``adapter.adapt``; blobs are
    base64-encoded; ``list:*`` values use pipe-delimited encoding.
    """

    def _custom_type(self, value, field_type):
        """Render a ``SQLCustomType`` via its ``encoder`` callback."""
        value = field_type.encoder(value)
        if value and field_type.type in ("string", "text", "json"):
            return self.adapter.adapt(value)
        return value or "NULL"

    @pre()
    def _before_all(self, obj, field_type):
        if isinstance(field_type, SQLCustomType):
            return self._custom_type(obj, field_type)
        if obj == "" and not field_type[:2] in ("st", "te", "js", "pa", "up"):
            return "NULL"
        r = self.exceptions(obj, field_type)
        return r

    def exceptions(self, obj, field_type):
        return None

    @for_instance(NoneType)
    def _none(self, value, field_type):
        return "NULL"

    @for_instance(Expression)
    def _expression(self, value, field_type):
        return str(value)

    @for_instance(Field)
    def _fieldexpr(self, value, field_type):
        return str(value)

    @before_type("reference")
    def reference_extras(self, field_type):
        return {"referenced": field_type[9:].strip()}

    @repr_for_type("reference", adapt=False)
    def _reference(self, value, referenced):
        if referenced in self.adapter.db.tables:
            return str(int(value))
        p = referenced.partition(".")
        if p[2] != "":
            try:
                ftype = self.adapter.db[p[0]][p[2]].type
                return self.adapter.represent(value, ftype)
            except (ValueError, KeyError):
                return repr(value)
        elif isinstance(value, (Row, Reference)):
            return str(value["id"])
        return str(int(value))

    @repr_for_type("blob", encode=True)
    def _blob(self, value):
        return b64encode(to_bytes(value))


@representers.register_for(NoSQLAdapter)
class NoSQLRepresenter(BaseRepresenter):
    """
    Default NoSQL representer — stores native Python values without
    SQL quoting. ``adapt`` is a no-op since NoSQL drivers receive
    Python objects, not SQL fragments.
    """

    def adapt(self, value):
        """No-op for NoSQL — drivers handle parameter encoding themselves."""
        return value

    @pre(is_breaking=True)
    def _before_all(self, obj, field_type):
        if isinstance(field_type, SQLCustomType):
            return True, field_type.encoder(obj)
        return False, obj

    @pre(is_breaking=True)
    def _nullify_empty_string(self, obj, field_type):
        if obj == "" and not (
            isinstance(field_type, str) and field_type[:2] in ("st", "te", "pa", "up")
        ):
            return True, None
        return False, obj

    @for_instance(NoneType)
    def _none(self, value, field_type):
        return None

    @for_instance(list, repr_type=True)
    def _repr_list(self, value, field_type):
        if isinstance(field_type, str) and not field_type.startswith("list:"):
            return [self.adapter.represent(v, field_type) for v in value]
        return value

    @repr_for_type("id")
    def _id(self, value):
        return int(value)

    @repr_for_type("integer")
    def _integer(self, value):
        return int(value)

    @repr_for_type("bigint")
    def _bigint(self, value):
        return int(value)

    @repr_for_type("double")
    def _double(self, value):
        return float(value)

    @repr_for_type("reference")
    def _reference(self, value):
        if isinstance(value, (Row, Reference)):
            value = value["id"]
        return int(value)

    @repr_for_type("boolean")
    def _boolean(self, value):
        if not isinstance(value, bool):
            if value and not str(value)[:1].upper() in "0F":
                return True
            return False
        return value

    @repr_for_type("string")
    def _string(self, value):
        return to_unicode(value)

    @repr_for_type("password")
    def _password(self, value):
        return to_unicode(value)

    @repr_for_type("text")
    def _text(self, value):
        return to_unicode(value)

    @repr_for_type("blob")
    def _blob(self, value):
        return value

    @repr_for_type("json")
    def _json(self, value):
        if isinstance(value, str):
            value = to_unicode(value)
            value = json.loads(value)
        return value

    def _represent_list(self, value):
        items = self._ensure_list(value)
        return [item for item in items if item is not None]

    @repr_for_type("date")
    def _date(self, value):
        if not isinstance(value, date):
            (y, m, d) = map(int, str(value).strip().split("-"))
            value = date(y, m, d)
        elif isinstance(value, datetime):
            (y, m, d) = (value.year, value.month, value.day)
            value = date(y, m, d)
        return value

    @repr_for_type("time")
    def _time(self, value):
        if not isinstance(value, time):
            time_items = list(map(int, str(value).strip().split(":")[:3]))
            if len(time_items) == 3:
                (h, mi, s) = time_items
            else:
                (h, mi, s) = time_items + [0]
            value = time(h, mi, s)
        return value

    @repr_for_type("datetime")
    def _datetime(self, value):
        if not isinstance(value, datetime):
            svalue = str(value)[:19]
            (y, m, d) = map(int, svalue[:10].strip().split("-"))
            tp = svalue[11:].strip().split(":")[:3]
            while len(tp) < 3:
                tp.append(0)
            (h, mi, s) = map(int, tp)
            value = datetime(y, m, d, h, mi, s)
        return value

    @repr_for_type("list:integer")
    def _list_integer(self, value):
        values = self._represent_list(value)
        return list(map(int, values))

    @repr_for_type("list:string")
    def _list_string(self, value):
        values = self._represent_list(value)
        return list(map(to_unicode, values))

    @repr_for_type("list:reference")
    def _list_reference(self, value):
        values = self._represent_list(value)
        return list(map(int, values))
