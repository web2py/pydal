# -*- coding: utf-8 -*-

"""
Bespoke container classes used across pydal.

Public surface:

* ``BasicStorage`` — dict-with-attribute-access base used by ``Row``,
  ``Table``, ``Select``, etc.
* ``OpRow`` — preserves ``(field, value)`` pairs for
  insert/update plumbing.
* ``Reference`` — an ``int`` subclass that lazily fetches the
  referenced row.
* ``SQLALL`` — placeholder used by ``Table.ALL`` to expand to a full
  field list inside ``select``.
* ``SQLCustomType`` — user-defined column type with encode/decode hooks.
* ``Serializable`` — mixin adding ``as_dict``/``as_xml``/``as_json``/
  ``as_yaml``.

Internal:

* ``cachedprop`` — read-only property cached on first access.
* ``SQLCallableList`` — ``list`` subclass that returns a shallow copy
  when called (used as ``db.tables``).
* ``RecordOperator`` / ``RecordUpdater`` / ``RecordDeleter`` —
  per-row update/delete shortcuts attached to fetched ``Row``s.
* ``MethodAdder`` — decorator used by ``table.methods.add``.
* ``FakeCursor`` / ``NullCursor`` / ``FakeDriver`` / ``NullDriver`` —
  test scaffolding for adapter behavior without a real driver.
* ``ExecutionHandler`` / ``TimingHandler`` — hooks called before/after
  each ``cursor.execute``.
* ``DatabaseStoredFile`` — store ``.table`` migration metadata in the
  database (mysql/postgres/sqlite only).
"""

import copy
import copyreg
import marshal
import struct
import time
import traceback
from os.path import exists
from typing import Any, Callable, Optional, Set

from .._globals import THREAD_LOCAL
from ..utils import to_bytes
from .serializers import serializers


class cachedprop:
    """
    Read-only ``@property`` that caches its result on first access.

    The wrapped function runs once per instance; subsequent reads hit
    the instance ``__dict__`` directly, bypassing the descriptor.
    """

    def __init__(self, fget: Callable, doc: Optional[str] = None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = self.fget(obj)
        return result


class BasicStorage:
    """
    Dict-with-attribute-access base used as the storage backbone for
    ``Row``, ``Table``, and friends.

    ``s.foo`` and ``s["foo"]`` are interchangeable; ``s["foo"] = 1``
    bypasses ``__setattr__`` (relevant when subclasses override it).
    Most dict methods are forwarded to the underlying ``__dict__``.
    """

    def __init__(self, *args, **kwargs):
        return self.__dict__.__init__(*args, **kwargs)

    def __getitem__(self, key):
        return self.__dict__.__getitem__(str(key))

    __setitem__ = object.__setattr__

    def __delitem__(self, key):
        try:
            delattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __bool__(self) -> bool:
        return len(self.__dict__) > 0

    __iter__ = lambda self: self.__dict__.__iter__()
    __str__ = lambda self: self.__dict__.__str__()
    __repr__ = lambda self: self.__dict__.__repr__()
    has_key = __contains__ = lambda self, key: key in self.__dict__

    def get(self, key, default=None):
        return self.__dict__.get(key, default)

    def update(self, *args, **kwargs):
        return self.__dict__.update(*args, **kwargs)

    def keys(self):
        return self.__dict__.keys()

    def iterkeys(self):
        return iter(self.__dict__.keys())

    def values(self):
        return self.__dict__.values()

    def itervalues(self):
        return iter(self.__dict__.values())

    def items(self):
        return self.__dict__.items()

    def iteritems(self):
        return iter(self.__dict__.items())

    pop = lambda self, *args, **kwargs: self.__dict__.pop(*args, **kwargs)
    clear = lambda self, *args, **kwargs: self.__dict__.clear(*args, **kwargs)
    copy = lambda self, *args, **kwargs: self.__dict__.copy(*args, **kwargs)


def pickle_basicstorage(s):
    """Pickle a ``BasicStorage`` as a plain dict."""
    return BasicStorage, (dict(s),)


copyreg.pickle(BasicStorage, pickle_basicstorage)


class OpRow:
    """
    Ordered ``(field, value)`` pairs collected for an INSERT or UPDATE.

    Unlike a plain dict, OpRow remembers the original ``Field`` object
    alongside the value, so the dialect can render the column with the
    field's ``rname`` / type metadata.
    """

    __slots__ = ("_table", "_fields", "_values")

    def __init__(self, table):
        object.__setattr__(self, "_table", table)
        object.__setattr__(self, "_fields", {})
        object.__setattr__(self, "_values", {})

    def set_value(self, key: str, value: Any, field=None) -> None:
        """Store ``value`` under ``key``; remember the associated field."""
        self._values[key] = value
        self._fields[key] = self._fields.get(key, field or self._table[key])

    def del_value(self, key: str) -> None:
        """Drop ``key`` from both the value- and field-maps."""
        del self._values[key]
        del self._fields[key]

    def __getitem__(self, key):
        return self._values[key]

    def __setitem__(self, key, value):
        return self.set_value(key, value)

    def __delitem__(self, key):
        return self.del_value(key)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError

    def __setattr__(self, key, value):
        return self.set_value(key, value)

    def __delattr__(self, key):
        return self.del_value(key)

    def __iter__(self):
        return self._values.__iter__()

    def __contains__(self, key) -> bool:
        return key in self._values

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return self._values.keys()

    def iterkeys(self):
        return iter(self._values.keys())

    def values(self):
        return self._values.values()

    def itervalues(self):
        return iter(self._values.values())

    def items(self):
        return self._values.items()

    def iteritems(self):
        return iter(self._values.items())

    def op_values(self):
        """
        Return the ordered list of ``(Field, value)`` pairs.

        This is the canonical form consumed by ``adapter._insert`` and
        ``adapter._update``.
        """
        return [(self._fields[key], value) for key, value in self._values.items()]

    def __repr__(self):
        return "<OpRow %s>" % repr(self._values)


class Serializable:
    """
    Mixin providing ``as_dict`` / ``as_xml`` / ``as_json`` / ``as_yaml``.

    Subclasses override ``as_dict`` to produce a serializable view of
    themselves; the other methods route through the shared
    ``serializers`` instance.
    """

    def as_dict(self, flat: bool = False, sanitize: bool = True):
        """Return a dict view; subclasses override to flatten state."""
        return self.__dict__

    def as_xml(self, sanitize: bool = True) -> str:
        """Serialize ``as_dict(flat=True)`` to XML."""
        return serializers.xml(self.as_dict(flat=True, sanitize=sanitize))

    def as_json(self, sanitize: bool = True) -> str:
        """Serialize ``as_dict(flat=True)`` to JSON."""
        return serializers.json(self.as_dict(flat=True, sanitize=sanitize))

    def as_yaml(self, sanitize: bool = True) -> str:
        """Serialize ``as_dict(flat=True)`` to YAML."""
        return serializers.yaml(self.as_dict(flat=True, sanitize=sanitize))


class Reference(int):
    """
    Foreign-key value that lazily loads the referenced row.

    Accessing ``ref.some_field`` triggers a fetch of the underlying
    record (via ``self._table[int(self)]``) the first time, then
    delegates further reads to the cached row. The wrapped integer is
    available as ``int(ref)`` and ``ref.id``.
    """

    def __allocate(self):
        """Fetch the referenced row from the database on first access."""
        if not self._record:
            self._record = self._table[int(self)]
        if not self._record:
            raise RuntimeError(
                "Using a recursive select but encountered a broken "
                + "reference: %s %d" % (self._table, int(self))
            )

    def __getattr__(self, key, default=None):
        if key == "id":
            return int(self)
        if key in self._table:
            self.__allocate()
        if self._record:
            return self._record.get(key, default)
        return None

    def get(self, key, default=None):
        return self.__getattr__(key, default)

    def __setattr__(self, key, value):
        if key.startswith("_"):
            int.__setattr__(self, key, value)
            return
        self.__allocate()
        self._record[key] = value

    def __getitem__(self, key):
        if key == "id":
            return int(self)
        self.__allocate()
        return self._record.get(key, None)

    def __setitem__(self, key, value):
        self.__allocate()
        self._record[key] = value


def Reference_unpickler(data):
    """Pickle-protocol callable: reconstruct a Reference from marshaled int."""
    return marshal.loads(data)


def Reference_pickler(data):
    """
    Pickle-protocol callable: serialize a Reference to a marshaled int.

    Older marshal modules lacked ``dumps`` on int subclasses; the
    fallback uses ``struct.pack`` directly.
    """
    try:
        marshal_dump = marshal.dumps(int(data))
    except AttributeError:
        marshal_dump = "i%s" % struct.pack("<i", int(data))
    return (Reference_unpickler, (marshal_dump,))


copyreg.pickle(Reference, Reference_pickler, Reference_unpickler)


class SQLCallableList(list):
    """
    ``list`` subclass that returns a shallow copy when *called*.

    Used as ``db.tables`` so ``db.tables()`` (no-arg call) yields a
    safe-to-mutate snapshot of the table-name list.
    """

    def __call__(self):
        return copy.copy(self)


class SQLALL:
    """
    Marker emitted by ``Table.ALL`` that expands to every field of a
    table inside ``Set.select``.

    Normally only constructed internally by ``define_table`` /
    ``with_alias``.
    """

    def __init__(self, table):
        self._table = table

    def __str__(self):
        return ", ".join([str(field) for field in self._table])


class SQLCustomType:
    """
    User-defined column type with custom encode/decode hooks.

    Args:
        type: the pydal-level type (default ``"string"``).
        native: the backend-specific column type (e.g. ``"integer"``).
        encoder: function to encode a value before storing.
        decoder: function to decode a fetched value back to a Python
            object.
        validator: validator(s) to use; defaults to the type's default.

    Example::

        decimal = SQLCustomType(
            type='double',
            native='integer',
            encoder=lambda x: int(float(x) * 100),
            decoder=lambda x: Decimal("0.00") + Decimal(str(float(x) / 100)),
        )

        db.define_table('example', Field('value', type=decimal))
    """

    def __init__(
        self,
        type: str = "string",
        native: Optional[str] = None,
        encoder: Optional[Callable] = None,
        decoder: Optional[Callable] = None,
        validator=None,
        _class: Optional[str] = None,
        widget=None,
        represent=None,
    ):
        self.type = type
        self.native = native
        self.encoder = encoder or (lambda x: x)
        self.decoder = decoder or (lambda x: x)
        self.validator = validator
        self._class = _class or type
        self.widget = widget
        self.represent = represent

    def startswith(self, text: Optional[str] = None) -> bool:
        """``str.startswith`` proxy for type-name matching."""
        try:
            return self.type.startswith(self, text)
        except TypeError:
            return False

    def endswith(self, text: Optional[str] = None) -> bool:
        """``str.endswith`` proxy for type-name matching."""
        try:
            return self.type.endswith(self, text)
        except TypeError:
            return False

    def __getslice__(self, a: int = 0, b: int = 100):
        return None

    def __getitem__(self, i):
        return None

    def __str__(self) -> str:
        return self._class


class RecordOperator:
    """
    Base for per-row update/delete operators attached to fetched Rows.

    Carries a back-reference to the originating column set, table, and
    id; subclasses define the actual operation in ``__call__``.
    """

    def __init__(self, colset, table, id):
        self.colset = colset
        self.db = table._db
        self.tablename = table._tablename
        self.id = id

    def __call__(self):
        pass


class RecordUpdater(RecordOperator):
    """
    Callable attached to ``Row.update_record`` — runs an UPDATE for
    this row and refreshes the colset with the new values.
    """

    def __call__(self, **fields):
        colset, db, tablename, id = self.colset, self.db, self.tablename, self.id
        table = db[tablename]
        newfields = fields or dict(colset)
        for fieldname in list(newfields.keys()):
            if fieldname not in table.fields or table[fieldname].type == "id":
                del newfields[fieldname]
        table._db(table._id == id, ignore_common_filters=True).update(**newfields)
        colset.update(newfields)
        return colset


class RecordDeleter(RecordOperator):
    """Callable attached to ``Row.delete_record`` — runs a DELETE for this row."""

    def __call__(self):
        return self.db(self.db[self.tablename]._id == self.id).delete()


class MethodAdder:
    """
    Decorator entry-point used by ``table.methods.add``.

    ``table.methods.foo`` returns a decorator that binds the decorated
    function as a method named ``foo`` on the table instance.
    """

    def __init__(self, table):
        self.table = table

    def __call__(self):
        return self.register()

    def __getattr__(self, method_name):
        return self.register(method_name)

    def register(self, method_name: Optional[str] = None):
        """Return a decorator that binds ``f`` to the table as a method."""

        def _decorated(f):
            import types

            instance = self.table
            method = types.MethodType(f, instance)
            name = method_name or f.__name__
            setattr(instance, name, method)
            return f

        return _decorated


class FakeCursor:
    """
    Stand-in cursor for adapters that don't support DB-API cursors
    (NoSQL drivers). Any attribute access raises so unimplemented code
    paths surface loudly.
    """

    def warn_bad_usage(self, attr: str):
        """Raise to flag access to an unimplemented cursor method."""
        raise Exception("FakeCursor.%s is not implemented" % attr)

    def __getattr__(self, attr):
        self.warn_bad_usage(attr)

    def __setattr__(self, attr, value):
        self.warn_bad_usage(attr)

    def close(self):
        return


class NullCursor(FakeCursor):
    """Quieter ``FakeCursor`` whose attribute access returns an empty list."""

    lastrowid = 1

    def __getattr__(self, attr):
        return lambda *a, **b: []


class FakeDriver(BasicStorage):
    """
    Driver stand-in. ``cursor()`` returns a ``FakeCursor``;
    ``commit()`` / ``close()`` are no-ops.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._build_cursor_()

    def _build_cursor_(self):
        """Construct and stash the inner cursor."""
        self._fake_cursor_ = FakeCursor()

    def cursor(self):
        """Return the stashed cursor."""
        return self._fake_cursor_

    def close(self):
        return None

    def commit(self):
        return None

    def __str__(self):
        state = ["%s=%r" % (attribute, value) for (attribute, value) in self.items()]
        return "\n".join(state)


class NullDriver(FakeDriver):
    """``FakeDriver`` variant whose cursor returns empty lists silently."""

    def _build_cursor_(self):
        self._fake_cursor_ = NullCursor()


class ExecutionHandler:
    """
    Base class for the before/after-execute hooks an adapter calls
    around every ``cursor.execute``. Subclasses (e.g. ``TimingHandler``,
    ``DebugHandler``) override the two empty methods.
    """

    def __init__(self, adapter):
        self.adapter = adapter

    def before_execute(self, command):
        """Called just before ``cursor.execute``."""

    def after_execute(self, command):
        """Called just after ``cursor.execute``."""


class TimingHandler(ExecutionHandler):
    """Per-statement timing hook — stores the most recent N timings on THREAD_LOCAL."""

    MAXSTORAGE = 100

    def _timings(self):
        THREAD_LOCAL._pydal_timings_ = getattr(THREAD_LOCAL, "_pydal_timings_", [])
        return THREAD_LOCAL._pydal_timings_

    @property
    def timings(self):
        """The recorded ``(command, elapsed_seconds)`` list for this thread."""
        return self._timings()

    def before_execute(self, command):
        self.t = time.time()

    def after_execute(self, command):
        dt = time.time() - self.t
        self.timings.append((command, dt))
        # Trim to MAXSTORAGE.
        del self.timings[: -self.MAXSTORAGE]


class DatabaseStoredFile:
    """
    File-like object that stores migration metadata in a database
    table (``web2py_filesystem``) instead of on disk.

    Supported on MySQL, PostgreSQL, and SQLite. The table is created
    lazily the first time it's needed per DAL URI.
    """

    web2py_filesystems: Set[str] = set()

    def escape(self, obj):
        return self.db._adapter.escape(obj)

    @staticmethod
    def try_create_web2py_filesystem(db) -> None:
        """Ensure the ``web2py_filesystem`` table exists on ``db``."""
        if db._uri not in DatabaseStoredFile.web2py_filesystems:
            if db._adapter.dbengine not in ("mysql", "postgres", "sqlite"):
                raise NotImplementedError(
                    "DatabaseStoredFile only supported by mysql, postgresql, sqlite"
                )
            blobType = "BYTEA" if db._adapter.dbengine == "postgres" else "BLOB"
            sql = (
                "CREATE TABLE IF NOT EXISTS web2py_filesystem (path VARCHAR(255), content %(blobType)s, PRIMARY KEY(path));"
                % {"blobType": blobType}
            )
            if db._adapter.dbengine == "mysql":
                sql = sql[:-1] + " ENGINE=InnoDB;"
            db.executesql(sql)
            DatabaseStoredFile.web2py_filesystems.add(db._uri)

    def __init__(self, db, filename: str, mode: str):
        if db._adapter.dbengine not in ("mysql", "postgres", "sqlite"):
            raise RuntimeError(
                "only MySQL/Postgres/SQLite can store metadata .table files"
                " in database for now"
            )
        self.db = db
        self.filename = filename
        self.mode = mode
        DatabaseStoredFile.try_create_web2py_filesystem(db)
        self.p = 0
        self.data = b""
        if mode in ("r", "rw", "rb", "a", "ab"):
            query = "SELECT content FROM web2py_filesystem WHERE path='%s'" % filename
            rows = self.db.executesql(query)
            if rows:
                self.data = to_bytes(rows[0][0])
            elif exists(filename):
                with open(filename, "rb") as datafile:
                    self.data = datafile.read()
            elif mode in ("r", "rw", "rb"):
                raise RuntimeError("File %s does not exist" % filename)

    def read(self, bytes: Optional[int] = None) -> bytes:
        """Return up to ``bytes`` from the current position (default: all remaining)."""
        if bytes is None:
            bytes = len(self.data)
        data = self.data[self.p: self.p + bytes]
        self.p += len(data)
        return data

    def readinto(self, bytes):
        return self.read(bytes)

    def readline(self) -> bytes:
        """Return bytes through (and including) the next newline."""
        i = self.data.find(b"\n", self.p) + 1
        if i > 0:
            data, self.p = self.data[self.p: i], i
        else:
            data, self.p = self.data[self.p:], len(self.data)
        return data

    def write(self, data) -> None:
        """Append ``data`` to the in-memory buffer (not yet persisted)."""
        self.data += data

    def close_connection(self) -> None:
        """Persist the buffer to the database, replacing any prior content."""
        if self.db is not None:
            self.db.executesql(
                "DELETE FROM web2py_filesystem WHERE path='%s'" % self.filename
            )
            placeholder = "?" if self.db._adapter.dbengine == "sqlite" else "%s"
            query = (
                "INSERT INTO web2py_filesystem(path,content) VALUES (%(placeholder)s, %(placeholder)s)"
                % {"placeholder": placeholder}
            )
            args = (self.filename, self.data)
            self.db.executesql(query, args)
            self.db.commit()
            self.db = None

    def close(self):
        self.close_connection()

    @staticmethod
    def is_operational_error(db, error) -> Optional[bool]:
        """True iff ``error`` is the driver's OperationalError type."""
        if not hasattr(db._adapter.driver, "OperationalError"):
            return None
        return isinstance(error, db._adapter.driver.OperationalError)

    @staticmethod
    def is_programming_error(db, error) -> Optional[bool]:
        """True iff ``error`` is the driver's ProgrammingError type."""
        if not hasattr(db._adapter.driver, "ProgrammingError"):
            return None
        return isinstance(error, db._adapter.driver.ProgrammingError)

    @staticmethod
    def exists(db, filename: str) -> bool:
        """True iff ``filename`` exists on disk OR in the DB filesystem."""
        if exists(filename):
            return True

        DatabaseStoredFile.try_create_web2py_filesystem(db)

        query = "SELECT path FROM web2py_filesystem WHERE path='%s'" % filename
        try:
            if db.executesql(query):
                return True
        except Exception as e:
            if not (
                DatabaseStoredFile.is_operational_error(db, e)
                or DatabaseStoredFile.is_programming_error(db, e)
            ):
                raise
            tb = traceback.format_exc()
            db.logger.error("Could not retrieve %s\n%s" % (filename, tb))
        return False
