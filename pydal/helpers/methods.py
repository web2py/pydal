# -*- coding: utf-8 -*-

"""
Free helper functions used across pydal.

This module collects small, self-contained helpers that don't belong
on a class:

* URI cleanup: ``hide_password``.
* Query DSL helpers: ``xorify``, ``use_common_filters``,
  ``merge_tablemaps``, ``smart_query``.
* List-of-* encoding: ``bar_encode`` / ``bar_decode_integer`` /
  ``bar_decode_string`` (and the underlying ``bar_escape`` /
  ``bar_unescape``).
* WKT geometry constructors: ``geoPoint``, ``geoLine``, ``geoPolygon``.
* Validator / represent defaults: ``auto_validators``,
  ``auto_represent``.
* Upload helpers: ``attempt_upload*``, ``delete_uploaded_files``.
* UUID helpers: ``uuidstr``, ``uuid2int``, ``int2uuid``.
* Archive: ``archive_record`` (record-versioning hook).
"""

import os
import re
import uuid
from functools import reduce
from io import BytesIO
from os.path import exists, join as pjoin
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union

from ..utils import to_bytes
from .classes import SQLCustomType
from .regex import REGEX_CONST_STRING, REGEX_CREDENTIALS, REGEX_UNPACK, REGEX_W

UNIT_SEPARATOR = "\x1f"  # ASCII unit separator for delimiting data


def hide_password(uri: Union[str, List[str]]) -> Union[str, List[str]]:
    """
    Mask the ``user:pass@`` portion of a DAL URI for safe logging.

    Accepts either a single URI or a list (replicated databases).
    """
    if isinstance(uri, (list, tuple)):
        return [hide_password(item) for item in uri]
    return re.sub(REGEX_CREDENTIALS, "******", uri)


def cleanup(text: str) -> str:
    """
    No-op identifier sanitizer.

    Historically this verified ``[0-9a-zA-Z_]`` only; the check is
    kept commented out in upstream pydal for backward compatibility
    and currently returns ``text`` unchanged.
    """
    return text


def list_represent(values: Optional[Iterable[Any]], row: Any = None) -> str:
    """
    Comma-join a list-of-* field's values for display.

    ``row`` is accepted but unused — preserved for compatibility with
    pydal's ``represent`` callback signature ``f(value, row=None)``.
    """
    return ", ".join(str(v) for v in (values or []))


def xorify(orderby: Optional[Sequence[Any]]) -> Any:
    """
    Reduce a sequence of orderby expressions to a single
    ``a | b | c | ...`` chain (which compiles to a comma-separated
    SQL clause).

    Returns ``None`` for empty input.
    """
    if not orderby:
        return None
    orderby2 = orderby[0]
    for item in orderby[1:]:
        orderby2 = orderby2 | item
    return orderby2


def use_common_filters(query: Any) -> bool:
    """True iff ``query`` exists and hasn't opted out of common filters."""
    return (
        query
        and hasattr(query, "ignore_common_filters")
        and not query.ignore_common_filters
    )


def merge_tablemaps(*maplist: Dict) -> Dict:
    """
    Merge ``{tablename: Table}`` maps, raising on name collisions.

    Two entries collide when they share the same name but reference
    *different* Table objects. Identical (same-identity) entries are
    fine — common when the same table appears in both the query and
    the field list.
    """
    maplist = list(maplist)
    for i, item in enumerate(maplist):
        if isinstance(item, dict):
            maplist[i] = dict(**item)
    ret = maplist[0]
    for item in maplist[1:]:
        if len(ret) > len(item):
            big, small = ret, item
        else:
            big, small = item, ret
        for key, val in small.items():
            if big.get(key, val) is not val:
                raise ValueError("Name conflict in table list: %s" % key)
        big.update(small)
        ret = big
    return ret


# ---------------------------------------------------------------------------
# list:* field encoding — pipe-delimited with ``||`` escaping.
# ---------------------------------------------------------------------------


def bar_escape(item: Any) -> str:
    """
    Escape a single ``list:*`` item for pipe-delimited storage.

    ``|`` becomes ``||``; leading/trailing ``||`` are flanked with the
    ASCII unit-separator so the surrounding pipes used as delimiters
    remain unambiguous.
    """
    item = str(item).replace("|", "||")
    if item.startswith("||"):
        item = "%s%s" % (UNIT_SEPARATOR, item)
    if item.endswith("||"):
        item = "%s%s" % (item, UNIT_SEPARATOR)
    return item


def bar_unescape(item: str) -> str:
    """Reverse of ``bar_escape``."""
    item = item.replace("||", "|")
    if item.startswith(UNIT_SEPARATOR):
        item = item[1:]
    if item.endswith(UNIT_SEPARATOR):
        item = item[:-1]
    return item


def bar_encode(items: Iterable[Any]) -> str:
    """Encode a list of items as ``|a|b|c|`` (with escapes)."""
    return "|%s|" % "|".join(bar_escape(item) for item in items if str(item).strip())


def bar_decode_integer(value: Any) -> List[int]:
    """
    Decode a ``|a|b|c|``-style integer list back to ``List[int]``.

    Accepts either a string or a file-like with ``read()``.
    """
    if not hasattr(value, "split") and hasattr(value, "read"):
        value = value.read()
    return [int(x) for x in value.split("|") if x.strip()]


def bar_decode_string(value: str) -> List[str]:
    """Decode a ``|a|b|c|``-style string list back to ``List[str]``."""
    return [bar_unescape(x) for x in re.split(REGEX_UNPACK, value[1:-1]) if x.strip()]


# ---------------------------------------------------------------------------
# Record archive (hook for record-versioning).
# ---------------------------------------------------------------------------


def archive_record(qset, fs, archive_table, current_record) -> bool:
    """
    Copy the rows matched by ``qset`` into ``archive_table`` whenever
    one of the about-to-change fields differs from the new value.

    Used by ``_enable_record_versioning`` as an ``_before_update``
    callback. Returns ``False`` so the surrounding update continues.
    """
    tablenames = qset.db._adapter.tables(qset.query)
    if len(tablenames) != 1:
        raise RuntimeError("cannot update join")
    for row in qset.select():
        fields = archive_table._filter_fields(row)
        for k, v in fs.items():
            if fields[k] != v:
                fields[current_record] = row.id
                archive_table.insert(**fields)
                break
    return False


# ---------------------------------------------------------------------------
# smart_query — natural-language predicate parser.
# ---------------------------------------------------------------------------


def smart_query(fields, text: str):
    """
    Parse a free-text predicate against a list of ``Field`` objects.

    Recognized vocabulary includes ``and``/``or``/``not``, comparison
    operators (``=``, ``<``, ``>``, ``<=``, ``>=``, ``!=``), and the
    word forms (``equal to``, ``not equal to``, ``less than``,
    ``starts with``, ``ends with``, ``in``, ``not in``, ``contains``).
    Quoted strings are protected from tokenization.

    Returns a pydal ``Query`` ready to pass to ``db(...)``. Raises
    ``RuntimeError`` on parse error.
    """
    from ..objects import Field, Table

    if not isinstance(fields, (list, tuple)):
        fields = [fields]
    new_fields = []
    for field in fields:
        if isinstance(field, Field):
            new_fields.append(field)
        elif isinstance(field, Table):
            for ofield in field:
                new_fields.append(ofield)
        else:
            raise RuntimeError("fields must be a list of fields")
    fields = new_fields
    field_map = {}
    for field in fields:
        n = field.name.lower()
        if n not in field_map:
            field_map[n] = field
        n = str(field).lower()
        if n not in field_map:
            field_map[n] = field
    constants: Dict[str, str] = {}
    i = 0
    while True:
        m = re.search(REGEX_CONST_STRING, text)
        if not m:
            break
        text = "%s#%i%s" % (text[: m.start()], i, text[m.end():])
        constants[str(i)] = m.group()[1:-1]
        i += 1
    text = re.sub(r"\s+", " ", text).lower()
    for a, b in [
        ("&", "and"),
        ("|", "or"),
        ("~", "not"),
        ("==", "="),
        ("<", "<"),
        (">", ">"),
        ("<=", "<="),
        (">=", ">="),
        ("<>", "!="),
        ("=<", "<="),
        ("=>", ">="),
        ("=", "="),
        (" less or equal than ", "<="),
        (" greater or equal than ", ">="),
        (" equal or less than ", "<="),
        (" equal or greater than ", ">="),
        (" less or equal ", "<="),
        (" greater or equal ", ">="),
        (" equal or less ", "<="),
        (" equal or greater ", ">="),
        (" not equal to ", "!="),
        (" not equal ", "!="),
        (" equal to ", "="),
        (" equal ", "="),
        (" equals ", "="),
        (" less than ", "<"),
        (" greater than ", ">"),
        (" starts with ", "startswith"),
        (" ends with ", "endswith"),
        (" not in ", "notbelongs"),
        (" in ", "belongs"),
        (" is ", "="),
    ]:
        if a[0] == " ":
            text = text.replace(" is" + a, " %s " % b)
        text = text.replace(a, " %s " % b)
    text = re.sub(r"\s+", " ", text).lower()
    text = re.sub(r"(?P<a>[\<\>\!\=])\s+(?P<b>[\<\>\!\=])", r"\g<a>\g<b>", text)
    query = field = neg = op = logic = None
    for item in text.split():
        if field is None:
            if item == "not":
                neg = True
            elif not neg and not logic and item in ("and", "or"):
                logic = item
            elif item in field_map:
                field = field_map[item]
            else:
                raise RuntimeError("Invalid syntax")
        elif field is not None and op is None:
            op = item
        elif op is not None:
            if item.startswith("#"):
                if item[1:] not in constants:
                    raise RuntimeError("Invalid syntax")
                value = constants[item[1:]]
            else:
                value = item
                if field.type in ("text", "string", "json"):
                    if op == "=":
                        op = "like"
            if op == "=":
                new_query = field == value
            elif op == "<":
                new_query = field < value
            elif op == ">":
                new_query = field > value
            elif op == "<=":
                new_query = field <= value
            elif op == ">=":
                new_query = field >= value
            elif op == "!=":
                new_query = field != value
            elif op == "belongs":
                new_query = field.belongs(value.split(","))
            elif op == "notbelongs":
                new_query = ~field.belongs(value.split(","))
            elif field.type == "list:string":
                if op == "contains":
                    new_query = field.contains(value)
                else:
                    raise RuntimeError("Invalid operation")
            elif field.type in ("text", "string", "json", "upload"):
                if op == "contains":
                    new_query = field.contains(value)
                elif op == "like":
                    new_query = field.ilike(value)
                elif op == "startswith":
                    new_query = field.startswith(value)
                elif op == "endswith":
                    new_query = field.endswith(value)
                else:
                    raise RuntimeError("Invalid operation")
            elif field._db._adapter.dbengine == "firestore" and field.type in (
                "list:integer",
                "list:string",
                "list:reference",
            ):
                if op == "contains":
                    new_query = field.contains(value)
                else:
                    raise RuntimeError("Invalid operation")
            else:
                raise RuntimeError("Invalid operation")
            if neg:
                new_query = ~new_query
            if query is None:
                query = new_query
            elif logic == "and":
                query &= new_query
            elif logic == "or":
                query |= new_query
            field = op = neg = logic = None
    return query


# ---------------------------------------------------------------------------
# Default validators / represent resolution.
# ---------------------------------------------------------------------------


def auto_validators(field) -> List[Any]:
    """
    Resolve the validator list for ``field``.

    Tries, in order:

    1. ``field.type.validator`` if the type is a ``SQLCustomType`` and
       defines one.
    2. ``db.validators_method(field)`` if the DAL has a callable
       ``validators_method`` (the default is ``default_validators``).
    3. ``db.validators[field_type]`` lookup.
    4. ``[]`` as a final fallback.
    """
    db = field.db
    field_type = field.type
    if isinstance(field_type, SQLCustomType):
        if hasattr(field_type, "validator"):
            return field_type.validator
        field_type = field_type.type
    elif not isinstance(field_type, str):
        return []
    if callable(db.validators_method):
        return db.validators_method(field)
    if not db.validators or not isinstance(db.validators, dict):
        return []
    field_validators = db.validators.get(field_type, [])
    if not isinstance(field_validators, (list, tuple)):
        field_validators = [field_validators]
    return field_validators


def _fieldformat(r, id: Any) -> str:
    """Render row ``r(id)`` via ``r._format`` (string or callable)."""
    row = r(id)
    if not row:
        return str(id)
    if hasattr(r, "_format") and isinstance(r._format, str):
        return r._format % row
    if hasattr(r, "_format") and callable(r._format):
        return r._format(row)
    return str(id)


class _repr_ref:
    """Callable that turns a reference id into its formatted display."""

    def __init__(self, ref=None):
        self.ref = ref

    def __call__(self, value, row=None):
        return value if value is None else _fieldformat(self.ref, value)


class _repr_ref_list(_repr_ref):
    """
    Callable that turns a list-of-references value into a
    comma-separated display string. Firestore's IN-list limit is
    handled by chunking lookups into groups of 30.
    """

    def __call__(self, value, row=None):
        if not value:
            return None
        db, id_ = self.ref._db, self.ref._id
        if db._adapter.dbengine == "firestore":
            # Firestore caps IN-queries at 30 values per call; chunk
            # the lookups and combine results.
            def count(values):
                return db(id_.belongs(values)).select(id_)

            rx = range(0, len(value), 30)
            refs = reduce(
                lambda a, b: a & b,
                [count(value[i: i + 30]) for i in rx],
            )
        else:
            refs = db(id_.belongs(value)).select(id_)
        return refs and ", ".join(_fieldformat(self.ref, x) for x in value) or ""


def auto_represent(field) -> Optional[Callable]:
    """
    Default ``represent`` callback for a field.

    Honors an explicit ``field.represent`` when set. Otherwise, for
    ``reference table`` / ``list:reference table`` fields whose target
    is a known DAL table, returns a callable that formats values via
    the target's ``_format``.
    """
    if field.represent:
        return field.represent
    if (
        field.db
        and field.type.startswith("reference")
        and field.type.find(".") < 0
        and field.type[10:] in field.db.tables
    ):
        referenced = field.db[field.type[10:]]
        return _repr_ref(referenced)
    if (
        field.db
        and field.type.startswith("list:reference")
        and field.type.find(".") < 0
        and field.type[15:] in field.db.tables
    ):
        referenced = field.db[field.type[15:]]
        return _repr_ref_list(referenced)
    return field.represent


def varquote_aux(name: str, quotestr: str = "%s") -> str:
    """
    Quote ``name`` only if it doesn't look like a simple identifier.

    Bare ``\\w+`` strings pass through unchanged; anything else is
    wrapped via ``quotestr``.
    """
    return name if REGEX_W.match(name) else quotestr % name


# ---------------------------------------------------------------------------
# UUID helpers.
# ---------------------------------------------------------------------------


def uuidstr() -> str:
    """Return a fresh UUID4 as a string."""
    return str(uuid.uuid4())


def uuid2int(uuidv: str) -> int:
    """Convert a UUID string to its 128-bit integer representation."""
    return uuid.UUID(uuidv).int


def int2uuid(n: int) -> str:
    """Convert a 128-bit integer back to a UUID string."""
    return str(uuid.UUID(int=n))


# ---------------------------------------------------------------------------
# GIS / WKT constructors — used by ``Field('...', 'geometry')`` writes.
# ---------------------------------------------------------------------------


def geoPoint(x: float, y: float) -> str:
    """Build a WKT POINT literal: ``POINT (x y)``."""
    return "POINT (%f %f)" % (x, y)


def geoLine(*line) -> str:
    """Build a WKT LINESTRING from a sequence of ``(x, y)`` pairs."""
    return "LINESTRING (%s)" % ",".join("%f %f" % item for item in line)


def geoPolygon(*line) -> str:
    """Build a WKT POLYGON from a sequence of ``(x, y)`` vertices."""
    return "POLYGON ((%s))" % ",".join("%f %f" % item for item in line)


# ---------------------------------------------------------------------------
# Upload field helpers.
# ---------------------------------------------------------------------------


def attempt_upload(table, fields: Dict[str, Any]) -> None:
    """
    Resolve any pending file uploads in ``fields`` by calling
    ``field.store(...)`` and substituting the resulting on-disk
    filename back into ``fields``.

    Recognizes:

    * file-like objects with ``.file`` and ``.filename`` (CGI uploads),
    * ``dict(data=..., filename=...)`` (JSON-style uploads),
    * file-like objects with ``.read`` and ``.name`` (open files).
    """
    for fieldname in table._upload_fieldnames & set(fields):
        value = fields[fieldname]
        if value is None or isinstance(value, str):
            continue
        if isinstance(value, bytes):
            continue
        if hasattr(value, "file") and hasattr(value, "filename"):
            new_name = table[fieldname].store(value.file, filename=value.filename)
        elif isinstance(value, dict):
            if "data" in value and "filename" in value:
                stream = BytesIO(to_bytes(value["data"]))
                new_name = table[fieldname].store(stream, filename=value["filename"])
            else:
                new_name = None
        elif hasattr(value, "read") and hasattr(value, "name"):
            new_name = table[fieldname].store(value, filename=value.name)
        else:
            raise RuntimeError("Unable to handle upload")
        fields[fieldname] = new_name


def attempt_upload_on_insert(table) -> Callable[[Dict], None]:
    """Return a callback suitable for ``Table._before_insert``."""

    def wrapped(fields):
        return attempt_upload(table, fields)

    return wrapped


def attempt_upload_on_update(table) -> Callable[[Any, Dict], None]:
    """Return a callback suitable for ``Table._before_update``."""

    def wrapped(dbset, fields):
        return attempt_upload(table, fields)

    return wrapped


def delete_uploaded_files(dbset, upload_fields: Optional[Dict] = None) -> bool:
    """
    For each row matched by ``dbset``, delete the on-disk file
    referenced by any ``upload`` field whose ``autodelete`` is set.

    ``upload_fields`` (when provided) is a ``{name: new_value}`` map
    that tells which fields are being changed AND what the new value
    is — files matching the new value are kept rather than deleted.

    Returns ``False`` so it can be chained as a ``_before_delete`` /
    ``_before_update`` hook without aborting the operation.
    """
    table = dbset.db._adapter.tables(dbset.query).popitem()[1]
    if upload_fields:
        fields = list(upload_fields)
        # Include compute upload fields (e.g. thumbnails) so derived
        # files get cleaned up too.
        fields += [f for f in table.fields if table[f].compute is not None]
    else:
        fields = table.fields
    fields = [
        f
        for f in fields
        if table[f].type == "upload"
        and table[f].uploadfield is True
        and table[f].autodelete
    ]
    if not fields:
        return False
    for record in dbset.select(*[table[f] for f in fields]):
        for fieldname in fields:
            field = table[fieldname]
            oldname = record.get(fieldname, None)
            if not oldname:
                continue
            if (
                upload_fields
                and fieldname in upload_fields
                and oldname == upload_fields[fieldname]
            ):
                continue
            if field.custom_delete:
                field.custom_delete(oldname)
            else:
                uploadfolder = field.uploadfolder
                if not uploadfolder:
                    uploadfolder = pjoin(dbset.db._adapter.folder, "..", "uploads")
                if field.uploadseparate:
                    items = oldname.split(".")
                    uploadfolder = pjoin(
                        uploadfolder, "%s.%s" % (items[0], items[1]), items[2][:2]
                    )
                oldpath = pjoin(uploadfolder, oldname)
                if field.uploadfs:
                    oldname = str(oldname)
                    if field.uploadfs.exists(oldname):
                        field.uploadfs.remove(oldname)
                else:
                    if exists(oldpath):
                        os.unlink(oldpath)
    return False
