"""
Base row parsers — the reverse direction of representers.

A parser converts a driver-returned column value back into the
expected pydal Python type (``id`` → ``int``, ``date`` → ``date``,
``json`` → ``dict``, ...). Per-backend parsers compose mixins from
this module by inheriting the relevant ``*Parser`` classes.
"""

import json
from base64 import b64decode
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from ..adapters.base import SQLAdapter
from ..helpers.classes import Reference
from ..helpers.methods import bar_decode_integer, bar_decode_string
from ..utils import to_bytes, to_native
from . import Parser, before_parse, for_type, parsers


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
