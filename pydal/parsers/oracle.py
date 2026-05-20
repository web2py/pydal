"""Oracle row parsers — integer/text/clob/json/date + list:* handlers."""

import json
from datetime import date, datetime

from ..adapters.oracle import Oracle
from . import for_type, parsers
from .base import BasicParser, ListsParser


class OracleParser(BasicParser):
    """
    Oracle parser handling integer, text, CLOB, JSON, date, and
    list-of-reference columns. cx_Oracle returns these in shapes that
    need extra coercion compared to other drivers.
    """

    @for_type("integer")
    def _integer(self, value):
        """cx_Oracle returns Decimal for INTEGER; coerce to int."""
        return int(value)

    @for_type("text")
    def _text(self, value):
        """Pass text through unchanged (CLOB is handled separately)."""
        return value

    @for_type("clob")
    def _clob(self, value):
        """Pass CLOB content through unchanged."""
        return value

    @for_type("json")
    def _json(self, value):
        """Parse Oracle JSON columns (stored as CLOB) via ``json.loads``."""
        return json.loads(value)

    @for_type("date")
    def _date(self, value):
        """Convert Oracle's datetime-as-date back to a real ``date``."""
        if isinstance(value, datetime):
            return value.date()
        (y, m, d) = map(int, str(value)[:10].strip().split("-"))
        return date(y, m, d)

    @for_type("list:reference")
    def _list_references(self, value, field_type):
        """Delegate to the inherited list:reference handler."""
        return super(OracleParser, self)._list_references.f(self, value, field_type)


class OracleListsParser(ListsParser):
    """Oracle-flavoured list-of-integer and list-of-string parsers."""

    @for_type("list:integer")
    def _list_integers(self, value):
        """Delegate to the inherited list:integer handler."""
        return super(OracleListsParser, self)._list_integers.f(self, value)

    @for_type("list:string")
    def _list_strings(self, value):
        """Delegate to the inherited list:string handler."""
        return super(OracleListsParser, self)._list_strings.f(self, value)


@parsers.register_for(Oracle)
class OracleCommonparser(OracleParser, OracleListsParser):
    """Composite Oracle parser combining basic and list-type handlers."""
