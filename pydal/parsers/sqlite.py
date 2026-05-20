"""SQLite row parser — decimal precision, date/datetime fallbacks, list/JSON handlers."""

from datetime import date, datetime
from decimal import Decimal

from ..adapters.sqlite import SQLite
from . import before_parse, for_type, parsers
from .base import DateParser, DateTimeParser, JSONParser, ListsParser, TimeParser


@parsers.register_for(SQLite)
class SQLiteParser(ListsParser, DateParser, TimeParser, DateTimeParser, JSONParser):
    """
    SQLite parser composing list/date/time/datetime/JSON handlers with
    decimal-precision support extracted from the type string.

    SQLite has no native ``DECIMAL`` storage class — values arrive as
    floats and we re-quantize them at parse time to the field's
    declared precision.
    """

    @before_parse("decimal")
    def decimal_extras(self, field_type):
        """Pull the precision off a ``decimal(p,d)`` type string."""
        return {"decimals": field_type[8:-1].split(",")[1].strip()}

    @for_type("decimal")
    def _decimal(self, value, decimals):
        """Quantize the driver-supplied float to ``decimals`` places."""
        value = "{0:.{precision}f}".format(value, precision=decimals)
        return Decimal(value)

    @for_type("date")
    def _date(self, value):
        """Fall back to string parsing if the driver didn't decode a date."""
        if not isinstance(value, date):
            return DateParser._declared_parsers_["_date"].f(self, value)
        return value

    @for_type("datetime")
    def _datetime(self, value):
        """Fall back to string parsing if the driver didn't decode a datetime."""
        if not isinstance(value, datetime):
            return DateTimeParser._declared_parsers_["_datetime"].f(self, value)
        return value
