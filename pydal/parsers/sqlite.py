from datetime import datetime, date
from decimal import Decimal
from ..adapters.sqlite import SQLite
from .base import ListsParser, DateParser, TimeParser, DateTimeParser, JSONParser
from . import parsers, for_type, before_parse


@parsers.register_for(SQLite)
class SQLiteParser(ListsParser, DateParser, TimeParser, DateTimeParser, JSONParser):
    @before_parse("decimal")
    def decimal_extras(self, field_type):
        return {"decimals": field_type[8:-1].split(",")[1].strip()}

    @for_type("decimal")
    def _decimal(self, value, decimals):
        value = "{0:.{precision}f}".format(value, precision=decimals)
        return Decimal(value)

    @for_type("date")
    def _date(self, value):
        if not isinstance(value, date):
            return DateParser._declared_parsers_["_date"].f(self, value)
        return value

    @for_type("datetime")
    def _datetime(self, value):
        if not isinstance(value, datetime):
            return DateTimeParser._declared_parsers_["_datetime"].f(self, value)
        return value
