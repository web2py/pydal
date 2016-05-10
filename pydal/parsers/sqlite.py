from decimal import Decimal
from ..adapters.sqlite import SQLite
from .base import ListsParser, TimeParser, JSONParser
from . import parsers, for_type, before_parse


@parsers.register_for(SQLite)
class SQLiteParser(ListsParser, TimeParser, JSONParser):
    @before_parse('decimal')
    def decimal_extras(self, field_type):
        return {'decimals': field_type[8:-1].split(',')[-1]}

    @for_type('decimal')
    def _decimal(self, value, decimals):
        value = ('%.' + decimals + 'f') % value
        return Decimal(value)
