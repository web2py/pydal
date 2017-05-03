from ..adapters.oracle import Oracle
import json
from .base import BasicParser, ListsParser
from datetime import datetime, date, time, timedelta
from . import parsers, for_type


class OracleParser(BasicParser):
    @for_type('integer')
    def _integer(self, value):
        return int(value)

    @for_type('text')
    def _text(self, value):
        return value.read()

    @for_type('clob')
    def _clob(self, value):
        return value.read()

    @for_type('json')
    def _json(self, value):
        return json.loads(value.read())

    @for_type('date')
    def _date(self, value):
        if isinstance(value, datetime):
            return value.date()
        (y, m, d) = map(int, str(value)[:10].strip().split('-'))
        return date(y, m, d)

    @for_type('list:reference')
    def _list_references(self, value, field_type):
        return super(OracleParser, self)._list_references.f(self, value.read(), field_type)

class OracleListsParser(ListsParser):
    @for_type('list:integer')
    def _list_integers(self, value):
        return super(OracleListsParser, self)._list_integers.f(self, value.read())

    @for_type('list:string')
    def _list_strings(self, value):
        return super(OracleListsParser, self)._list_strings.f(self, value.read())


@parsers.register_for(Oracle)
class OracleCommonparser(
    OracleParser, OracleListsParser
):
    pass