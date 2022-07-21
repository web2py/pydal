import json
from base64 import b64decode
from datetime import date, datetime, time, timedelta

from .._compat import to_bytes, to_native
from ..adapters.oracle import Oracle
from . import for_type, parsers
from .base import BasicParser, ListsParser


class OracleParser(BasicParser):
    @for_type("integer")
    def _integer(self, value):
        return int(value)

    @for_type("text")
    def _text(self, value):
        return value

    @for_type("clob")
    def _clob(self, value):
        return value

    @for_type("blob")
    def _blob(self, value):
        # decoded = b64decode(value.read())
        decoded = b64decode(to_bytes(value))
        try:
            decoded = to_native(decoded)
        except:
            pass
        return decoded

    @for_type("json")
    def _json(self, value):
        return json.loads(value)

    @for_type("date")
    def _date(self, value):
        if isinstance(value, datetime):
            return value.date()
        (y, m, d) = map(int, str(value)[:10].strip().split("-"))
        return date(y, m, d)

    @for_type("list:reference")
    def _list_references(self, value, field_type):
        return super(OracleParser, self)._list_references.f(self, value, field_type)


class OracleListsParser(ListsParser):
    @for_type("list:integer")
    def _list_integers(self, value):
        return super(OracleListsParser, self)._list_integers.f(self, value)

    @for_type("list:string")
    def _list_strings(self, value):
        return super(OracleListsParser, self)._list_strings.f(self, value)


@parsers.register_for(Oracle)
class OracleCommonparser(OracleParser, OracleListsParser):
    pass
