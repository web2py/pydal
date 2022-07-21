from ..adapters.postgres import Postgre, PostgreBoolean, PostgreNew
from . import for_type, parsers
from .base import BasicParser, JSONParser, ListsParser


@parsers.register_for(Postgre)
class PostgreParser(ListsParser, JSONParser):
    @for_type("jsonb")
    def _jsonb(self, value):
        return self.registered["json"](value, "json")


class PostgreAutoJSONParser(ListsParser):
    pass


@parsers.register_for(PostgreNew)
class PostgreNewParser(JSONParser):
    pass


class PostgreNewAutoJSONParser(BasicParser):
    pass


@parsers.register_for(PostgreBoolean)
class PostgreBooleanParser(JSONParser):
    @for_type("boolean")
    def _boolean(self, value):
        return value


class PostgreBooleanAutoJSONParser(BasicParser):
    @for_type("boolean")
    def _boolean(self, value):
        return value
