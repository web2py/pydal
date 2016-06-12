from ..adapters.postgres import Postgre, PostgreNew, PostgreBoolean
from .base import BasicParser, ListsParser, JSONParser
from . import parsers, for_type


@parsers.register_for(Postgre)
class PostgreParser(ListsParser, JSONParser):
    pass


class PostgreAutoJSONParser(ListsParser):
    pass


@parsers.register_for(PostgreNew)
class PostgreNewParser(JSONParser):
    pass


class PostgreNewAutoJSONParser(BasicParser):
    pass


@parsers.register_for(PostgreBoolean)
class PostgreBooleanParser(JSONParser):
    @for_type('boolean')
    def _boolean(self, value):
        return value


class PostgreBooleanAutoJSONParser(BasicParser):
    @for_type('boolean')
    def _boolean(self, value):
        return value
