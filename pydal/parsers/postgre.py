from ..adapters.postgres import Postgre, PostgreNew
from .base import BasicParser, ListsParser, JSONParser
from . import parsers


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
