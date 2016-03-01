from ..adapters.postgres import Postgre
from .base import ListsParser, DateTimeParser, JSONParser
from . import parsers


@parsers.register_for(Postgre)
class PostgreParser(ListsParser, JSONParser):
    pass


class PostgreAutoJSONParser(ListsParser):
    pass
