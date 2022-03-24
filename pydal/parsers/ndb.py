from ..adapters.ndb import GoogleNDB
from .base import BasicParser, JSONParser
from . import parsers, for_type


@parsers.register_for(GoogleNDB)
class GoogleNDBParser(BasicParser, JSONParser):
    @for_type("id")
    def _id(self, value):
        return value

    @for_type("boolean")
    def _boolean(self, value):
        return value
