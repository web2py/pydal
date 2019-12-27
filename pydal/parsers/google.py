from ..adapters.google import GoogleDatastore
from .base import BasicParser, JSONParser
from . import parsers, for_type


@parsers.register_for(GoogleDatastore)
class GoogleDatastoreParser(BasicParser, JSONParser):
    @for_type("id")
    def _id(self, value):
        return value

    @for_type("boolean")
    def _boolean(self, value):
        return value
