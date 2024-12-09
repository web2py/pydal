import base64

from ..adapters.google import Firestore
from ..helpers.classes import Reference
from . import before_parse, for_type, parsers
from .base import BasicParser, JSONParser


@parsers.register_for(Firestore)
class FirestoreParser(BasicParser, JSONParser):
    @for_type("id")
    def _id(self, value):
        return value

    @for_type("boolean")
    def _boolean(self, value):
        return value

    @for_type("id")
    def _id(self, value):
        if not isinstance(value, int):
            return int(value)
        return value

    @for_type("json")
    def _json(self, value):
        return value

    @for_type("blob")
    def _json(self, value):
        return value

    @before_parse("reference")
    def reference_extras(self, field_type):
        return {"referee": field_type[10:].strip()}

    @for_type("reference")
    def _reference(self, value, referee):
        if not isinstance(value, int):
            value = int(value)
        if "." not in referee:
            value = Reference(value)
            value._table, value._record = self.adapter.db[referee], None
        return value

    @before_parse("list:reference")
    def referencelist_extras(self, field_type):
        return {"field_type": field_type}

    @for_type("list:reference")
    def _list_references(self, value, field_type):
        return [self.registered["reference"](el, field_type[5:]) for el in value]

    @for_type("date")
    def _date(self, value):
        if isinstance(value, datetime):
            return value.date()
        return value

    @for_type("datetime")
    def _datetme(self, value):
        return value

    @for_type("time")
    def _time(self, value):
        if isinstance(value, datetime):
            return value.time()
        return value
