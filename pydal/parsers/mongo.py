from datetime import datetime

from .._compat import PY2, integer_types
from ..adapters.mongo import Mongo, MongoBlob
from ..helpers.classes import Reference
from . import Parser, before_parse, for_type, parsers

long = integer_types[-1]


@parsers.register_for(Mongo)
class MongoParser(Parser):
    @for_type("id")
    def _id(self, value):
        if isinstance(value, self.adapter.ObjectId):
            return int(str(value), 16)
        return int(value)

    @for_type("blob")
    def _blob(self, value):
        return MongoBlob.decode(value) if PY2 else value

    @before_parse("reference")
    def reference_extras(self, field_type):
        return {"referee": field_type[10:].strip()}

    @for_type("reference")
    def _reference(self, value, referee):
        if isinstance(value, self.adapter.ObjectId):
            value = int(str(value), 16)
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

    @for_type("time")
    def _time(self, value):
        if isinstance(value, datetime):
            return value.time()
        return value
