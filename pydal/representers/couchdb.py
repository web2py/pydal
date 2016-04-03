from .._compat import integer_types
from ..adapters.couchdb import CouchDB
from ..helpers.classes import Reference
from ..helpers.serializers import serializers
from ..objects import Row
from .base import NoSQLRepresenter
from . import representers, for_type

long = integer_types[-1]


@representers.register_for(CouchDB)
class CouchDBRepresenter(NoSQLRepresenter):
    def adapt(self, value):
        return repr(not isinstance(value, unicode) and value or
                    value and value.encode('utf8'))

    @for_type('id')
    def _id(self, value):
        return str(long(value))

    @for_type('reference', adapt=False)
    def _reference(self, value):
        if isinstance(value, (Row, Reference)):
            value = value['id']
        return self.adapter.object_id(value)

    @for_type('date', adapt=False)
    def _date(self, value):
        return serializers.json(value)

    @for_type('time', adapt=False)
    def _time(self, value):
        serializers.json(value)

    @for_type('datetime', adapt=False)
    def _datetime(self, value):
        return serializers.json(value)

    @for_type('boolean', adapt=False)
    def _boolean(self, value):
        return serializers.json(value)
