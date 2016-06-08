import datetime
from .._compat import PY2, basestring, to_bytes
from ..adapters.mongo import Mongo, MongoBlob
from ..helpers.classes import Reference
from ..objects import Row
from .base import NoSQLRepresenter
from . import representers, for_type


@representers.register_for(Mongo)
class MongoRepresenter(NoSQLRepresenter):
    @for_type('id')
    def _id(self, value):
        return self.adapter.object_id(value)

    @for_type('reference')
    def _reference(self, value):
        if isinstance(value, (Row, Reference)):
            value = value['id']
        return self.adapter.object_id(value)

    @for_type('date')
    def _date(self, value):
        # this piece of data can be stripped off based on the fieldtype
        t = datetime.time(0, 0, 0)
        # mongodb doesn't have a date object and so it must datetime,
        # string or integer
        return datetime.datetime.combine(value, t)

    @for_type('time')
    def _time(self, value):
        # this piece of data can be stripped off based on the fieldtype
        d = datetime.date(2000, 1, 1)
        # mongodb doesn't have a time object and so it must datetime,
        # string or integer
        return datetime.datetime.combine(d, value)

    @for_type('datetime')
    def _datetime(self, value):
        return value

    @for_type('blob')
    def _blob(self, value):
        if isinstance(value, basestring) and value == '':
            value = None
        return MongoBlob(value) if PY2 else to_bytes(value)

    @for_type('list:reference')
    def _list_reference(self, value):
        values = self._represent_list(value)
        return list(map(self.adapter.object_id, values))
