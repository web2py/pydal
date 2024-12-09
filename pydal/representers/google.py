import base64

from .._compat import integer_types, to_unicode
from ..adapters.google import Firestore
from ..helpers.serializers import serializers
from . import for_type, pre, representers
from .base import NoSQLRepresenter

long = integer_types[-1]


@representers.register_for(Firestore)
class FirestoreRepresenter(NoSQLRepresenter):
    @pre(is_breaking=True)
    def _keep_lists_for_in_operator(self, obj, field_type):
        if (
            isinstance(field_type, str)
            and isinstance(obj, list)
            and not field_type.startswith("list:")
        ):
            return True, [self.adapter.represent(v, field_type) for v in obj]
        return False, obj

    @for_type("json")
    def _json(self, value):
        return value

    @for_type("blob")
    def _blob(self, value):
        return value

    @for_type("reference")
    def _reference(self, value):
        return str(value)

    @for_type("list:integer")
    def _list_integer(self, value):
        if not isinstance(value, list):
            return int(value)
        values = self._represent_list(value)
        return list(map(int, values))

    @for_type("list:string")
    def _list_string(self, value):
        if not isinstance(value, list):
            return str(value)
        values = self._represent_list(value)
        return list(map(to_unicode, values))

    @for_type("list:reference")
    def _list_reference(self, value):
        if not isinstance(value, list):
            return str(value)
        values = self._represent_list(value)
        return list(map(str, values))
