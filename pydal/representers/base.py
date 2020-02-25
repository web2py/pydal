import json
from base64 import b64encode
from datetime import date, time, datetime
from .._compat import PY2, integer_types, to_unicode, to_bytes, basestring
from ..adapters.base import SQLAdapter, NoSQLAdapter
from ..helpers.classes import Reference, SQLCustomType
from ..helpers.methods import bar_encode
from ..helpers.serializers import serializers
from ..objects import Row, Expression, Field
from . import Representer, representers, for_type, before_type, for_instance, pre

long = integer_types[-1]
NoneType = type(None)


class BaseRepresenter(Representer):
    @for_type("boolean", adapt=False)
    def _boolean(self, value):
        if value and not str(value)[:1].upper() in "0F":
            return self.adapter.smart_adapt(self.dialect.true)
        return self.adapter.smart_adapt(self.dialect.false)

    @for_type("id", adapt=False)
    def _id(self, value):
        return str(long(value))

    @for_type("integer", adapt=False)
    def _integer(self, value):
        return str(long(value))

    @for_type("decimal", adapt=False)
    def _decimal(self, value):
        return str(value)

    @for_type("double", adapt=False)
    def _double(self, value):
        return repr(float(value))

    @for_type("date", encode=True)
    def _date(self, value):
        if isinstance(value, (date, datetime)):
            return value.isoformat()[:10]
        return str(value)

    @for_type("time", encode=True)
    def _time(self, value):
        if isinstance(value, time):
            return value.isoformat()[:10]
        return str(value)

    @for_type("datetime", encode=True)
    def _datetime(self, value):
        if isinstance(value, datetime):
            value = value.isoformat(self.dialect.dt_sep)[:19]
        elif isinstance(value, date):
            value = value.isoformat()[:10] + self.dialect.dt_sep + "00:00:00"
        else:
            value = str(value)
        return value

    def _ensure_list(self, value):
        if not value:
            value = []
        elif not isinstance(value, (list, tuple)):
            value = [value]
        return value

    def _listify_elements(self, elements):
        return bar_encode(elements)

    @for_type("list:integer")
    def _list_integer(self, value):
        values = self._ensure_list(value)
        values = [int(val) for val in values if val != ""]
        return self._listify_elements(values)

    @for_type("list:string")
    def _list_string(self, value):
        value = self._ensure_list(value)
        if PY2:
            try:
                value = map(str, value)
            except:
                value = map(lambda x: unicode(x).encode(self.adapter.db_codec), value)
        else:
            value = list(map(str, value))
        return self._listify_elements(value)

    @for_type("list:reference", adapt=False)
    def _list_reference(self, value):
        return self.registered_t["list:integer"](value, "list:reference")


class JSONRepresenter(Representer):
    @for_type("json", encode=True)
    def _json(self, value):
        return serializers.json(value)


@representers.register_for(SQLAdapter)
class SQLRepresenter(BaseRepresenter):
    def _custom_type(self, value, field_type):
        value = field_type.encoder(value)
        if value and field_type.type in ("string", "text", "json"):
            return self.adapter.adapt(value)
        return value or "NULL"

    @pre()
    def _before_all(self, obj, field_type):
        if isinstance(field_type, SQLCustomType):
            return self._custom_type(obj, field_type)
        if obj == "" and not field_type[:2] in ("st", "te", "js", "pa", "up"):
            return "NULL"
        r = self.exceptions(obj, field_type)
        return r

    def exceptions(self, obj, field_type):
        return None

    @for_instance(NoneType)
    def _none(self, value, field_type):
        return "NULL"

    @for_instance(Expression)
    def _expression(self, value, field_type):
        return str(value)

    @for_instance(Field)
    def _fieldexpr(self, value, field_type):
        return str(value)

    @before_type("reference")
    def reference_extras(self, field_type):
        return {"referenced": field_type[9:].strip()}

    @for_type("reference", adapt=False)
    def _reference(self, value, referenced):
        if referenced in self.adapter.db.tables:
            return str(long(value))
        p = referenced.partition(".")
        if p[2] != "":
            try:
                ftype = self.adapter.db[p[0]][p[2]].type
                return self.adapter.represent(value, ftype)
            except (ValueError, KeyError):
                return repr(value)
        elif isinstance(value, (Row, Reference)):
            return str(value["id"])
        return str(long(value))

    @for_type("blob", encode=True)
    def _blob(self, value):
        return b64encode(to_bytes(value))


@representers.register_for(NoSQLAdapter)
class NoSQLRepresenter(BaseRepresenter):
    def adapt(self, value):
        return value

    @pre(is_breaking=True)
    def _before_all(self, obj, field_type):
        if isinstance(field_type, SQLCustomType):
            return True, field_type.encoder(obj)
        return False, obj

    @pre(is_breaking=True)
    def _nullify_empty_string(self, obj, field_type):
        if obj == "" and not (
            isinstance(field_type, str) and field_type[:2] in ("st", "te", "pa", "up")
        ):
            return True, None
        return False, obj

    @for_instance(NoneType)
    def _none(self, value, field_type):
        return None

    @for_instance(list, repr_type=True)
    def _repr_list(self, value, field_type):
        if isinstance(field_type, str) and not field_type.startswith("list:"):
            return [self.adapter.represent(v, field_type) for v in value]
        return value

    @for_type("id")
    def _id(self, value):
        return long(value)

    @for_type("integer")
    def _integer(self, value):
        return long(value)

    @for_type("bigint")
    def _bigint(self, value):
        return long(value)

    @for_type("double")
    def _double(self, value):
        return float(value)

    @for_type("reference")
    def _reference(self, value):
        if isinstance(value, (Row, Reference)):
            value = value["id"]
        return long(value)

    @for_type("boolean")
    def _boolean(self, value):
        if not isinstance(value, bool):
            if value and not str(value)[:1].upper() in "0F":
                return True
            return False
        return value

    @for_type("string")
    def _string(self, value):
        return to_unicode(value)

    @for_type("password")
    def _password(self, value):
        return to_unicode(value)

    @for_type("text")
    def _text(self, value):
        return to_unicode(value)

    @for_type("blob")
    def _blob(self, value):
        return value

    @for_type("json")
    def _json(self, value):
        if isinstance(value, basestring):
            value = to_unicode(value)
            value = json.loads(value)
        return value

    def _represent_list(self, value):
        items = self._ensure_list(value)
        return [item for item in items if item is not None]

    @for_type("date")
    def _date(self, value):
        if not isinstance(value, date):
            (y, m, d) = map(int, str(value).strip().split("-"))
            value = date(y, m, d)
        elif isinstance(value, datetime):
            (y, m, d) = (value.year, value.month, value.day)
            value = date(y, m, d)
        return value

    @for_type("time")
    def _time(self, value):
        if not isinstance(value, time):
            time_items = list(map(int, str(value).strip().split(":")[:3]))
            if len(time_items) == 3:
                (h, mi, s) = time_items
            else:
                (h, mi, s) = time_items + [0]
            value = time(h, mi, s)
        return value

    @for_type("datetime")
    def _datetime(self, value):
        if not isinstance(value, datetime):
            (y, m, d) = map(int, str(value)[:10].strip().split("-"))
            time_items = list(map(int, str(value)[11:].strip().split(":")[:3]))
            while len(time_items) < 3:
                time_items.append(0)
            (h, mi, s) = time_items
            value = datetime(y, m, d, h, mi, s)
        return value

    @for_type("list:integer")
    def _list_integer(self, value):
        values = self._represent_list(value)
        return list(map(int, values))

    @for_type("list:string")
    def _list_string(self, value):
        values = self._represent_list(value)
        return list(map(to_unicode, values))

    @for_type("list:reference")
    def _list_reference(self, value):
        values = self._represent_list(value)
        return list(map(long, values))
