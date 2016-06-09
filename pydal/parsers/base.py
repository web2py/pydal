import json
from base64 import b64decode
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from .._compat import PY2, integer_types, basestring, to_bytes, to_native
from ..adapters.base import SQLAdapter
from ..helpers.classes import Reference
from ..helpers.methods import bar_decode_string, bar_decode_integer
from . import Parser, parsers, for_type, before_parse

long = integer_types[-1]


class BasicParser(Parser):
    @for_type('id')
    def _id(self, value):
        return long(value)

    @for_type('integer')
    def _integer(self, value):
        return long(value)

    @for_type('float')
    def _float(self, value):
        return float(value)

    @for_type('double')
    def _double(self, value):
        return self.registered['float'](value, 'double')

    @for_type('boolean')
    def _boolean(self, value):
        return value == self.dialect.true or str(value)[:1].lower() == 't'

    @for_type('blob')
    def _blob(self, value):
        return to_native(b64decode(to_bytes(value)))

    @before_parse('reference')
    def reference_extras(self, field_type):
        return {'referee': field_type[10:].strip()}

    @for_type('reference')
    def _reference(self, value, referee):
        if '.' not in referee:
            value = Reference(value)
            value._table, value._record = self.adapter.db[referee], None
        return value

    @before_parse('list:reference')
    def referencelist_extras(self, field_type):
        return {'field_type': field_type}

    @for_type('list:reference')
    def _list_references(self, value, field_type):
        return [self.registered['reference'](
            el, field_type[5:]) for el in value]

    @for_type('bigint')
    def _bigint(self, value):
        return self.registered['integer'](value, 'bigint')


class DateParser(Parser):
    @for_type('date')
    def _date(self, value):
        if isinstance(value, datetime):
            return value.date()
        (y, m, d) = map(int, str(value)[:10].strip().split('-'))
        return date(y, m, d)


class TimeParser(Parser):
    @for_type('time')
    def _time(self, value):
        if isinstance(value, datetime):
            return value.time()
        time_items = list(map(int, str(value)[:8].strip().split(':')[:3]))
        if len(time_items) == 3:
            (h, mi, s) = time_items
        else:
            (h, mi, s) = time_items + [0]
        return time(h, mi, s)


class DateTimeParser(Parser):
    @for_type('datetime')
    def _datetime(self, value):
        value = str(value)
        date_part, time_part, timezone = value[:10], value[11:19], value[19:]
        if '+' in timezone:
            ms, tz = timezone.split('+')
            h, m = tz.split(':')
            dt = timedelta(seconds=3600 * int(h) + 60 * int(m))
        elif '-' in timezone:
            ms, tz = timezone.split('-')
            h, m = tz.split(':')
            dt = -timedelta(seconds=3600 * int(h) + 60 * int(m))
        else:
            ms = timezone.upper().split('Z')[0]
            dt = None
        (y, m, d) = map(int, date_part.split('-'))
        time_parts = time_part and time_part.split(':')[:3] or (0, 0, 0)
        while len(time_parts) < 3:
            time_parts.append(0)
        time_items = map(int, time_parts)
        (h, mi, s) = time_items
        if ms and ms[0] == '.':
            ms = int(float('0' + ms) * 1000000)
        else:
            ms = 0
        value = datetime(y, m, d, h, mi, s, ms)
        if dt:
            value = value + dt
        return value


class DecimalParser(Parser):
    @for_type('decimal')
    def _decimal(self, value):
        return Decimal(value)


class JSONParser(Parser):
    @for_type('json')
    def _json(self, value):
        #if 'loads' not in self.driver_auto_json:
        if not isinstance(value, basestring):
            raise RuntimeError('json data not a string')
        if PY2 and isinstance(value, unicode):
            value = value.encode('utf-8')
        return json.loads(value)


class ListsParser(BasicParser):
    @for_type('list:integer')
    def _list_integers(self, value):
        return bar_decode_integer(value)

    @for_type('list:string')
    def _list_strings(self, value):
        return bar_decode_string(value)

    @for_type('list:reference')
    def _list_references(self, value, field_type):
        value = bar_decode_integer(value)
        return [self.registered['reference'](
            el, field_type[5:]) for el in value]


@parsers.register_for(SQLAdapter)
class Commonparser(
    ListsParser, DateParser, TimeParser, DateTimeParser, DecimalParser,
    JSONParser
):
    pass
