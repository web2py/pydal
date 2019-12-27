import datetime
import decimal
import json as jsonlib
from .._compat import PY2, integer_types

long = integer_types[-1]


class Serializers(object):
    _custom_ = {}

    def _json_parse(self, o):
        if hasattr(o, "custom_json") and callable(o.custom_json):
            return o.custom_json()
        if isinstance(o, (datetime.date, datetime.datetime, datetime.time)):
            return o.isoformat()[:19].replace("T", " ")
        elif isinstance(o, long):
            return int(o)
        elif isinstance(o, decimal.Decimal):
            return str(o)
        elif isinstance(o, set):
            return list(o)
        elif hasattr(o, "as_list") and callable(o.as_list):
            return o.as_list()
        elif hasattr(o, "as_dict") and callable(o.as_dict):
            return o.as_dict()
        if self._custom_.get("json") is not None:
            return self._custom_["json"](o)
        raise TypeError(repr(o) + " is not JSON serializable")

    def __getattr__(self, name):
        if self._custom_.get(name) is not None:
            return self._custom_[name]
        raise NotImplementedError("No " + str(name) + " serializer available.")

    def json(self, value):
        value = jsonlib.dumps(value, default=self._json_parse)
        rep28 = r"\u2028"
        rep29 = r"\2029"
        if PY2:
            rep28 = rep28.decode("raw_unicode_escape")
            rep29 = rep29.decode("raw_unicode_escape")
        return value.replace(rep28, "\\u2028").replace(rep29, "\\u2029")

    def yaml(self, value):
        if self._custom_.get("yaml") is not None:
            return self._custom_.get("yaml")(value)
        try:
            from yaml import dump
        except ImportError:
            raise NotImplementedError("No yaml serializer available.")
        return dump(value)


serializers = Serializers()
