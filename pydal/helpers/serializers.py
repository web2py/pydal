import datetime
import decimal
import json as jsonlib


class Serializers(object):
    _custom_ = {}

    def _json_parse(self, o):
        if hasattr(o, 'custom_json') and callable(o.custom_json):
            return o.custom_json()
        if isinstance(o, (datetime.date,
                          datetime.datetime,
                          datetime.time)):
            return o.isoformat()[:19].replace('T', ' ')
        elif isinstance(o, long):
            return int(o)
        elif isinstance(o, decimal.Decimal):
            return str(o)
        elif isinstance(o, set):
            return list(o)
        elif hasattr(o, 'as_list') and callable(o.as_list):
            return o.as_list()
        elif hasattr(o, 'as_dict') and callable(o.as_dict):
            return o.as_dict()
        if self._custom_.get('json') is not None:
            return self._custom_['json'](o)
        raise TypeError(repr(o) + " is not JSON serializable")

    def __getattr__(self, name):
        if self._custom_.get(name) is not None:
            return self._custom_[name]
        raise NotImplementedError("No "+str(name)+" serializer available.")

    def json(self, value):
        value = jsonlib.dumps(value, default=self._json_parse)
        return value.replace(ur'\u2028', '\\u2028').replace(
            ur'\2029', '\\u2029')

    def yaml(self, value):
        if self._custom_.get('yaml') is not None:
            return self._custom_.get('yaml')(value)
        try:
            from yaml import dump
        except ImportError:
            raise NotImplementedError("No yaml serializer available.")
        return dump(value)


serializers = Serializers()
