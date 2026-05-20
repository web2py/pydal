"""
Generic JSON/YAML serialization with extensible custom encoders.

A single ``serializers`` instance is exposed at module level and used
throughout pydal (notably by ``Rows.as_json`` and various validators).

Custom encoders can be installed by assigning into
``Serializers._custom_``; lookups fall back to a registered encoder
named after the missing attribute (so ``serializers.xml`` resolves to
``serializers._custom_["xml"]`` when present).
"""

import datetime
import decimal
import json as jsonlib
from typing import Any, Callable, ClassVar, Dict


class Serializers:
    """
    Format-extensible serializer container.

    The default JSON encoder handles dates, times, decimals, sets,
    and objects exposing ``custom_json`` / ``as_list`` / ``as_dict``.
    Anything else triggers a ``TypeError`` unless a custom JSON
    encoder is registered under ``_custom_["json"]``.
    """

    _custom_: ClassVar[Dict[str, Callable[[Any], Any]]] = {}

    def _json_parse(self, o: Any) -> Any:
        """
        Default fallback encoder for ``json.dumps``.

        Tried in order:

        * ``o.custom_json()`` if defined.
        * ISO-format for date / datetime / time, with the ``T`` joiner
          replaced by a space and microseconds trimmed.
        * ``str(o)`` for Decimal.
        * ``list(o)`` for sets.
        * ``o.as_list()`` / ``o.as_dict()`` if defined.
        * A user-registered ``_custom_["json"]`` if present.
        * Otherwise, ``TypeError``.
        """
        if hasattr(o, "custom_json") and callable(o.custom_json):
            return o.custom_json()
        if isinstance(o, (datetime.date, datetime.datetime, datetime.time)):
            return o.isoformat()[:19].replace("T", " ")
        if isinstance(o, decimal.Decimal):
            return str(o)
        if isinstance(o, set):
            return list(o)
        if hasattr(o, "as_list") and callable(o.as_list):
            return o.as_list()
        if hasattr(o, "as_dict") and callable(o.as_dict):
            return o.as_dict()
        if self._custom_.get("json") is not None:
            return self._custom_["json"](o)
        raise TypeError(repr(o) + " is not JSON serializable")

    def __getattr__(self, name: str) -> Callable[[Any], str]:
        """
        Resolve missing attributes through ``_custom_``.

        ``serializers.xml(value)`` works iff ``_custom_["xml"]`` was
        installed; otherwise raises ``NotImplementedError``.
        """
        if self._custom_.get(name) is not None:
            return self._custom_[name]
        raise NotImplementedError("No " + str(name) + " serializer available.")

    def json(self, value: Any) -> str:
        """Serialize ``value`` to a JSON string using ``_json_parse``."""
        return jsonlib.dumps(value, default=self._json_parse)

    def yaml(self, value: Any) -> str:
        """
        Serialize ``value`` to YAML.

        Uses ``_custom_["yaml"]`` when registered, otherwise PyYAML's
        ``yaml.dump``. Raises ``NotImplementedError`` if neither is
        available.
        """
        if self._custom_.get("yaml") is not None:
            return self._custom_.get("yaml")(value)
        try:
            from yaml import dump
        except ImportError:
            raise NotImplementedError("No yaml serializer available.")
        return dump(value)


serializers = Serializers()
