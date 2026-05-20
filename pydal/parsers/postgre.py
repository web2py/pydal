"""PostgreSQL row parsers — JSON, JSONB, boolean, list-type fields."""

from ..adapters.postgres import Postgre, PostgreBoolean, PostgreNew
from . import for_type, parsers
from .base import BasicParser, JSONParser, ListsParser


@parsers.register_for(Postgre)
class PostgreParser(ListsParser, JSONParser):
    """Postgres parser with ``jsonb`` reusing the ``json`` parser."""

    @for_type("jsonb")
    def _jsonb(self, value):
        """Parse ``jsonb`` columns the same way as ``json``."""
        return self.registered["json"](value, "json")


class PostgreAutoJSONParser(ListsParser):
    """Variant for drivers that auto-decode JSON; no JSON parsing needed."""


@parsers.register_for(PostgreNew)
class PostgreNewParser(JSONParser):
    """Parser for newer Postgres adapters (no separate list/array handling)."""


class PostgreNewAutoJSONParser(BasicParser):
    """Newer Postgres + auto-JSON-decoding driver."""


@parsers.register_for(PostgreBoolean)
class PostgreBooleanParser(JSONParser):
    """
    Postgres parser that trusts the driver's native boolean conversion.

    The generic JSONParser would re-route booleans through string
    coercion; pass values through unchanged here.
    """

    @for_type("boolean")
    def _boolean(self, value):
        """Pass the driver's already-decoded boolean through unchanged."""
        return value


class PostgreBooleanAutoJSONParser(BasicParser):
    """Postgres native boolean + auto-JSON driver — same passthrough."""

    @for_type("boolean")
    def _boolean(self, value):
        """Pass driver-native booleans through."""
        return value
