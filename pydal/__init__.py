"""
pyDAL — a pure-Python database abstraction layer.

Public API exposed at the package root:

* ``DAL`` — the database connection / schema container.
* ``Field`` — a column descriptor used with ``DAL.define_table``.
* ``SQLCustomType`` — descriptor for backend-specific column types.
* ``QueryBuilder`` — natural-language → ``Query`` parser.
* ``QueryParseError`` — raised by ``QueryBuilder`` on parse failure.
* ``geoPoint`` / ``geoLine`` / ``geoPolygon`` — WKT geometry helpers.

See ``README.md`` for the full API tour.
"""

__version__ = "3.20260520.0"

from .base import DAL
from .helpers.classes import SQLCustomType
from .helpers.methods import geoLine, geoPoint, geoPolygon
from .objects import Field
from .querybuilder import QueryBuilder, QueryParseError

__all__ = [
    "DAL",
    "Field",
    "SQLCustomType",
    "QueryBuilder",
    "QueryParseError",
    "geoLine",
    "geoPoint",
    "geoPolygon",
    "__version__",
]
