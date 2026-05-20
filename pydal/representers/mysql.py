"""MySQL representer — SQL + JSON, no per-type overrides needed."""

from ..adapters.mysql import MySQL
from . import representers
from .base import JSONRepresenter, SQLRepresenter


@representers.register_for(MySQL)
class MySQLRepresenter(SQLRepresenter, JSONRepresenter):
    """Plain SQL + JSON representer. Inherits every type handler."""
