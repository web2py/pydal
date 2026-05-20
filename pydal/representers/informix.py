"""Informix representer — wraps ``date`` / ``datetime`` in ``to_date(...)``."""

import datetime

from ..adapters.informix import Informix
from . import representers
from .base import SQLRepresenter


@representers.register_for(Informix)
class InformixRepresenter(SQLRepresenter):
    """Informix-specific date/datetime rendering using ``to_date()``."""

    def exceptions(self, obj, field_type):
        """Render dates/datetimes via Informix's ``to_date(value, format)``."""
        if field_type == "date":
            if isinstance(obj, (datetime.date, datetime.datetime)):
                obj = obj.isoformat()[:10]
            else:
                obj = str(obj)
            return "to_date('%s','%%Y-%%m-%%d')" % obj
        if field_type == "datetime":
            if isinstance(obj, datetime.datetime):
                obj = obj.isoformat()[:19].replace("T", " ")
            elif isinstance(obj, datetime.date):
                obj = obj.isoformat()[:10] + " 00:00:00"
            else:
                obj = str(obj)
            return "to_date('%s','%%Y-%%m-%%d %%H:%%M:%%S')" % obj
        return None
