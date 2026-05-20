"""Oracle representer — blob/date/datetime wrappers (``to_date``, CLOB)."""

import datetime
from base64 import b64encode

from ..adapters.oracle import Oracle
from ..utils import to_bytes, to_native
from . import representers
from .base import JSONRepresenter, SQLRepresenter


@representers.register_for(Oracle)
class OracleRepresenter(SQLRepresenter, JSONRepresenter):
    """Oracle-specific value rendering for blob, date, datetime."""

    def exceptions(self, obj, field_type):
        """Render BLOB via ``:CLOB('base64...')`` and dates via ``to_date(...)``."""
        if field_type == "blob":
            if not isinstance(obj, bytes):
                obj = to_bytes(obj)
            obj = to_native(b64encode(obj))
            return ":CLOB('%s')" % obj
        if field_type == "date":
            if isinstance(obj, (datetime.date, datetime.datetime)):
                obj = obj.isoformat()[:10]
            else:
                obj = str(obj)
            return "to_date('%s','yyyy-mm-dd')" % obj
        if field_type == "datetime":
            if isinstance(obj, datetime.datetime):
                obj = obj.isoformat()[:19].replace("T", " ")
            elif isinstance(obj, datetime.date):
                obj = obj.isoformat()[:10] + " 00:00:00"
            else:
                obj = str(obj)
            return "to_date('%s','yyyy-mm-dd hh24:mi:ss')" % obj
        return None
