"""DB2 representer — overrides ``blob`` and ``datetime`` rendering."""

import base64
import datetime

from ..adapters.db2 import DB2
from ..utils import to_bytes
from . import representers
from .base import SQLRepresenter


@representers.register_for(DB2)
class DB2Representer(SQLRepresenter):
    """DB2-specific value rendering for ``blob`` and ``datetime``."""

    def exceptions(self, obj, field_type):
        """Render BLOBs via ``BLOB(...)`` and datetimes in DB2's dotted format."""
        if field_type == "blob":
            # DB2 wants base64-wrapped blobs; b64encode needs bytes input.
            obj = base64.b64encode(to_bytes(obj)).decode("ascii")
            return "BLOB('%s')" % obj
        if field_type == "datetime":
            if isinstance(obj, datetime.datetime):
                obj = obj.isoformat()[:19].replace("T", "-").replace(":", ".")
            elif isinstance(obj, datetime.date):
                obj = obj.isoformat()[:10] + "-00.00.00"
            return "'%s'" % obj
        return None
