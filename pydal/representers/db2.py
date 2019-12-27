import base64
import datetime
from ..adapters.db2 import DB2
from .base import SQLRepresenter
from . import representers


@representers.register_for(DB2)
class DB2Representer(SQLRepresenter):
    def exceptions(self, obj, field_type):
        if field_type == "blob":
            obj = base64.b64encode(str(obj))
            return "BLOB('%s')" % obj
        elif field_type == "datetime":
            if isinstance(obj, datetime.datetime):
                obj = obj.isoformat()[:19].replace("T", "-").replace(":", ".")
            elif isinstance(obj, datetime.date):
                obj = obj.isoformat()[:10] + "-00.00.00"
            return "'%s'" % obj
        return None
