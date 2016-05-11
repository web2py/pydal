import base64
import datetime
from ..adapters.oracle import Oracle
from .base import SQLRepresenter
from . import representers


@representers.register_for(Oracle)
class OracleRepresenter(SQLRepresenter):
    def exceptions(self, obj, field_type):
        if field_type == 'blob':
            obj = base64.b64encode(str(obj))
            return ":CLOB('%s')" % obj
        elif field_type == 'date':
            if isinstance(obj, (datetime.date, datetime.datetime)):
                obj = obj.isoformat()[:10]
            else:
                obj = str(obj)
            return "to_date('%s','yyyy-mm-dd')" % obj
        elif field_type == 'datetime':
            if isinstance(obj, datetime.datetime):
                obj = obj.isoformat()[:19].replace('T', ' ')
            elif isinstance(obj, datetime.date):
                obj = obj.isoformat()[:10]+' 00:00:00'
            else:
                obj = str(obj)
            return "to_date('%s','yyyy-mm-dd hh24:mi:ss')" % obj
        return None
