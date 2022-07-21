from ..adapters.mysql import MySQL
from . import representers
from .base import JSONRepresenter, SQLRepresenter


@representers.register_for(MySQL)
class MySQLRepresenter(SQLRepresenter, JSONRepresenter):
    pass
