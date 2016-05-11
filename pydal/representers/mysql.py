from ..adapters.mysql import MySQL
from .base import SQLRepresenter, JSONRepresenter
from . import representers


@representers.register_for(MySQL)
class MySQLRepresenter(SQLRepresenter, JSONRepresenter):
    pass
