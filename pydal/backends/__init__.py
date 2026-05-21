"""
Per-backend modules — adapter + dialect + parser + representer for each
supported database. Importing this package triggers registration of every
backend with the four dispatchers exposed in ``pydal.backend_base``.
"""

from . import (
    couchdb,
    db2,
    firebird,
    google,
    informix,
    ingres,
    mongo,
    mssql,
    mysql,
    oracle,
    postgres,
    sap,
    snowflake,
    sqlite,
    teradata,
)

# Re-export the dispatchers and most-commonly-referenced concrete adapters
# for backwards compatibility with code that did ``from pydal.adapters import``.
from ..backend_base import (
    adapters,
    dialects,
    parsers,
    representers,
)
from .couchdb import CouchDB
from .db2 import DB2
from .firebird import FireBird
from .google import GoogleSQL
from .informix import Informix
from .ingres import Ingres
from .mongo import Mongo
from .mssql import MSSQL
from .mysql import MySQL
from .oracle import Oracle
from .postgres import Postgres, PostgresPsyco
from .sap import SAPDB
from .snowflake import Snowflake
from .sqlite import SQLite
from .teradata import Teradata
