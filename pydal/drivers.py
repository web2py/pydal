"""
Registry of available DB-API Python drivers.

Each backend's driver module is probed by ``try / except ImportError``
at module load time. Whatever's importable is added to ``DRIVERS`` so
``find_driver()`` in the adapter base can pick it up later.

Module-level constants exposed for adapters:

* ``DRIVERS`` — ``{driver_name: module}`` mapping of installed drivers.
* ``is_jdbc`` — True when the zxJDBC driver is importable (Jython).
* ``psycopg2_adapt`` — psycopg2's ``adapt`` callable, or None.
* ``cx_Oracle`` — the cx_Oracle module, or None.
* ``pyodbc`` — pyodbc (or pypyodbc fallback) module, or None.
* ``couchdb`` — couchdb module, or None.

The unconditional ``None`` assignments at the top guarantee the names
exist regardless of which drivers are present — a previous version
only set them inside one ``try`` branch, leaving them unbound when
that branch's import failed.
"""

from typing import Any, Dict, Optional

DRIVERS: Dict[str, Any] = {}

# Default sentinels — overwritten below if the relevant driver is
# present. Keeping them defined unconditionally avoids NameError when
# adapters do ``from ..drivers import psycopg2_adapt`` etc. on systems
# where the driver isn't installed.
psycopg2_adapt: Optional[Any] = None
cx_Oracle: Optional[Any] = None
pyodbc: Optional[Any] = None
couchdb: Optional[Any] = None
is_jdbc: bool = False


try:
    from google.cloud import firestore
    from google.cloud.firestore_v1.base_query import FieldFilter  # noqa: F401

    DRIVERS["firestore"] = firestore
except ImportError:
    pass

try:
    from pysqlite2 import dbapi2 as sqlite2

    DRIVERS["sqlite2"] = sqlite2
except ImportError:
    pass

try:
    from sqlite3 import dbapi2 as sqlite3

    DRIVERS["sqlite3"] = sqlite3
except ImportError:
    pass

try:
    import pymysql

    DRIVERS["pymysql"] = pymysql
except ImportError:
    pass

try:
    import snowflake.connector as snowflakeconnector

    DRIVERS["snowflakeconnector"] = snowflakeconnector
except ImportError:
    pass

try:
    import MySQLdb

    DRIVERS["MySQLdb"] = MySQLdb
except ImportError:
    pass

try:
    import mysql.connector as mysqlconnector

    DRIVERS["mysqlconnector"] = mysqlconnector
except ImportError:
    pass

try:
    import psycopg2
    from psycopg2.extensions import adapt as psycopg2_adapt

    DRIVERS["psycopg2"] = psycopg2
except ImportError:
    pass

try:
    import cx_Oracle  # noqa: F811

    DRIVERS["cx_Oracle"] = cx_Oracle
except ImportError:
    pass

try:
    import pyodbc  # noqa: F811

    DRIVERS["pyodbc"] = pyodbc
except ImportError:
    try:
        import pypyodbc as pyodbc  # noqa: F811

        DRIVERS["pyodbc"] = pyodbc
    except ImportError:
        pass

try:
    import ibm_db_dbi

    DRIVERS["ibm_db_dbi"] = ibm_db_dbi
except ImportError:
    pass

try:
    import Sybase

    DRIVERS["Sybase"] = Sybase
except ImportError:
    pass

try:
    import kinterbasdb

    DRIVERS["kinterbasdb"] = kinterbasdb
except ImportError:
    pass

try:
    import fdb

    DRIVERS["fdb"] = fdb
except ImportError:
    pass

try:
    import firebirdsql

    DRIVERS["firebirdsql"] = firebirdsql
except ImportError:
    pass

try:
    import informixdb

    DRIVERS["informixdb"] = informixdb
except ImportError:
    pass

try:
    import sapdb

    DRIVERS["sapdb"] = sapdb
except ImportError:
    pass

try:
    import cubriddb

    DRIVERS["cubriddb"] = cubriddb
except ImportError:
    pass

try:
    import java.sql
    from com.ziclix.python.sql import zxJDBC

    # The org.sqlite Jython driver is needed by java.sql for sqlite
    # over JDBC; the side-effect import is intentional.
    from org.sqlite import JDBC  # noqa: F401

    zxJDBC_sqlite = java.sql.DriverManager
    DRIVERS["zxJDBC"] = zxJDBC
    is_jdbc = True
except ImportError:
    pass

try:
    import couchdb  # noqa: F811

    DRIVERS["couchdb"] = couchdb
except ImportError:
    pass

try:
    import pymongo

    DRIVERS["pymongo"] = pymongo
except ImportError:
    pass

try:
    import imaplib

    DRIVERS["imaplib"] = imaplib
except ImportError:
    pass

try:
    import pytds

    DRIVERS["pytds"] = pytds
except ImportError:
    pass

try:
    import pymssql

    DRIVERS["pymssql"] = pymssql
except ImportError:
    pass

try:
    import mssql_python

    DRIVERS["mssql-python"] = mssql_python
except ImportError:
    pass
