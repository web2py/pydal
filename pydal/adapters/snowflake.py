import re
import os.path
from .._compat import PY2, with_metaclass, iterkeys, to_unicode, long
from .._globals import IDENTITY, THREAD_LOCAL
from .base import SQLAdapter
from ..utils import split_uri_args
from . import AdapterMeta, adapters, with_connection, with_connection_or_raise

try:
    from ..drivers import snowflakeconnector
except ImportError:
    snowflakeconnector = None

@adapters.register_for("snowflake")
class Snowflake(SQLAdapter):
    dbengine = "snowflake"
    drivers = ("snowflakeconnector",)

    REGEX_URI = (
        "(?P<user>[^:]+):(?P<password>[^:]+):"
        "(?P<warehouse>[^:@]+)(:(?P<account>[^@]*))?"
        r"@(?P<schema>[^:/]+|\[[^\]]+\])/(?P<db>[^?]+)$"
    )

    def _initialize_(self):
        super(Snowflake, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        if password is None:
            password = ""
        account = m.group("account")
        schema = m.group("schema")
        warehouse = m.group("warehouse")

        db = m.group("db")
        self.driver_args.update(user=user, password=password, database=db, account=account, schema=schema, warehouse=warehouse)


    def connector(self):
        return self.driver.connect(**self.driver_args)

    #def after_connection(self):
        #self.execute("SET CLIENT_ENCODING TO 'UTF8'")
        #self.execute("SET standard_conforming_strings=on;")

    def lastrowid(self, table):
        if self._last_insert:
            return long(self.cursor.fetchone()[0])
        sequence_name = table._sequence_name
        self.execute("SELECT currval(%s);" % self.adapt(sequence_name))
        return long(self.cursor.fetchone()[0])

    def _insert(self, table, fields):
        self._last_insert = None
        if fields:
            #retval = None
            if hasattr(table, "_id"):
                self._last_insert = (table._id, 1)
                #retval = table._id._rname
            return self.dialect.insert(
                table._rname,
                ",".join(el[0]._rname for el in fields),
                ",".join(self.expand(v, f.type) for f, v in fields),
            )
        return self.dialect.insert_empty(table._rname)



    @with_connection
    def prepare(self, key):
        self.execute("PREPARE TRANSACTION '%s';" % key)

    @with_connection
    def commit_prepared(self, key):
        self.execute("COMMIT PREPARED '%s';" % key)

    @with_connection
    def rollback_prepared(self, key):
        self.execute("ROLLBACK PREPARED '%s';" % key)
