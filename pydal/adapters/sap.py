import re
from .._compat import integer_types, long
from .base import SQLAdapter
from . import adapters


@adapters.register_for("sapdb")
class SAPDB(SQLAdapter):
    dbengine = "sapdb"
    drivers = ("sapdb",)

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])/(?P<db>[^?]+)$"
    )

    def _initialize_(self):
        super(SAPDB, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        if password is None:
            password = ""
        host = m.group("host")
        db = m.group("db")
        self.driver_args.update(user=user, password=password, database=db, host=host)

    def connector(self):
        self.driver.connect(**self.driver_args)

    def lastrowid(self, table):
        self.execute("select %s.NEXTVAL from dual" % table._sequence_name)
        return long(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        self.execute("CREATE SEQUENCE %s;" % table._sequence_name)
        self.execute(
            "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s');"
            % (table._rname, table._id._rname, table._sequence_name)
        )
        self.execute(query)
