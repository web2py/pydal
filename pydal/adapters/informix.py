from . import adapters, with_connection_or_raise
from .base import SQLAdapter


@adapters.register_for("informix")
class Informix(SQLAdapter):
    dbengine = "informix"
    drivers = ("informixdb",)

    def _initialize_(self):
        super(Informix, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = self.REGEX_URI.match(ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        if not user:
            raise SyntaxError("User required")
        password = self.credential_decoder(m.group("password"))
        if not password:
            password = ""
        host = m.group("host")
        if not host:
            raise SyntaxError("Host name required")
        db = m.group("db")
        if not db:
            raise SyntaxError("Database name required")
        self.dsn = "%s@%s" % (db, host)
        self.driver_args.update(user=user, password=password)
        self.get_connection()

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)

    def _after_first_connection(self):
        self.dbms_version = int(self.connection.dbms_version.split(".")[0])

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        command = self.filter_sql_command(args[0])
        if command[-1:] == ";":
            command = command[:-1]
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def test_connection(self):
        self.execute("SELECT COUNT(*) FROM systables;")

    def lastrowid(self, table):
        return self.cursor.sqlerrd[1]


@adapters.register_for("informix-se")
class InformixSE(Informix):
    def rowslice(self, rows, minimum=0, maximum=None):
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]
