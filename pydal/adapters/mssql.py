import re

from .._compat import PY2, integer_types, iteritems, long, to_unicode
from .._globals import IDENTITY
from ..utils import split_uri_args
from . import adapters, with_connection_or_raise
from .base import SQLAdapter


class Slicer(object):
    def rowslice(self, rows, minimum=0, maximum=None):
        if maximum is None:
            return rows[minimum:]
        return rows[minimum:maximum]


class MSSQL(SQLAdapter):
    dbengine = "mssql"
    drivers = ("pyodbc", "pytds", "pymssql", "mssql-python")

    REGEX_DSN = "^.+$"
    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?"
        "/(?P<db>[^?]+)"
        r"(\?(?P<uriargs>.*))?$"
    )

    def __init__(
        self,
        db,
        uri,
        pool_size=0,
        folder=None,
        db_codec="UTF-8",
        credential_decoder=IDENTITY,
        driver_args={},
        adapter_args={},
        srid=4326,
        after_connection=None,
    ):
        self.srid = srid
        super(MSSQL, self).__init__(
            db,
            uri,
            pool_size,
            folder,
            db_codec,
            credential_decoder,
            driver_args,
            adapter_args,
            after_connection,
        )

    def _initialize_(self):
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            self.dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group("user"))
            password = self.credential_decoder(m.group("password"))
            if password is None:
                password = ""
            host = m.group("host")
            db = m.group("db")
            port = m.group("port") or "1433"
            # Parse the optional uri name-value arg pairs after the '?'
            # (in the form of arg1=value1&arg2=value2&...)
            # (drivers like FreeTDS insist on uppercase parameter keys)
            argsdict = {"DRIVER": "{SQL Server}"}
            uriargs = m.group("uriargs")
            if uriargs:
                for argkey, argvalue in split_uri_args(
                    uriargs, separators="&", need_equal=True
                ).items():
                    argsdict[argkey.upper()] = argvalue
            uriargs = ";".join(["%s=%s" % (ak, av) for (ak, av) in iteritems(argsdict)])
            self.dsn = "SERVER=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;%s" % (
                host,
                port,
                db,
                user,
                password,
                uriargs,
            )

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)

    def lastrowid(self, table):
        self.execute("SELECT SCOPE_IDENTITY();")
        return int(self.cursor.fetchone()[0])


@adapters.register_for("mssql")
class MSSQL1(MSSQL, Slicer):
    pass


@adapters.register_for("mssql3")
class MSSQL3(MSSQL):
    pass


@adapters.register_for("mssql4")
class MSSQL4(MSSQL):
    pass


class MSSQLN(MSSQL):
    def represent(self, obj, field_type):
        rv = super(MSSQLN, self).represent(obj, field_type)
        if field_type in ("string", "text", "json") and rv.startswith("'"):
            rv = "N" + rv
        return rv

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        if PY2:
            args = list(args)
            args[0] = to_unicode(args[0])
        return super(MSSQLN, self).execute(*args, **kwargs)


@adapters.register_for("mssqln", "mssql2")
class MSSQL1N(MSSQLN, Slicer):
    pass


@adapters.register_for("mssql3n")
class MSSQL3N(MSSQLN):
    pass


@adapters.register_for("mssql4n")
class MSSQL4N(MSSQLN):
    pass


@adapters.register_for("pytds")
class PyTDS(MSSQL):
    def _initialize_(self):
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            self.dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            self.dsn = m.group("host")
            self.driver_args.update(
                user=self.credential_decoder(m.group("user")),
                password=self.credential_decoder(m.group("password")) or "",
                database=m.group("db"),
                port=m.group("port") or "1433",
            )

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)


@adapters.register_for("vertica")
class Vertica(MSSQL1):
    def lastrowid(self, table):
        self.execute("SELECT SCOPE_IDENTITY();")
        return int(self.cursor.fetchone()[0])


@adapters.register_for("sybase")
class Sybase(MSSQL1):
    dbengine = "sybase"

    def _initialize_(self):
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            user = self.credential_decoder(m.group("user"))
            password = self.credential_decoder(m.group("password"))
            if password is None:
                password = ""
            host = m.group("host")
            db = m.group("db")
            port = m.group("port") or "1433"
            self.dsn = "sybase:host=%s:%s;dbname=%s" % (host, port, db)
            self.driver_args.update(
                user=self.credential_decoder(user),
                passwd=self.credential_decoder(password),
            )

#Added support for Pymssql
@adapters.register_for("pymssql")
class PyMssql(MSSQL):
    def _initialize_(self):
        import pymssql 
        self.driver = pymssql
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        if "@" not in ruri:
            m = re.match(self.REGEX_DSN, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL")
            self.dsn = m.group()
        else:
            m = re.match(self.REGEX_URI, ruri)
            if not m:
                raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)
            self.dsn = m.group("host")
            self.driver_args.update(
                user=self.credential_decoder(m.group("user")),
                password=self.credential_decoder(m.group("password")) or "",
                database=m.group("db"),
                port=m.group("port") or "1433",
            )

    def connector(self):
        return self.driver.connect(self.dsn, **self.driver_args)


# Microsoft mssql-python driver support (SQL Server 2012+)
@adapters.register_for("mssqlpython")
class MSSQLPython(MSSQL4):
    """
    MSSQL adapter for Microsoft's official mssql-python driver.
    
    Supports SQL Server 2012+ with modern OFFSET/FETCH pagination.
    Features encryption-by-default and Microsoft Entra ID authentication.
    
    URI format:
        mssqlpython://user:password@server:port/database?param=value
    
    Supported URI parameters:
        - encrypt: Enable encryption (default: yes, values: yes/no/true/false/1/0)
        - trust_server_certificate: Trust self-signed certificates (default: no)
        - authentication: Authentication mode. Supported values:
            * ActiveDirectoryPassword: Username/password authentication
            * ActiveDirectoryInteractive: Interactive browser login
            * ActiveDirectoryMSI: Managed Identity (system/user-assigned)
            * ActiveDirectoryServicePrincipal: Service principal with client ID/secret
            * ActiveDirectoryIntegrated: Integrated Windows Auth/Kerberos
            * ActiveDirectoryDefault: Default authentication based on environment
            * ActiveDirectoryDeviceCode: Device code flow authentication
    
    Example URIs:
        # Standard SQL authentication with encryption
        mssqlpython://user:pass@server.database.windows.net:1433/mydb
        
        # Entra ID Interactive authentication
        mssqlpython://user@server.database.windows.net/mydb?authentication=ActiveDirectoryInteractive
        
        # Managed Identity (no user/password needed)
        mssqlpython://@server.database.windows.net/mydb?authentication=ActiveDirectoryMSI
        
        # Disable encryption (for testing/legacy)
        mssqlpython://user:pass@localhost:1433/mydb?encrypt=no&trust_server_certificate=yes
    """
    drivers = ("mssql-python",)  # Note: URI scheme is mssqlpython://, driver name is mssql-python
    
    # Override REGEX_URI to make user optional for Managed Identity scenarios
    REGEX_URI = (
        r"^(?P<user>[^:@]*)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?"
        r"/(?P<db>[^?]+)"
        r"(\?(?P<uriargs>.*))?$"
    )

    def _initialize_(self):
        import mssql_python
        self.driver = mssql_python
        # Skip MSSQL._initialize_() and call grandparent to avoid base MSSQL's REGEX
        super(MSSQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL: %s" % self.uri)

        # Parse URI query parameters
        uriargs = m.group("uriargs")
        uri_args = split_uri_args(uriargs, need_equal=True) if uriargs else {}

        # Extract basic connection parameters
        user = self.credential_decoder(m.group("user")) if m.group("user") else ""
        password = self.credential_decoder(m.group("password")) if m.group("password") else ""
        
        # Build driver connection arguments
        self.driver_args.update(
            server=m.group("host"),
            database=m.group("db"),
            port=int(m.group("port") or "1433"),
        )
        
        # Add credentials if provided (not required for some Entra ID auth modes)
        if user:
            self.driver_args["user"] = user
        if password:
            self.driver_args["password"] = password

        # Handle authentication parameter
        # Supported modes: ActiveDirectoryPassword, ActiveDirectoryInteractive,
        # ActiveDirectoryMSI, ActiveDirectoryServicePrincipal, 
        # ActiveDirectoryIntegrated, ActiveDirectoryDefault, ActiveDirectoryDeviceCode
        if "authentication" in uri_args:
            self.driver_args["authentication"] = uri_args["authentication"]

        # Handle encryption parameters (default: enabled for security)
        encrypt_param = uri_args.get("encrypt", "yes").lower()
        # mssql-python expects string values "yes"/"no", not boolean
        if encrypt_param in ("yes", "true", "1"):
            self.driver_args["encrypt"] = "yes"
        else:
            self.driver_args["encrypt"] = "no"

        # Handle trust_server_certificate parameter (default: disabled for security)
        if "trust_server_certificate" in uri_args:
            trust_param = uri_args["trust_server_certificate"].lower()
            # mssql-python expects string values "yes"/"no", not boolean
            if trust_param in ("yes", "true", "1"):
                self.driver_args["trust_server_certificate"] = "yes"
            else:
                self.driver_args["trust_server_certificate"] = "no"

        # Pass through any additional driver-specific parameters
        # Exclude already-handled parameters
        handled_params = {"authentication", "encrypt", "trust_server_certificate"}
        for key, value in uri_args.items():
            if key not in handled_params:
                self.driver_args[key] = value

    def connector(self):
        return self.driver.connect(**self.driver_args)