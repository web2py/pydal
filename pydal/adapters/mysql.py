"""MySQL adapter (and CUBRID variant)."""

import re

from ..utils import split_uri_args
from . import adapters, with_connection
from .base import SQLAdapter


@adapters.register_for("mysql:mysqlconnector")
@adapters.register_for("mysql:pymysql")
@adapters.register_for("mysql:MySQLdb")
@adapters.register_for("mysql")
class MySQL(SQLAdapter):
    """
    MySQL adapter — supports MySQLdb, pymysql, and mysql.connector.

    URI shape: ``mysql://user:pass@host:port/dbname?set_encoding=utf8&unix_socket=...``.
    Either ``host`` or ``unix_socket`` must be present.

    Distributed transactions are supported via XA. ALTER TABLE
    auto-commits (MySQL DDL is implicitly transactional). Conservative
    socket-level timeouts are installed by default for ``pymysql`` /
    ``MySQLdb`` so dead connections fail fast in a NAT/LB-fronted setup.
    """

    dbengine = "mysql"
    drivers = ("MySQLdb", "pymysql", "mysqlconnector")
    commit_on_alter_table = True
    support_distributed_transaction = True

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]*|\[[^\]]+\])(:(?P<port>\d+))?"
        "/(?P<db>[^?]+)"
        r"(\?(?P<uriargs>.*))?$"
    )  # set_encoding and unix_socket

    def _initialize_(self):
        """Parse URI components and prepare ``driver_args`` for ``connector``."""
        super(MySQL, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        host = m.group("host")
        uriargs = m.group("uriargs")
        if uriargs:
            uri_args = split_uri_args(uriargs, need_equal=True)
            charset = uri_args.get("set_encoding") or "utf8"
            socket = uri_args.get("unix_socket")
        else:
            charset = "utf8"
            socket = None
        # NOTE:
        # MySQLdb (see http://mysql-python.sourceforge.net/MySQLdb.html)
        # use UNIX sockets and named pipes by default if no host is given
        # or host is 'localhost'; as opposed to
        # pymysql (see https://pymysql.readthedocs.io/en/latest/modules/connections.html)
        # or mysqlconnector (see https://dev.mysql.com/doc/connectors/en/connector-python-connectargs.html)
        # driver, where you have to specify the socket explicitly.
        if not host and not socket:
            raise SyntaxError("Host or UNIX socket name required")
        db = m.group("db")
        port = int(m.group("port") or "3306")
        self.driver_args.update(user=user, db=db, charset=charset)
        if password is not None:
            self.driver_args["passwd"] = password
        if socket:
            self.driver_args["unix_socket"] = socket
        else:
            self.driver_args.update(host=host, port=port)
        # Safe socket-level timeout defaults so the pool-checkout SELECT 1
        # in test_connection can detect dead sockets instead of blocking
        # forever in recv() when a NAT/LB has silently evicted the entry.
        # Only applied if the user has not set them via driver_args.
        # mysqlconnector uses different parameter names, so skip it.
        if self.driver_name in ("pymysql", "MySQLdb"):
            self.driver_args.setdefault("connect_timeout", 10)
            self.driver_args.setdefault("read_timeout", 30)
            self.driver_args.setdefault("write_timeout", 30)

    def connector(self):
        """Open a MySQL connection; pop the optional ``cursor_buffered`` flag."""
        cursor_buffered = self.driver_args.get("cursor_buffered")
        if cursor_buffered:
            del self.driver_args["cursor_buffered"]
        conn = self.driver.connect(**self.driver_args)
        if cursor_buffered:
            conn.cursor = lambda conn=conn: conn.cursor(buffered=True)
        return conn

    def after_connection(self):
        """Enable FK checks and disable backslash escapes for safer literals."""
        self.execute("SET FOREIGN_KEY_CHECKS=1;")
        self.execute("SET sql_mode='NO_BACKSLASH_ESCAPES';")

    def distributed_transaction_begin(self, key):
        """Open an MySQL XA transaction branch."""
        self.execute("XA START;")

    @with_connection
    def prepare(self, key):
        """End and PREPARE this XA branch (phase 1 of 2PC)."""
        self.execute("XA END;")
        self.execute("XA PREPARE;")

    @with_connection
    def commit_prepared(self, key):
        """Commit the prepared XA branch (phase 2 of 2PC)."""
        self.execute("XA COMMIT;")

    @with_connection
    def rollback_prepared(self, key):
        """Roll back the prepared XA branch."""
        self.execute("XA ROLLBACK;")


@adapters.register_for("cubrid")
class Cubrid(MySQL):
    """CUBRID adapter — mostly MySQL-compatible; drops the ``charset`` arg."""

    dbengine = "cubrid"
    drivers = ("cubriddb",)

    def _initialize_(self):
        """Same as MySQL but the CUBRID driver doesn't accept ``charset=``."""
        super()._initialize_()
        del self.driver_args["charset"]
