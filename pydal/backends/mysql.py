"""Backend module for mysql."""

# ============================================================
# Adapter
# ============================================================

import re

from ..utils import split_uri_args
from ..backend_base import adapters, with_connection
from ..backend_base import SQLAdapter


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

# ============================================================
# Dialect
# ============================================================

from ..helpers.methods import varquote_aux
from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(MySQL)
class MySQLDialect(SQLDialect):
    """
    MySQL dialect.

    Identifier quoting uses backticks. Big text/blob columns map to
    ``LONGTEXT`` / ``LONGBLOB``. Synthetic IDs use ``AUTO_INCREMENT``.
    ``regexp`` is a native operator; ``substring`` maps to ``SUBSTRING``;
    ``epoch`` to ``UNIX_TIMESTAMP``.

    ``DROP TABLE`` toggles ``FOREIGN_KEY_CHECKS`` around the statement
    so circular FKs don't block drops.
    """

    quote_template = "`%s`"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "DATETIME"

    @sqltype_for("text")
    def type_text(self):
        return "LONGTEXT"

    @sqltype_for("blob")
    def type_blob(self):
        return "LONGBLOB"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("id")
    def type_id(self):
        return "INT AUTO_INCREMENT NOT NULL"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT AUTO_INCREMENT NOT NULL"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INT %(null)s %(unique)s, INDEX %(index_name)s "
            + "(%(field_name)s), FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT %(null)s %(unique)s, INDEX %(index_name)s "
            + "(%(field_name)s), FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ", CONSTRAINT `FK_%(constraint_name)s` FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_key)s ON DELETE "
            + "%(on_delete_action)s"
        )

    def varquote(self, val):
        return varquote_aux(val, "`%s`")

    def insert_empty(self, table):
        return "INSERT INTO %s VALUES (DEFAULT);" % table

    def delete(self, table, where=None):
        tablename = self.writing_alias(table)
        whr = ""
        if where:
            whr = " %s" % self.where(where)
        return "DELETE %s FROM %s%s;" % (table.sql_shortref, tablename, whr)

    @property
    def random(self):
        return "RAND()"

    def substring(self, field, parameters, query_env={}):
        return "SUBSTRING(%s,%s,%s)" % (
            self.expand(field, query_env=query_env),
            parameters[0],
            parameters[1],
        )

    def epoch(self, first, query_env={}):
        return "UNIX_TIMESTAMP(%s)" % self.expand(first, query_env=query_env)

    def concat(self, *items, **kwargs):
        query_env = kwargs.get("query_env", {})
        tmp = (self.expand(x, "string", query_env=query_env) for x in items)
        return "CONCAT(%s)" % ",".join(tmp)

    def regexp(self, first, second, match_parameter=None, query_env={}):
        return "(%s REGEXP %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
        )

    def cast(self, first, second, query_env={}):
        if second == "LONGTEXT":
            second = "CHAR"
        return "CAST(%s AS %s)" % (first, second)

    def drop_table(self, table, mode):
        # breaks db integrity but without this mysql does not drop table
        return [
            "SET FOREIGN_KEY_CHECKS=0;",
            "DROP TABLE %s;" % table._rname,
            "SET FOREIGN_KEY_CHECKS=1;",
        ]

    def drop_index(self, name, table, if_exists=False):
        return "DROP INDEX %s ON %s;" % (self.quote(name), table._rname)

# ============================================================
# Representer
# ============================================================

from ..backend_base import representers
from ..backend_base import JSONRepresenter, SQLRepresenter


@representers.register_for(MySQL)
class MySQLRepresenter(SQLRepresenter, JSONRepresenter):
    """Plain SQL + JSON representer. Inherits every type handler."""

