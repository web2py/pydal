"""Backend module for firebird."""

# ============================================================
# Adapter
# ============================================================

import re

from ..backend_base import adapters
from ..backend_base import SQLAdapter


@adapters.register_for("firebird")
class FireBird(SQLAdapter):
    """
    FireBird adapter.

    Drivers (tried in order): ``kinterbasdb``, ``firebirdsql``,
    ``fdb``, ``pyodbc``.

    Distributed transactions are supported; ALTER TABLE auto-commits
    (FireBird DDL is implicitly transactional).

    URI shape: ``firebird://user:pass@host:port/db?set_encoding=UTF8``.
    Synthetic IDs come from per-table generators; an INSERT trigger
    populates the id column via ``gen_id()`` when not supplied.
    """

    dbengine = "firebird"
    drivers = ("kinterbasdb", "firebirdsql", "fdb", "pyodbc")

    support_distributed_transaction = True
    commit_on_alter_table = True

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])(:(?P<port>\d+))?"
        "/(?P<db>[^?]+)"
        r"(\?set_encoding=(?P<charset>\w+))?$"
    )

    def _initialize_(self):
        """Parse the URI, build the ``host/port:db`` DSN, stash in driver_args."""
        super()._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        if password is None:
            password = ""
        host = m.group("host")
        db = self.credential_decoder(m.group("db"))
        port = int(m.group("port") or 3050)
        charset = m.group("charset") or "UTF8"
        self.driver_args.update(
            dsn="%s/%s:%s" % (host, port, db),
            user=user,
            password=password,
            charset=charset,
        )

    def connector(self):
        """Open a new FireBird connection using the stashed driver_args."""
        return self.driver.connect(**self.driver_args)

    def test_connection(self):
        """Ping with the standard ``SELECT current_timestamp FROM RDB$DATABASE``."""
        self.execute("select current_timestamp from RDB$DATABASE")

    def lastrowid(self, table):
        """Fetch the current generator value for the table's sequence."""
        sequence_name = table._sequence_name
        self.execute("SELECT gen_id(%s, 0) FROM rdb$database" % sequence_name)
        return int(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        """
        Create the table, its generator, and the BEFORE-INSERT trigger
        that auto-populates the id column when not supplied.
        """
        tablename = table._rname
        sequence_name = table._sequence_name
        trigger_name = table._trigger_name
        self.execute(query)
        self.execute("create generator %s;" % sequence_name)
        self.execute("set generator %s to 0;" % sequence_name)
        qid = self.dialect.quote_template % "id"
        self.execute(
            "create trigger %s for %s active before insert position 0 as\n"
            "begin\nif(new.%s is null) then new.%s = gen_id(%s, 1);\n"
            "end;" % (trigger_name, tablename, qid, qid, sequence_name)
        )


@adapters.register_for("firebird_embedded")
class FireBirdEmbedded(FireBird):
    """
    FireBird Embedded variant — local-file connection, no host:port.

    URI shape: ``firebird_embedded://user:pass@/path/to/db?set_encoding=UTF8``.
    """

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<path>[^?]+)(\?set_encoding=(?P<charset>\w+))?$"
    )

    def _initialize_(self):
        """
        Parse the embedded-URI form and prepare driver_args.

        Deliberately skips ``FireBird._initialize_`` (which would parse
        the host/port URI) and goes straight to ``SQLAdapter`` — hence
        the explicit ``super(FireBird, self)._initialize_()`` rather
        than ``super()._initialize_()``.
        """
        super(FireBird, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        user = self.credential_decoder(m.group("user"))
        password = self.credential_decoder(m.group("password"))
        if password is None:
            password = ""
        pathdb = m.group("path")
        charset = m.group("charset") or "UTF8"
        self.driver_args.update(
            host="", database=pathdb, user=user, password=password, charset=charset
        )

# ============================================================
# Dialect
# ============================================================

from ..objects import Expression
from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(FireBird)
class FireBirdDialect(SQLDialect):
    """
    FireBird dialect.

    Synthetic IDs come from a per-table generator plus a BEFORE-INSERT
    trigger (set up by the adapter's ``create_sequence_and_triggers``).
    Text columns map to ``BLOB SUB_TYPE 1``. Pagination uses
    ``FIRST n SKIP m`` between ``SELECT`` and the field list.

    Subclassed by ``InformixDialect`` for shared sequence behavior.
    """

    @sqltype_for("text")
    def type_text(self):
        """FireBird TEXT is a BLOB sub-type 1 (text)."""
        return "BLOB SUB_TYPE 1"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_double(self):
        return "DOUBLE PRECISION"

    @sqltype_for("decimal")
    def type_decimal(self):
        return "DECIMAL(%(precision)s,%(scale)s)"

    @sqltype_for("blob")
    def type_blob(self):
        return "BLOB SUB_TYPE 0"

    @sqltype_for("id")
    def type_id(self):
        return "INTEGER PRIMARY KEY"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT PRIMARY KEY"

    @sqltype_for("reference")
    def type_reference(self):
        return "INTEGER REFERENCES %(foreign_key)s " + "ON DELETE %(on_delete_action)s"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return "BIGINT REFERENCES %(foreign_key)s " + "ON DELETE %(on_delete_action)s"

    def sequence_name(self, tablename):
        return self.quote("genid_%s" % tablename)

    def trigger_name(self, tablename):
        return "trg_id_%s" % tablename

    @property
    def random(self):
        return "RAND()"

    def not_null(self, default, field_type):
        return "DEFAULT %s NOT NULL" % self.adapter.represent(default, field_type)

    def epoch(self, val, query_env={}):
        return "DATEDIFF(second, '1970-01-01 00:00:00', %s)" % self.expand(
            val, query_env=query_env
        )

    def substring(self, field, parameters, query_env={}):
        return "SUBSTRING(%s from %s for %s)" % (
            self.expand(field, query_env=query_env),
            parameters[0],
            parameters[1],
        )

    def length(self, val, query_env={}):
        return "CHAR_LENGTH(%s)" % self.expand(val, query_env=query_env)

    def contains(self, first, second, case_sensitive=True, query_env={}):
        if first.type.startswith("list:"):
            second = Expression(
                None,
                self.concat(
                    "|",
                    Expression(None, self.replace(second, ("|", "||"), query_env)),
                    "|",
                ),
            )
        return "(%s CONTAINING %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, "string", query_env=query_env),
        )

    def select(
        self,
        fields,
        tables,
        where=None,
        groupby=None,
        having=None,
        orderby=None,
        limitby=None,
        distinct=False,
        for_update=False,
        with_cte=None,
    ):
        dst, whr, grp, order, limit, offset, upd = "", "", "", "", "", "", ""
        if distinct is True:
            dst = " DISTINCT"
        elif distinct:
            dst = " DISTINCT ON (%s)" % distinct
        if where:
            whr = " %s" % self.where(where)
        if groupby:
            grp = " GROUP BY %s" % groupby
            if having:
                grp += " HAVING %s" % having
        if orderby:
            order = " ORDER BY %s" % orderby
        if limitby:
            (lmin, lmax) = limitby
            limit = " FIRST %i" % (lmax - lmin)
            offset = " SKIP %i" % lmin
        if for_update:
            upd = " FOR UPDATE"

        if with_cte:
            recursive, cte = with_cte
            recursive = " RECURSIVE" if recursive else ""
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

        return "%sSELECT%s%s%s %s FROM %s%s%s%s%s;" % (
            with_cte,
            dst,
            limit,
            offset,
            fields,
            tables,
            whr,
            grp,
            order,
            upd,
        )

    def drop_table(self, table, mode):
        sequence_name = table._sequence_name
        return [
            "DROP TABLE %s %s;" % (table._rname, mode),
            "DROP GENERATOR %s;" % sequence_name,
        ]

    def truncate(self, table, mode=""):
        return [
            "DELETE FROM %s;" % table._rname,
            "SET GENERATOR %s TO 0;" % table._sequence_name,
        ]

