"""Backend module for sap."""

# ============================================================
# Adapter
# ============================================================

import re

from ..backend_base import adapters
from ..backend_base import SQLAdapter


@adapters.register_for("sapdb")
class SAPDB(SQLAdapter):
    """
    SAPDB / MaxDB adapter.

    Driver: ``sapdb``. Sequences are explicit (``CREATE SEQUENCE ...``)
    and IDs are fetched via ``SELECT seq.NEXTVAL FROM DUAL``.

    URI shape: ``sapdb://user:pass@host/dbname``.
    """

    dbengine = "sapdb"
    drivers = ("sapdb",)

    REGEX_URI = (
        "^(?P<user>[^:@]+)(:(?P<password>[^@]*))?"
        r"@(?P<host>[^:/]+|\[[^\]]+\])/(?P<db>[^?]+)$"
    )

    def _initialize_(self):
        """Parse the URI and populate ``driver_args`` for ``connector``."""
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
        db = m.group("db")
        self.driver_args.update(user=user, password=password, database=db, host=host)

    def connector(self):
        """Open a new SAPDB connection. Returns the connection."""
        # Previously this method discarded the return value of
        # ``driver.connect`` and returned ``None``, which broke pooling
        # for SAPDB.
        return self.driver.connect(**self.driver_args)

    def lastrowid(self, table):
        """Fetch the next IDENTITY via ``SELECT seq.NEXTVAL FROM dual``."""
        self.execute("select %s.NEXTVAL from dual" % table._sequence_name)
        return int(self.cursor.fetchone()[0])

    def create_sequence_and_triggers(self, query, table, **args):
        """Create the per-table sequence and wire it as the id column default."""
        self.execute("CREATE SEQUENCE %s;" % table._sequence_name)
        self.execute(
            "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s');"
            % (table._rname, table._id._rname, table._sequence_name)
        )
        self.execute(query)

# ============================================================
# Dialect
# ============================================================

from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(SAPDB)
class SAPDBDialect(SQLDialect):
    """
    SAPDB / MaxDB dialect.

    Pagination has no native ``LIMIT``/``OFFSET`` — implemented via a
    nested subquery using ``ROWNO``. ``id`` columns are plain ``INT``
    populated by explicit per-table sequences.
    """

    @sqltype_for("integer")
    def type_integer(self):
        """SAPDB INTEGER is the same as ``INT``."""
        return "INT"

    @sqltype_for("text")
    def type_text(self):
        return "LONG"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_double(self):
        return "DOUBLE PRECISION"

    @sqltype_for("decimal")
    def type_decimal(self):
        return "FIXED(%(precision)s,%(scale)s)"

    @sqltype_for("id")
    def type_id(self):
        return "INT PRIMARY KEY"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT PRIMARY KEY"

    @sqltype_for("reference")
    def type_reference(self):
        return (
            "INT, FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return (
            "BIGINT, FOREIGN KEY (%(field_name)s) REFERENCES "
            + "%(foreign_key)s ON DELETE %(on_delete_action)s"
        )

    def sequence_name(self, tablename):
        return self.quote("%s_id_Seq" % tablename)

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
        """
        SAP MaxDB / SAPDB SELECT. ``limitby`` is implemented via a
        nested ROWNO subquery (no native ``OFFSET``). ``with_cte`` is
        accepted for signature compatibility with the base SQLDialect.
        """
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
            if whr:
                whr2 = whr + " AND w_row > %i" % lmin
            else:
                whr2 = self.where("w_row > %i" % lmin)
            return (
                "SELECT%s %s FROM (SELECT w_tmp.*, ROWNO w_row FROM "
                + "(SELECT %s FROM %s%s%s%s) w_tmp WHERE ROWNO=%i) %s%s%s%s;"
                % (
                    dst,
                    fields,
                    fields,
                    tables,
                    whr,
                    grp,
                    order,
                    lmax,
                    tables,
                    whr2,
                    grp,
                    order,
                )
            )
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s%s%s %s FROM %s%s%s%s%s;" % (
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

