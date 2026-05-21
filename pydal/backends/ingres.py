"""Backend module for ingres."""

# ============================================================
# Adapter
# ============================================================

from ..backend_base import adapters
from ..backend_base import SQLAdapter


@adapters.register_for("ingres")
class Ingres(SQLAdapter):
    """
    Ingres adapter via pyodbc.

    The URI body is either a full ODBC connection string (must contain
    ``=``) or a bare local database name, in which case we synthesize
    a connection string with OS authentication using the ``Ingres``
    ODBC driver.

    After CREATE TABLE the adapter switches the storage structure to
    ``btree`` for primary-key performance and (when there's a synthetic
    ``id``) replaces the placeholder sequence name with the real one.
    """

    dbengine = "ingres"
    drivers = ("pyodbc",)

    def _initialize_(self):
        """Parse the URI into ``self.ruri`` — full ODBC string or local DB name."""
        super()._initialize_()
        ruri = self.uri.split("://", 1)[1]
        connstr = ruri.lstrip()
        while connstr.startswith("/"):
            connstr = connstr[1:]
        if "=" in connstr:
            # Already a regular ODBC connection string — use as-is.
            ruri = connstr
        else:
            # Just a local database name; assume OS authentication via
            # the default ``Ingres`` ODBC driver on the local node.
            database_name = connstr
            default_driver_name = "Ingres"
            vnode = "(local)"
            ruri = "Driver={%s};Server=%s;Database=%s" % (
                default_driver_name,
                vnode,
                database_name,
            )
        self.ruri = ruri

    def connector(self):
        """Open a new Ingres ODBC connection. Returns the connection."""
        # Was missing the return previously, which broke pooling.
        return self.driver.connect(self.ruri, **self.driver_args)

    def create_sequence_and_triggers(self, query, table, **args):
        """
        Run CREATE TABLE plus the Ingres-specific btree conversion.

        For tables with an explicit ``primarykey``, switch storage to
        ``btree unique on <keys>``. For tables with a synthetic ``id``,
        also create the backing sequence and substitute its name into
        the CREATE TABLE statement before executing.
        """
        if hasattr(table, "_primarykey"):
            modify_tbl_sql = "modify %s to btree unique on %s" % (
                table._rname,
                # Was ``table.primarykey`` — wrong attribute name.
                ", ".join(["'%s'" % x for x in table._primarykey]),
            )
            self.execute(modify_tbl_sql)
        else:
            tmp_seqname = "%s_iisq" % table._raw_rname
            query = query.replace(self.dialect.INGRES_SEQNAME, tmp_seqname)
            self.execute("create sequence %s" % tmp_seqname)
            self.execute(query)
            self.execute("modify %s to btree unique on %s" % (table._rname, "id"))


@adapters.register_for("ingresu")
class IngresUnicode(Ingres):
    """Ingres adapter variant with Unicode column types."""

# ============================================================
# Dialect
# ============================================================

from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(Ingres)
class IngresDialect(SQLDialect):
    """
    Ingres dialect.

    Pagination uses ``SELECT FIRST n ... OFFSET m``. Synthetic IDs use
    a per-table sequence (the ``INGRES_SEQNAME`` placeholder is
    substituted by the adapter). Date/datetime columns use the ANSI
    types ``ANSIDATE`` / ``TIMESTAMP WITHOUT TIME ZONE``.
    """
    # Sequence name used in the default-next-value clauses for ``id``
    # and ``big-id`` columns. Read by the Ingres adapter when rewriting
    # CREATE TABLE statements to substitute a real sequence name.
    INGRES_SEQNAME = "ii***lineitemsequence"

    @sqltype_for("text")
    def type_text(self):
        return "CLOB"

    @sqltype_for("integer")
    def type_integer(self):
        return "INTEGER4"

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("double")
    def type_float(self):
        return "FLOAT8"

    @sqltype_for("date")
    def type_date(self):
        return "ANSIDATE"

    @sqltype_for("time")
    def type_time(self):
        return "TIME WITHOUT TIME ZONE"

    @sqltype_for("datetime")
    def type_datetime(self):
        return "TIMESTAMP WITHOUT TIME ZONE"

    @sqltype_for("id")
    def type_id(self):
        return (
            "int not null unique with default next value for %s" % self.INGRES_SEQNAME
        )

    @sqltype_for("big-id")
    def type_big_id(self):
        return (
            "bigint not null unique with default next value for %s"
            % self.INGRES_SEQNAME
        )

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

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return (
            ", CONSTRAINT FK_%(constraint_name)s FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_key)s "
            + "ON DELETE %(on_delete_action)s"
        )

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            " CONSTRAINT FK_%(constraint_name)s_PK FOREIGN KEY "
            + "(%(field_name)s) REFERENCES %(foreign_table)s"
            + "(%(foreign_key)s) ON DELETE %(on_delete_action)s"
        )

    def left_join(self, val, query_env={}):
        # Left join must always have an ON clause
        if not isinstance(val, str):
            val = self.expand(val, query_env=query_env)
        return "LEFT OUTER JOIN %s" % val

    @property
    def random(self):
        return "RANDOM()"

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
        Ingres-flavored SELECT.

        Differs from the SQL standard in two ways:

        * ``FIRST N`` appears between ``SELECT`` and the field list
          (instead of ``LIMIT N`` at the end).
        * ``OFFSET N`` is emitted after the ORDER BY clause, separate
          from ``FIRST``.

        ``with_cte`` is accepted for signature compatibility with the
        base ``SQLDialect.select``; Ingres CTE handling is not currently
        wired through this method.
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
            fetch_amt = lmax - lmin
            if fetch_amt:
                limit = " FIRST %i" % fetch_amt
            if lmin:
                offset = " OFFSET %i" % lmin
        if for_update:
            upd = " FOR UPDATE"
        return "SELECT%s%s %s FROM %s%s%s%s%s%s;" % (
            dst,
            limit,
            fields,
            tables,
            whr,
            grp,
            order,
            offset,
            upd,
        )


@dialects.register_for(IngresUnicode)
class IngresUnicodeDialect(IngresDialect):
    @sqltype_for("string")
    def type_string(self):
        return "NVARCHAR(%(length)s)"

    @sqltype_for("text")
    def type_text(self):
        return "NCLOB"

