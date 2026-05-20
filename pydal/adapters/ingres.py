"""Ingres adapter (uses pyodbc; experimental)."""

from . import adapters
from .base import SQLAdapter


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
