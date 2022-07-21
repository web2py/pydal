from . import adapters
from .base import SQLAdapter


@adapters.register_for("ingres")
class Ingres(SQLAdapter):
    dbengine = "ingres"
    drivers = ("pyodbc",)

    def _initialize_(self):
        super(Ingres, self)._initialize_()
        ruri = self.uri.split("://", 1)[1]
        connstr = ruri.lstrip()
        while connstr.startswith("/"):
            connstr = connstr[1:]
        if "=" in connstr:
            # Assume we have a regular ODBC connection string and just use it
            ruri = connstr
        else:
            # Assume only (local) dbname is passed in with OS auth
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
        self.driver.connect(self.ruri, **self.driver_args)

    def create_sequence_and_triggers(self, query, table, **args):
        # post create table auto inc code (if needed)
        # modify table to btree for performance....
        # Older Ingres releases could use rule/trigger like Oracle above.
        if hasattr(table, "_primarykey"):
            modify_tbl_sql = "modify %s to btree unique on %s" % (
                table._rname,
                ", ".join(["'%s'" % x for x in table.primarykey]),
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
    pass
