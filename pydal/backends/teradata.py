"""Backend module for teradata."""

# ============================================================
# Adapter
# ============================================================

from ..backend_base import adapters
from ..backend_base import SQLAdapter


@adapters.register_for("teradata")
class Teradata(SQLAdapter):
    """
    Teradata adapter via pyodbc.

    Teradata has two quirks worth noting:

    * Cursors must be explicitly closed before the connection, otherwise
      ``SQL_ACTIVE_STATEMENTS`` limit errors accumulate over time.
    * IDENTITY columns are not retrievable through ``lastrowid`` and
      aren't sequential anyway — ``lastrowid`` always returns 1.

    Driver: ``pyodbc``.
    """

    dbengine = ""
    drivers = ("pyodbc",)

    def _initialize_(self):
        """Stash the post-``://`` portion of the URI as the raw DSN string."""
        super()._initialize_()
        self.ruri = self.uri.split("://", 1)[1]

    def connector(self):
        """Open a new pyodbc connection using ``self.ruri`` as the DSN string."""
        return self.driver.connect(self.ruri, **self.driver_args)

    def close(self):
        """Close the cursor first; Teradata doesn't do this implicitly."""
        self.cursor.close()
        super().close()

    def lastrowid(self, table):
        """Always returns 1 — Teradata can't retrieve IDENTITY values."""
        return 1

# ============================================================
# Dialect
# ============================================================

from ..backend_base import dialects, sqltype_for
from ..backend_base import SQLDialect


@dialects.register_for(Teradata)
class TeradataDialect(SQLDialect):
    """
    Teradata dialect.

    Has no ``TEXT``/``CLOB`` type — uses ``VARCHAR(2000)``. Has no
    native ``LIMIT``/``OFFSET``; ``SELECT TOP n`` is used where
    supported, and slicing otherwise happens client-side via
    ``rowslice``.
    """

    @sqltype_for("integer")
    def type_integer(self):
        """Teradata INTEGER as ``INT``."""
        return "INT"

    @sqltype_for("text")
    def type_text(self):
        """Teradata has no TEXT type — fall back to ``VARCHAR(2000)``."""
        return "VARCHAR(2000)"

    @sqltype_for("json")
    def type_json(self):
        return "VARCHAR(4000)"

    @sqltype_for("float")
    def type_float(self):
        return "REAL"

    @sqltype_for("list:integer")
    def type_list_integer(self):
        return self.types["json"]

    @sqltype_for("list:string")
    def type_list_string(self):
        return self.types["json"]

    @sqltype_for("list:reference")
    def type_list_reference(self):
        return self.types["json"]

    @sqltype_for("bigint")
    def type_bigint(self):
        return "BIGINT"

    @sqltype_for("id")
    def type_id(self):
        return "INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL"

    @sqltype_for("big-id")
    def type_big_id(self):
        return "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL"

    @sqltype_for("reference")
    def type_reference(self):
        return "INT"

    @sqltype_for("big-reference")
    def type_big_reference(self):
        return "BIGINT"

    @sqltype_for("geometry")
    def type_geometry(self):
        return "ST_GEOMETRY"

    @sqltype_for("reference FK")
    def type_reference_fk(self):
        return " REFERENCES %(foreign_key)s "

    @sqltype_for("reference TFK")
    def type_reference_tfk(self):
        return (
            " FOREIGN KEY (%(field_name)s) REFERENCES %(foreign_table)s"
            + " (%(foreign_key)s)"
        )

    def left_join(self, val, query_env={}):
        # Left join must always have an ON clause
        if not isinstance(val, str):
            val = self.expand(val, query_env=query_env)
        return "LEFT OUTER JOIN %s" % val

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
            limit = " TOP %i" % lmax
        if for_update:
            upd = " FOR UPDATE"

        if with_cte:
            recursive, cte = with_cte
            recursive = " RECURSIVE" if recursive else ""
            with_cte = "WITH%s %s " % (recursive, cte)
        else:
            with_cte = ""

        return "%sSELECT%s%s %s FROM %s%s%s%s%s%s;" % (
            with_cte,
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

    def truncate(self, table, mode=""):
        return ["DELETE FROM %s ALL;" % table._rname]

