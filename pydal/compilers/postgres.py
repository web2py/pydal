"""
PostgresCompiler: Postgres-specific expression compilation.

Overrides LIKE/ILIKE rendering to cast non-text operands to ``::text``
before comparison, since Postgres has no implicit integer→text coercion
for the ``~~`` (LIKE) operator.
"""

from __future__ import annotations

from .. import ast
from ..backends.postgres import Postgres, PostgresPsyco
from . import compilers
from .sql import SQLCompiler

_TEXT_TYPES = frozenset(("string", "text", "json", "jsonb"))


@compilers.register_for(Postgres)
class PostgresCompiler(SQLCompiler):
    def _render_insert(self, n: ast.Insert, table: str, cols: str, values: str) -> str:
        sql = super()._render_insert(n, table, cols, values)
        if self.adapter is None:
            return sql
        table = self.adapter.db.get(n.table)
        if table is None or not hasattr(table, "_id"):
            return sql
        return "%s RETURNING %s;" % (sql.rstrip(";"), table._id._rname)

    def _render_like_left(self, l: ast.Node, lowered_left: bool) -> str:
        # For non-text fields (e.g. integer) Postgres rejects bare LIKE;
        # cast the operand to text first.
        rendered = self.visit(l)
        if getattr(l, "type", None) not in _TEXT_TYPES:
            rendered = "%s::text" % rendered
        return ("LOWER(%s)" % rendered) if lowered_left else rendered

    # GIS operations deliberately live here rather than in the legacy
    # Postgres dialect. Their operands have independent types: the second
    # geometry operand inherits geometry/geography, while tolerances,
    # distances, precision/options, and SRID/Proj4 arguments stay scalar.
    def _geo_binary(self, name, l, r):
        return "%s(%s,%s)" % (name, self.visit(l), self.visit(r))

    def op_st_contains(self, l, r, _):
        return self._geo_binary("ST_Contains", l, r)

    def op_st_equals(self, l, r, _):
        return self._geo_binary("ST_Equals", l, r)

    def op_st_intersects(self, l, r, _):
        return self._geo_binary("ST_Intersects", l, r)

    def op_st_overlaps(self, l, r, _):
        return self._geo_binary("ST_Overlaps", l, r)

    def op_st_touches(self, l, r, _):
        return self._geo_binary("ST_Touches", l, r)

    def op_st_within(self, l, r, _):
        return self._geo_binary("ST_Within", l, r)

    def op_st_distance(self, l, r, _):
        return self._geo_binary("ST_Distance", l, r)

    def op_st_simplify(self, l, r, _):
        return self._geo_binary("ST_Simplify", l, r)

    def op_st_simplifypreservetopology(self, l, r, _):
        return self._geo_binary("ST_SimplifyPreserveTopology", l, r)

    def op_st_transform(self, l, r, _):
        return self._geo_binary("ST_Transform", l, r)

    def un_st_astext(self, x, _):
        return "ST_AsText(%s)" % self.visit(x)

    def un_st_aswkb(self, x, _):
        # Preserve pydal's historical semantics: st_aswkb() is a pass-through
        # expression rather than an implicit ST_AsBinary() call.
        return self.visit(x)

    def un_st_x(self, x, _):
        return "ST_X(%s)" % self.visit(x)

    def un_st_y(self, x, _):
        return "ST_Y(%s)" % self.visit(x)

    def fn_st_asgeojson(self, args, _):
        if len(args) != 3:
            raise ValueError("st_asgeojson expects geometry, precision, and options")
        return "ST_AsGeoJSON(%s,%s,%s)" % tuple(self.visit(a) for a in args)

    def fn_st_dwithin(self, args, _):
        return "ST_DWithin(%s,%s,%s)" % tuple(self.visit(a) for a in args)


@compilers.register_for(PostgresPsyco)
class PostgresPsycoCompiler(PostgresCompiler):
    parameterize = True
    placeholder_style = "format"


__all__ = ["PostgresCompiler", "PostgresPsycoCompiler"]
