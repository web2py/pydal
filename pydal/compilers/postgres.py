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
    def _json_operand(self, node, sql_type):
        rendered = self.visit(node)
        if self._ctx is not None and isinstance(node, ast.Literal):
            rendered = "%s::%s" % (rendered, sql_type)
        return rendered

    def _json_path_operand(self, node):
        if isinstance(node, ast.Literal) and node.type == "json_path":
            if self._ctx is not None:
                return "%s::text[]" % self._ctx.bind(node.value)
            if self.adapter is not None and isinstance(self.adapter, PostgresPsyco):
                return str(self.adapter.adapt(node.value))
            return "ARRAY[%s]::text[]" % ",".join(
                str(self._represent(value, "string")) for value in node.value
            )
        return self._json_operand(node, "text[]")

    def op_json_key(self, l, r, _):
        key_type = "integer" if getattr(r, "type", None) == "integer" else "text"
        return "%s->%s" % (
            self.visit(l),
            self._json_operand(r, key_type),
        )

    def op_json_key_value(self, l, r, _):
        key_type = "integer" if getattr(r, "type", None) == "integer" else "text"
        return "%s->>%s" % (
            self.visit(l),
            self._json_operand(r, key_type),
        )

    def op_json_path(self, l, r, _):
        return "%s#>%s" % (self.visit(l), self._json_path_operand(r))

    def op_json_path_value(self, l, r, _):
        return "%s#>>%s" % (self.visit(l), self._json_path_operand(r))

    def op_json_contains(self, l, r, _):
        left = self.visit(l)
        if not isinstance(l, ast.FieldRef):
            left = "(%s)" % left
        return "%s::jsonb@>%s::jsonb" % (left, self.visit(r))

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


@compilers.register_for(PostgresPsyco)
class PostgresPsycoCompiler(PostgresCompiler):
    parameterize = True
    placeholder_style = "format"


__all__ = ["PostgresCompiler", "PostgresPsycoCompiler"]
