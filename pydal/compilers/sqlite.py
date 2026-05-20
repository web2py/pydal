"""
SQLiteCompiler: SQLite-specific overrides.

Mirrors the deltas in pydal/dialects/sqlite.py:

* ``extract`` uses the ``web2py_extract`` user function instead of the
  ANSI ``EXTRACT`` syntax.
* ``regexp`` emits a plain ``(left REGEXP right)`` (no ESCAPE clause).

Everything else inherits from SQLCompiler unchanged.
"""

from __future__ import annotations

from ..adapters.sqlite import SQLite
from . import compilers
from .sql import SQLCompiler


@compilers.register_for(SQLite)
class SQLiteCompiler(SQLCompiler):
    """
    SQLite-specific compiler. Defaults to parameterized SQL with
    ``?`` placeholders; rejects ``DISTINCT ON``; routes date extraction
    through the ``web2py_extract`` user function.
    """

    # Parameterize by default on SQLite: ``?`` placeholders are
    # universally supported by every sqlite driver, and the
    # ParamSQL/execute wiring takes care of value binding. Inline mode
    # is still available per-instance for byte-exact oracle tests.
    parameterize = True
    placeholder_style = "qmark"

    def _compile_select_body(self, n):
        """
        Same as the base body, but reject ``DISTINCT ON`` upfront —
        SQLite doesn't support it, and the legacy dialect raises here too.
        """
        if n.distinct is not True and n.distinct:
            raise SyntaxError("DISTINCT ON is not supported by SQLite")
        return super()._compile_select_body(n)

    def fn_extract(self, args, opts):
        """SQLite extract via the ``web2py_extract`` user function."""
        return "web2py_extract('%s', %s)" % (
            opts.get("unit", ""),
            self.visit(args[0]),
        )

    def un_epoch(self, x, _):
        """SQLite epoch via ``web2py_extract('epoch', ...)``."""
        return "web2py_extract('epoch', %s)" % self.visit(x)

    def op_regexp(self, l, r, _):
        """Render ``(left REGEXP right)`` — SQLite uses no ESCAPE clause."""
        return "(%s REGEXP %s)" % (
            self.visit(l),
            self.visit(r) if not _isliteral(r) else self._represent(r.value, "string"),
        )


def _isliteral(n):
    """
    Helper: True if ``n`` is an ``ast.Literal`` (deferred import to
    avoid a circular dependency at module load time).
    """
    from .. import ast
    return isinstance(n, ast.Literal)


__all__ = ["SQLiteCompiler"]
