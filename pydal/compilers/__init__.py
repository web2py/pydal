"""
SQL compilers: ast.Node -> SQL string.

A compiler walks an ``ast.Node`` and emits SQL text. The base
``SQLCompiler`` matches pydal's default ``SQLDialect`` output;
backend-specific subclasses override only the handful of methods that
diverge.

The compiler is the only layer that knows about SQL. Set/Table hand
ASTs to a compiler and never build SQL themselves.

Discovery uses the same Dispatcher pattern as dialects/parsers/
representers: a compiler class is registered for an adapter class via
``@compilers.register_for(SomeAdapter)``, and adapters call
``compilers.get_for(self)`` during ``_load_dependencies`` to obtain
the right instance.
"""

from ..helpers._internals import Dispatcher

compilers: Dispatcher = Dispatcher("compiler")

# Importing the modules below triggers their @compilers.register_for(...)
# side-effects so the registry is populated.
from .sql import SQLCompiler       # noqa: E402, F401  (side-effect import)
from .sqlite import SQLiteCompiler  # noqa: E402, F401  (side-effect import)

__all__ = ["SQLCompiler", "SQLiteCompiler", "compilers"]
