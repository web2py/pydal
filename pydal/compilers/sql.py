"""
SQLCompiler: ast.Node -> SQL string.

Bit-compatible with ``pydal/dialects/base.py`` SQLDialect output for the
op vocabulary covered by the AST. Backend-specific subclasses override
only divergent methods (regexp syntax, extract style, quote char, ...).

The compiler depends on a ``represent(value, field_type) -> str`` callable
to turn primitive values into SQL literals. During the migration, callers
pass ``adapter.represent`` so the compiler stays bit-for-bit compatible
with the existing ``represent``/``representer`` chain. Layer 4 will fold
that responsibility into the compiler itself (or a dedicated Renderer).
"""

from __future__ import annotations

import datetime as _datetime
from typing import Any, Callable, List, Optional, Tuple

from .. import ast
from ..backend_base import SQLAdapter
from . import compilers


class ParamSQL(str):
    """
    A SQL fragment that carries bound parameters alongside it.

    Subclasses ``str`` so the rest of pydal can continue to treat compiled
    SQL as a plain string (logging, caching keys, etc.) while a parallel
    tuple of parameter values rides along on the ``.params`` attribute.
    ``SQLAdapter.execute`` detects this attribute and forwards the
    parameters to ``cursor.execute(sql, params)`` automatically.

    Note: most ``str`` operations (``.lower()``, ``.replace()``, ``+``)
    return a plain ``str`` and drop ``.params``. That's fine for pydal,
    which neither transforms nor concatenates compiled SQL, but it's a
    pitfall worth knowing.
    """

    # Declared at class level so type checkers know about it.
    params: Tuple[Any, ...]

    def __new__(cls, sql: str, params: "Tuple[Any, ...] | List[Any]" = ()) -> "ParamSQL":
        obj = super().__new__(cls, sql)
        obj.params = tuple(params)
        return obj


class Ctx:
    """
    Per-compile state for parameter binding.

    A fresh Ctx is created at every ``compile_*`` entry point when
    ``SQLCompiler.parameterize`` is True. It accumulates raw Python
    values in ``params`` and hands back placeholder strings shaped for
    the dialect (``?``, ``$N``, ``%s``, ...).
    """

    __slots__ = ("params", "placeholder_style")

    def __init__(self, placeholder_style: str = "qmark"):
        self.params: List[Any] = []
        self.placeholder_style = placeholder_style

    def bind(self, value: Any) -> str:
        """Append ``value`` to the params list and return its placeholder."""
        self.params.append(value)
        idx = len(self.params)
        style = self.placeholder_style
        if style == "qmark":
            return "?"
        if style == "numeric":
            return "$%d" % idx
        if style == "format":
            return "%s"
        if style == "pyformat":
            return "%%(p%d)s" % idx
        raise ValueError("unknown placeholder_style %r" % style)


# Field types eligible for parameter binding. For each, ``_adapt_for_bind``
# below converts the Python value into the wire form (matching pydal's
# representer output, sans the quoting). Types not listed here keep the
# legacy inline path — list:*, json/jsonb, blob/upload, geo*, etc., have
# bespoke encodings (pipe-delimiters, JSON serialization, base64) and
# aren't worth the round-trip through bound params.
_PARAMETERIZABLE_TYPES = frozenset(
    {
        "string", "text", "password",
        "integer", "bigint",
        "float", "double",
        "boolean",
        "date", "time", "datetime",
    }
)


@compilers.register_for(SQLAdapter)
class SQLCompiler:
    """
    Walks an AST and emits SQL text.

    The class layout mirrors ast.py: one ``v_<NodeName>`` per node type,
    plus dispatch tables for BinOp/UnaryOp/FuncCall that route to
    ``op_<name>`` / ``un_<name>`` / ``fn_<name>`` methods. Override one
    of those to customize one operator for one backend.
    """

    # ---- knobs subclasses may tweak ----
    quote_template: str = '"%s"'
    true_exp: str = "1"
    false_exp: str = "0"
    # Bound-form tokens for boolean values. Mirror ``SQLDialect.true``/
    # ``false`` — "T"/"F" for the SQL base; subclasses override
    # (mysql uses "1"/"0", etc.).
    true_token: str = "T"
    false_token: str = "F"
    # Separator between date and time in a datetime literal — matches
    # ``SQLDialect.dt_sep``.
    dt_sep: str = " "
    # When True, compile_* entry points return ParamSQL with bound
    # parameters in place of inlined literals (for the types listed in
    # _PARAMETERIZABLE_TYPES). Default False to preserve byte-for-byte
    # compatibility with SQLDialect output; flip per instance to opt in.
    parameterize: bool = False
    # DB-API placeholder style. Subclasses set this to match their driver.
    placeholder_style: str = "qmark"

    def __init__(
        self,
        adapter=None,
        represent: Optional[Callable[[Any, Optional[str]], str]] = None,
        parameterize: Optional[bool] = None,
        placeholder_style: Optional[str] = None,
    ):
        """
        The compiler is constructed either:

        * via the Dispatcher: ``SQLCompiler(adapter)`` — uses
          ``adapter.represent`` for value rendering, and keeps the
          adapter reachable as ``self.adapter`` for follow-up hooks.
        * standalone for testing: ``SQLCompiler(represent=fn)``.

        ``parameterize`` and ``placeholder_style`` override the class
        defaults per-instance — useful for opt-in experiments.
        """
        self.adapter = adapter
        if adapter is not None:
            self._represent = adapter.represent
        else:
            self._represent = represent or self._default_represent
        if parameterize is not None:
            self.parameterize = parameterize
        if placeholder_style is not None:
            self.placeholder_style = placeholder_style
        # Per-compile context. ``None`` means inline mode (no binding);
        # a Ctx instance means we're inside a parameterized compile.
        self._ctx: Optional[Ctx] = None
        # Stack of parent-Select tablename scopes — populated when one
        # SELECT body is recursively compiled inside another (e.g. via
        # ``belongs(set.subselect(...))``). Lets correlated subqueries
        # prune outer-scoped tables from their own FROM clause.
        self._scope_stack: list = []

    # ------------------------------------------------------------------ entry

    def _begin(self) -> Optional[Ctx]:
        if not self.parameterize:
            return None
        ctx = Ctx(self.placeholder_style)
        self._ctx = ctx
        return ctx

    def _finish(self, sql: str, ctx: Optional[Ctx]):
        self._ctx = None
        if ctx is None:
            return sql
        return ParamSQL(sql, ctx.params)

    def compile_expression(self, node: ast.Node):
        """
        Compile a single expression node into SQL.

        Returns a plain ``str`` in inline mode; a ``ParamSQL`` carrying
        bound parameters when ``parameterize=True``.
        """
        ctx = self._begin()
        try:
            sql = self.visit(node)
        finally:
            self._ctx = None
        return ParamSQL(sql, ctx.params) if ctx is not None else sql

    # ----- statement entry points (Layer 2c) -----

    def compile_select(self, n: ast.Select):
        """
        Compile a ``Select`` node into a full ``SELECT ...;`` statement.

        Mirrors ``SQLDialect.select`` byte-for-byte for the supported
        single-table shape: fields, sources, WHERE, GROUP BY/HAVING,
        ORDER BY, LIMIT/OFFSET, FOR UPDATE, DISTINCT(/ON).
        """
        ctx = self._begin()
        try:
            sql = self._compile_select_body(n)
        finally:
            self._ctx = None
        return ParamSQL(sql, ctx.params) if ctx is not None else sql

    def _compile_select_body(self, n: ast.Select) -> str:
        # IMPORTANT: visits happen in SQL-position order. Positional ``?``
        # placeholders bind to params in the order they appear in the
        # final SQL string, so we MUST accumulate params in the same
        # order. Out-of-order visiting silently scrambles bindings.

        # Correlated-subquery scope pruning. If a parent SELECT pushed
        # its tables onto _scope_stack and this Select is correlated,
        # drop any source/join whose effective name (alias if aliased,
        # else canonical) is already in the outer scope. Aliased tables
        # contribute their alias to scope — ``"t1" AS "tmp"`` puts
        # ``tmp`` in scope, NOT ``t1``, so a child referencing the real
        # ``t1`` is a fresh declaration, not a correlation.
        # ``n.outer_scope`` lets the caller pre-seed extra names (used
        # by Select._compile when it passes ``outer_scoped=[...]``).
        parent_scope: frozenset = (
            self._scope_stack[-1] if self._scope_stack else frozenset()
        )
        if n.outer_scope:
            parent_scope = parent_scope | frozenset(n.outer_scope)

        def _effective(node):
            if not isinstance(node, ast.TableRef):
                return None
            return node.alias if node.alias else node.name

        active_sources = n.sources
        active_joins = n.joins
        if parent_scope and n.correlated:
            active_sources = tuple(
                s for s in n.sources
                if _effective(s) not in parent_scope
            )
            active_joins = tuple(
                j for j in n.joins
                if _effective(j.target) not in parent_scope
            )

        # Push this Select's effective-name scope so nested SELECTs see it.
        my_scope = set(parent_scope)
        for s in n.sources:
            name = _effective(s)
            if name is not None:
                my_scope.add(name)
        for j in n.joins:
            name = _effective(j.target)
            if name is not None:
                my_scope.add(name)
        self._scope_stack.append(frozenset(my_scope))

        try:
            # DISTINCT (may contain a sub-expression)
            if n.distinct is True:
                dst = " DISTINCT"
            elif n.distinct:
                dst = " DISTINCT ON (%s)" % self.visit(n.distinct)
            else:
                dst = ""
            # WITH <ctes> — emitted FIRST so positional placeholders
            # bind in the right order. Recursive CTEs flag the header.
            with_cte = ""
            if n.with_cte:
                pieces = []
                is_recursive = False
                for cte in n.with_cte:
                    pieces.append(self._compile_cte(cte))
                    if cte.recursive_parts:
                        is_recursive = True
                rec = " RECURSIVE" if is_recursive else ""
                with_cte = "WITH%s %s " % (rec, ", ".join(pieces))
            # SELECT <fields>
            fields = ", ".join(self.visit(f) for f in n.fields)
            # FROM <sources>
            sources = ", ".join(self.visit(t) for t in active_sources)
            # <joins>
            joins = ""
            if active_joins:
                joins = " " + " ".join(self.visit(j) for j in active_joins)
            # WHERE
            whr = " WHERE %s" % self.visit(n.where) if n.where is not None else ""
            # GROUP BY / HAVING
            grp = ""
            if n.groupby:
                grp = " GROUP BY %s" % ", ".join(self.visit(g) for g in n.groupby)
                if n.having is not None:
                    grp += " HAVING %s" % self.visit(n.having)
            # ORDER BY
            order = ""
            if n.orderby:
                order = " ORDER BY %s" % ", ".join(self.visit(o) for o in n.orderby)
            # LIMIT / OFFSET (always inline)
            limit = offset = ""
            if n.limit:
                lmin, lmax = n.limit
                limit = " LIMIT %i" % (lmax - lmin)
                offset = " OFFSET %i" % lmin
            # FOR UPDATE
            upd = " FOR UPDATE" if n.for_update else ""
            return "%sSELECT%s %s FROM %s%s%s%s%s%s%s%s;" % (
                with_cte, dst, fields, sources, joins, whr, grp, order, limit, offset, upd,
            )
        finally:
            self._scope_stack.pop()

    def v_Select(self, n: ast.Select) -> str:
        """
        Render a Select node as a parenthesized subquery.

        Used when a Select appears inside an outer query (most commonly
        ``Field.belongs(set.subselect(...))``). We strip the trailing
        ``;`` from the body and wrap in parens. Param accumulation
        happens in the SAME ctx as the outer compile, so a single param
        list rides on the final ParamSQL.
        """
        body = self._compile_select_body(n)
        if body.endswith(";"):
            body = body[:-1]
        return "(%s)" % body

    def _compile_cte(self, c: ast.Cte) -> str:
        """``name(col1, col2, ...) AS (body [UNION other ...])``."""
        body = self._compile_select_body(c.select)
        if body.endswith(";"):
            body = body[:-1]
        if c.recursive_parts:
            for union_type, rec in c.recursive_parts:
                rec_sql = self._compile_select_body(rec)
                if rec_sql.endswith(";"):
                    rec_sql = rec_sql[:-1]
                body = "%s %s %s" % (body, union_type, rec_sql)
        cols = ""
        if c.columns:
            cols = "(%s)" % ", ".join(c.columns)
        return "%s%s AS (%s)" % (self.q(c.name), cols, body)

    # ----- join node -----

    def v_Join(self, n: ast.Join) -> str:
        """
        Render a ``Join`` node — ``CROSS JOIN``, ``[INNER] JOIN ... ON``,
        or ``LEFT JOIN ... [ON]`` depending on ``n.kind`` and ``n.on``.
        """
        target = self.visit(n.target)
        if n.kind == "cross":
            return "CROSS JOIN %s" % target
        if n.kind == "inner":
            if n.on is None:
                return "JOIN %s" % target
            return "JOIN %s ON %s" % (target, self.visit(n.on))
        if n.kind == "left":
            if n.on is None:
                return "LEFT JOIN %s" % target
            return "LEFT JOIN %s ON %s" % (target, self.visit(n.on))
        raise NotImplementedError("Join kind %r" % n.kind)

    def compile_insert(self, n: ast.Insert):
        """
        Compile an ``Insert`` AST node into ``INSERT INTO ... ;`` SQL.

        Honors ``n.sqlsafe`` for aliased writes (INSERT always targets the
        underlying physical table). Multi-row INSERT not yet supported.
        """
        ctx = self._begin()
        try:
            table = n.sqlsafe if n.sqlsafe is not None else self._table_sql(n.table)
            if not n.rows or not n.rows[0]:
                sql = "INSERT INTO %s DEFAULT VALUES;" % table
            else:
                if len(n.rows) > 1:
                    raise NotImplementedError("multi-row INSERT not yet supported")
                cols = ",".join(self._column_sql(n.table, c) for c in n.cols)
                values = ",".join(self.visit(v) for v in n.rows[0])
                sql = "INSERT INTO %s(%s) VALUES (%s);" % (table, cols, values)
        finally:
            self._ctx = None
        return ParamSQL(sql, ctx.params) if ctx is not None else sql

    def compile_update(self, n: ast.Update):
        """
        Compile an ``Update`` AST node into ``UPDATE ... SET ... WHERE ...;`` SQL.

        Honors ``n.sqlsafe`` for aliased writes. Subqueries inside
        SET/WHERE see the UPDATE's target table as outer scope.
        """
        ctx = self._begin()
        # Push the UPDATE target so any subquery in SET/WHERE knows the
        # outer-scope table — same correlated-subquery semantics as SELECT.
        self._scope_stack.append(frozenset({n.table}))
        try:
            table = n.sqlsafe if n.sqlsafe is not None else self._writing_alias(n.table)
            sets = ",".join(
                "%s=%s" % (self._column_sql(n.table, col), self.visit(val))
                for col, val in n.sets
            )
            whr = " WHERE %s" % self.visit(n.where) if n.where is not None else ""
            sql = "UPDATE %s SET %s%s;" % (table, sets, whr)
        finally:
            self._scope_stack.pop()
            self._ctx = None
        return ParamSQL(sql, ctx.params) if ctx is not None else sql

    def compile_delete(self, n: ast.Delete):
        """Compile a ``Delete`` AST node into ``DELETE FROM ... WHERE ...;`` SQL."""
        ctx = self._begin()
        self._scope_stack.append(frozenset({n.table}))
        try:
            table = n.sqlsafe if n.sqlsafe is not None else self._writing_alias(n.table)
            whr = " WHERE %s" % self.visit(n.where) if n.where is not None else ""
            sql = "DELETE FROM %s%s;" % (table, whr)
        finally:
            self._scope_stack.pop()
            self._ctx = None
        return ParamSQL(sql, ctx.params) if ctx is not None else sql

    def compile_count(self, n: ast.Count):
        """Compile a Count node into ``SELECT COUNT(...) FROM ...;``."""
        ctx = self._begin()
        # Outer scope for correlated subqueries: this Count's source tables.
        outer = set()
        for s in n.query.sources:
            if isinstance(s, ast.TableRef):
                outer.add(s.name)
        self._scope_stack.append(frozenset(outer))
        try:
            inner = n.query
            if n.distinct is not None:
                count_expr = "COUNT(DISTINCT %s)" % self.visit(n.distinct)
            else:
                count_expr = "COUNT(*)"
            tables = ",".join(self.visit(t) for t in inner.sources)
            whr = " WHERE %s" % self.visit(inner.where) if inner.where is not None else ""
            sql = "SELECT %s FROM %s%s;" % (count_expr, tables, whr)
        finally:
            self._scope_stack.pop()
            self._ctx = None
        return ParamSQL(sql, ctx.params) if ctx is not None else sql

    # ------------------------------------------------------------------ utils
    def q(self, name: str) -> str:
        """Apply the dialect's ``quote_template`` to ``name`` (e.g. ``"foo"``)."""
        return self.quote_template % name

    def _table_sql(self, tablename: str) -> str:
        """
        Resolve a logical tablename to its physical SQL identifier.

        Honors user-supplied ``rname=`` when an adapter is bound. Falls
        back to quoted logical name otherwise.
        """
        if self.adapter is not None:
            t = self.adapter.db.get(tablename)
            if t is not None:
                return t._rname
        return self.q(tablename)

    def _writing_alias(self, tablename: str) -> str:
        """
        Tablename rendering for UPDATE/DELETE — defers to the dialect's
        ``writing_alias`` when possible (it rejects aliased writes on
        SQLite, for example).
        """
        if self.adapter is not None:
            t = self.adapter.db.get(tablename)
            if t is not None:
                return self.adapter.dialect.writing_alias(t)
        return self.q(tablename)

    def _column_sql(self, tablename: str, fieldname: str) -> str:
        """
        Resolve a logical (table, field) pair to the column's physical
        SQL identifier (``_rname`` — already-quoted-or-not).
        """
        if self.adapter is not None:
            t = self.adapter.db.get(tablename)
            if t is not None and fieldname in t.fields:
                return t[fieldname]._rname
        return self.q(fieldname)

    @staticmethod
    def _default_represent(value, type_):
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        return "'%s'" % str(value).replace("'", "''")

    # ------------------------------------------------------------------ visit
    def visit(self, node: ast.Node) -> str:
        """Dispatch to ``v_<NodeName>`` for the given AST node."""
        return getattr(self, "v_" + type(node).__name__)(node)

    # ---- leaves ----

    def v_FieldRef(self, n: ast.FieldRef) -> str:
        """Render a ``FieldRef`` as ``"table"."column"`` (honors rname)."""
        # Translator usually bakes the pre-formatted SQL identifier
        # into the node (it knows about rname + table aliasing). For
        # hand-built nodes in unit tests, fall back to default quoting.
        if n.sqlsafe is not None:
            return n.sqlsafe
        if self.adapter is not None:
            t = self.adapter.db.get(n.table)
            if t is not None and n.name in t.fields:
                return t[n.name].sqlsafe
        return self.q(n.table) + "." + self.q(n.name)

    def v_Literal(self, n: ast.Literal) -> str:
        """
        Render a ``Literal``: bind it as a parameter when the type is
        in the allowlist, otherwise inline via the legacy representer.
        """
        # Parameter-binding takes priority when:
        #   * we're in a parameterized compile (ctx is set), AND
        #   * the value's type is in the safe-to-bind allowlist, AND
        #   * the value isn't None (NULL is rendered inline).
        # Everything else falls back to the inline representation,
        # which is bit-compatible with the legacy SQLDialect path.
        if (
            self._ctx is not None
            and n.value is not None
            and isinstance(n.type, str)
            and (n.type in _PARAMETERIZABLE_TYPES or n.type.startswith("decimal"))
        ):
            return self._ctx.bind(self._adapt_for_bind(n.value, n.type))
        if n.type:
            return str(self._represent(n.value, n.type))
        if isinstance(n.value, bool):
            return self.true_exp if n.value else self.false_exp
        if isinstance(n.value, (list, tuple)):
            return ",".join(str(self._represent(v, None)) for v in n.value)
        return str(n.value)

    def _adapt_for_bind(self, value, type_):
        """
        Convert a Python value to its DB-API bind form for ``type_``.

        Mirrors the pydal representer's output, sans the SQL quoting:

        * ``boolean`` -> ``true_token`` / ``false_token`` (``"T"``/``"F"`` by default)
        * ``date`` -> ``"YYYY-MM-DD"``
        * ``time`` -> ``"HH:MM:SS"`` (with truncation matching the legacy
          slice ``[:10]`` — preserved for bug-for-bug compatibility)
        * ``datetime`` -> ``"YYYY-MM-DD<sep>HH:MM:SS"``
        * everything else: passthrough (driver handles native types).
        """
        if type_ == "boolean":
            if value and str(value)[:1].upper() not in "0F":
                return self.true_token
            return self.false_token
        if type_ == "date":
            if isinstance(value, (_datetime.date, _datetime.datetime)):
                return value.isoformat()[:10]
            return str(value)
        if type_ == "time":
            if isinstance(value, _datetime.time):
                return value.isoformat()[:10]
            return str(value)
        if type_ == "datetime":
            if isinstance(value, _datetime.datetime):
                return value.isoformat(self.dt_sep)[:19]
            if isinstance(value, _datetime.date):
                return value.isoformat()[:10] + self.dt_sep + "00:00:00"
            return str(value)
        return value

    def v_Raw(self, n: ast.Raw) -> str:
        """Emit a ``Raw`` node verbatim (translator pre-shapes it)."""
        return n.sql

    def v_Star(self, n: ast.Star) -> str:
        """Render ``Star`` as ``*`` or ``"table".*``."""
        return (self.q(n.table) + ".*") if n.table else "*"

    def v_TableRef(self, n: ast.TableRef) -> str:
        """Render a ``TableRef`` (with optional alias) as a SQL identifier."""
        # Defer to pydal's sql_fullref when we have an adapter — it
        # handles rname + aliasing + dialect quirks. The standalone path
        # (no adapter) keeps the plain quoted form for unit tests.
        if self.adapter is not None:
            t = self.adapter.db.get(n.name)
            if t is not None:
                if n.alias and n.alias != n.name:
                    return self.adapter.sqlsafe_table(n.alias, t._rname)
                return t.sql_fullref
        if n.alias and n.alias != n.name:
            return "%s AS %s" % (self.q(n.name), self.q(n.alias))
        return self.q(n.name)

    def v_Aliased(self, n: ast.Aliased) -> str:
        """
        Render ``Aliased`` as ``expr AS alias`` (alias quoted only when
        wrapping a ``Select`` — matches the legacy convention).
        """
        # Convention matches the legacy dialect:
        # * field/expression aliases (``.with_alias("foo")``) emit
        #   ``expr AS foo`` — alias is unquoted.
        # * Select-as-source aliases (``nested_select(...).with_alias("sub")``)
        #   emit ``(SELECT ...) AS "sub"`` — alias is quoted, matching
        #   ``sqlsafe_table``.
        alias = self.q(n.alias) if isinstance(n.node, ast.Select) else n.alias
        return "%s AS %s" % (self.visit(n.node), alias)

    # ---- composites: dispatch by op/name ----

    def v_BinOp(self, n: ast.BinOp) -> str:
        """Dispatch a ``BinOp`` to ``op_<name>``."""
        method = getattr(self, "op_" + n.op, None)
        if method is None:
            raise NotImplementedError("SQLCompiler: unknown BinOp %r" % n.op)
        return method(n.left, n.right, dict(n.opts))

    def v_UnaryOp(self, n: ast.UnaryOp) -> str:
        """Dispatch a ``UnaryOp`` to ``un_<name>``."""
        method = getattr(self, "un_" + n.op, None)
        if method is None:
            raise NotImplementedError("SQLCompiler: unknown UnaryOp %r" % n.op)
        return method(n.operand, dict(n.opts))

    def v_FuncCall(self, n: ast.FuncCall) -> str:
        """Dispatch a ``FuncCall`` to ``fn_<name>``."""
        method = getattr(self, "fn_" + n.name, None)
        if method is None:
            raise NotImplementedError("SQLCompiler: unknown FuncCall %r" % n.name)
        return method(n.args, dict(n.opts))

    def v_InList(self, n: ast.InList) -> str:
        """Render ``expr IN (v1, v2, ...)`` or ``expr IN (SELECT ...)``."""
        if not n.values:
            return "(1=0)"
        # Special case: a single Select-as-IN-list. Avoid double
        # parenthesization — emit ``(left IN (SELECT ...))`` rather than
        # ``(left IN ((SELECT ...)))`` by inlining the SELECT body
        # directly inside the IN parens.
        if len(n.values) == 1 and isinstance(n.values[0], ast.Select):
            sub = self._compile_select_body(n.values[0])
            if sub.endswith(";"):
                sub = sub[:-1]
            return "(%s IN (%s))" % (self.visit(n.expr), sub)
        items = ",".join(self.visit(v) for v in n.values)
        return "(%s IN (%s))" % (self.visit(n.expr), items)

    # ================================================================== ops
    # Comparisons, logical, arithmetic, ordering, null-predicates.
    # Each ``op_<name>`` / ``un_<name>`` is invoked by the visitor above
    # and emits the SQL fragment for one AST operator. They share a tiny
    # vocabulary so overriding for a backend is one-method-per-divergence.
    # =====================================================================

    def _cmp(self, sym: str, l: ast.Node, r: ast.Node, null_form: Optional[str]) -> str:
        """
        Shared rendering for binary comparison ops.

        ``null_form`` is the SQL fragment to substitute when the right
        operand is ``Literal(None)`` (e.g. ``"IS NULL"``); pass ``None``
        if the op doesn't accept a None right (``<``, ``>``, ...).
        """
        if isinstance(r, ast.Literal) and r.value is None:
            if null_form is None:
                raise RuntimeError("Cannot compare %s %s None" % (self.visit(l), sym))
            return "(%s %s)" % (self.visit(l), null_form)
        return "(%s %s %s)" % (self.visit(l), sym, self.visit(r))

    def op_eq(self, l, r, _):
        """Render ``(left = right)`` — collapses to ``IS NULL`` for ``Literal(None)``."""
        return self._cmp("=", l, r, "IS NULL")

    def op_ne(self, l, r, _):
        """Render ``(left <> right)`` — collapses to ``IS NOT NULL`` for ``Literal(None)``."""
        return self._cmp("<>", l, r, "IS NOT NULL")

    def op_lt(self, l, r, _):
        """Render ``(left < right)``."""
        return self._cmp("<", l, r, None)

    def op_lte(self, l, r, _):
        """Render ``(left <= right)``."""
        return self._cmp("<=", l, r, None)

    def op_gt(self, l, r, _):
        """Render ``(left > right)``."""
        return self._cmp(">", l, r, None)

    def op_gte(self, l, r, _):
        """Render ``(left >= right)``."""
        return self._cmp(">=", l, r, None)

    # Logical

    def op_and(self, l, r, _):
        """Render ``(left AND right)``."""
        return "(%s AND %s)" % (self.visit(l), self.visit(r))

    def op_or(self, l, r, _):
        """Render ``(left OR right)``."""
        return "(%s OR %s)" % (self.visit(l), self.visit(r))

    def un_not(self, x, _):
        """Render ``(NOT operand)``."""
        return "(NOT %s)" % self.visit(x)

    # Arithmetic

    @staticmethod
    def _is_numerical_type(field_type):
        """True for pydal numeric types (integer/float/double/bigint/boolean/decimal*)."""
        if not isinstance(field_type, str):
            return False
        if field_type in ("integer", "float", "double", "bigint", "boolean"):
            return True
        return field_type.startswith("decimal")

    def op_add(self, l, r, opts):
        """Render ``(left + right)`` for numeric types, ``(left || right)`` for strings."""
        # SQLDialect.add is type-overloaded: numeric types render as `+`,
        # everything else (string, text, ...) renders as `||` concat.
        ltype = opts.get("left_type")
        if ltype is not None and not self._is_numerical_type(ltype):
            return "(%s || %s)" % (self.visit(l), self.visit(r))
        return "(%s + %s)" % (self.visit(l), self.visit(r))

    def op_sub(self, l, r, _):
        """Render ``(left - right)``."""
        return "(%s - %s)" % (self.visit(l), self.visit(r))

    def op_mul(self, l, r, _):
        """Render ``(left * right)``."""
        return "(%s * %s)" % (self.visit(l), self.visit(r))

    def op_div(self, l, r, _):
        """Render ``(left / right)``."""
        return "(%s / %s)" % (self.visit(l), self.visit(r))

    def op_mod(self, l, r, _):
        """Render ``(left %% right)`` (SQL modulo)."""
        return "(%s %% %s)" % (self.visit(l), self.visit(r))

    # Order / comma

    def un_invert(self, x, _):
        """Render ``operand DESC`` (used inside ORDER BY)."""
        return "%s DESC" % self.visit(x)

    def op_comma(self, l, r, _):
        """Render ``left, right`` — comma chain in ORDER BY / DISTINCT ON."""
        return "%s, %s" % (self.visit(l), self.visit(r))

    # Null predicates (folded in by the translator)

    def un_is_null(self, x, _):
        """Render ``(operand IS NULL)``."""
        return "(%s IS NULL)" % self.visit(x)

    def un_is_not_null(self, x, _):
        """Render ``(operand IS NOT NULL)``."""
        return "(%s IS NOT NULL)" % self.visit(x)

    # =====================================================================
    # String / pattern match
    # =====================================================================

    def _like_escape(self, term: str, escape_char: str = "\\") -> str:
        """
        Escape user-supplied LIKE pattern characters (% and _ and the
        escape char itself). Mirrors SQLDialect._like_escaper_default.
        """
        return (
            term.replace("\\", "\\\\")
                .replace("%", r"\%")
                .replace("_", r"\_")
        )

    def _like_render(self, l: ast.Node, r: ast.Node, escape, lowered_left: bool):
        """
        Common shape for LIKE/ILIKE.

        ``r`` may be a Literal (escape its backslashes when escape=None)
        or a non-Literal expression (rendered as-is). For ILIKE we also
        lowercase the rendered second string and wrap the first in LOWER.
        """
        if isinstance(r, ast.Literal):
            rendered = self._represent(r.value, r.type or "string")
            if lowered_left:
                rendered = str(rendered).lower()
            if escape is None:
                escape = "\\"
                rendered = str(rendered).replace(escape, escape * 2)
        else:
            rendered = self.visit(r)
            if escape is None:
                escape = "\\"
        left = ("LOWER(%s)" % self.visit(l)) if lowered_left else self.visit(l)
        return "(%s LIKE %s ESCAPE '%s')" % (left, rendered, escape)

    def op_like(self, l, r, opts):
        """Render ``(left LIKE right ESCAPE '...')`` — case-sensitive match."""
        return self._like_render(l, r, opts.get("escape"), lowered_left=False)

    def op_ilike(self, l, r, opts):
        """Render ``(LOWER(left) LIKE lower(right) ESCAPE '...')`` — case-insensitive match."""
        return self._like_render(l, r, opts.get("escape"), lowered_left=True)

    def op_startswith(self, l, r, _):
        """Render ``(left LIKE 'pat%' ESCAPE '\\\\')``."""
        if not isinstance(r, ast.Literal):
            raise NotImplementedError("startswith on non-literal not supported")
        pat = self._like_escape(str(r.value)) + "%"
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.visit(l),
            self._represent(pat, "string"),
        )

    def op_endswith(self, l, r, _):
        """Render ``(left LIKE '%pat' ESCAPE '\\\\')``."""
        if not isinstance(r, ast.Literal):
            raise NotImplementedError("endswith on non-literal not supported")
        pat = "%" + self._like_escape(str(r.value))
        return "(%s LIKE %s ESCAPE '\\')" % (
            self.visit(l),
            self._represent(pat, "string"),
        )

    def op_contains(self, l, r, opts):
        """
        Substring match, type-aware.

        Mirrors SQLDialect.contains: for list:* types the pattern is
        wrapped in pipe-delimiters and pipes are doubled. Otherwise
        it's a plain ``%pattern%`` LIKE/ILIKE.
        """
        case_sensitive = opts.get("case_sensitive", False)
        # Prefer the type baked in by the translator; fall back to a
        # type-from-AST guess if the BinOp was hand-built.
        ltype = opts.get("left_type") or self._left_type(l)
        if ltype and ltype.startswith("list:") and isinstance(r, ast.Literal):
            raw = str(r.value).replace("|", "||")
            pat = "%|" + self._like_escape(raw) + "|%"
        elif isinstance(r, ast.Literal):
            pat = "%" + self._like_escape(str(r.value)) + "%"
        else:
            raise NotImplementedError("contains on non-literal not supported")
        new_r = ast.Literal(pat, "string")
        op = self.op_like if case_sensitive else self.op_ilike
        return op(l, new_r, {"escape": "\\"})

    @staticmethod
    def _left_type(l: ast.Node) -> Optional[str]:
        """
        Best-effort: pull a pydal field type out of the left operand.

        For FieldRef we don't have type info in the AST; the translator
        already burned it into Literal.type when relevant. Callers that
        need it can subclass.
        """
        if isinstance(l, ast.Literal):
            return l.type
        return None

    # =====================================================================
    # JSON / GIS — left out of the base; defined in dialect-specific
    # compiler subclasses where they exist. The base raises a clear
    # NotImplementedError via the dispatch table.
    # =====================================================================

    # =====================================================================
    # Simple unary functions
    # =====================================================================

    def un_lower(self, x, _):
        """Render ``LOWER(operand)``."""
        return "LOWER(%s)" % self.visit(x)

    def un_upper(self, x, _):
        """Render ``UPPER(operand)``."""
        return "UPPER(%s)" % self.visit(x)

    def un_length(self, x, _):
        """Render ``LENGTH(operand)``."""
        return "LENGTH(%s)" % self.visit(x)

    def un_epoch(self, x, _):
        """Render ``EXTRACT(epoch FROM operand)`` — SQLite overrides via ``web2py_extract``."""
        return "EXTRACT(epoch FROM %s)" % self.visit(x)

    def un_coalesce_zero(self, x, _):
        """Render ``COALESCE(operand, 0)`` — common idiom for summing nullables."""
        return "COALESCE(%s,0)" % self.visit(x)

    # =====================================================================
    # Multi-arg / named functions
    # =====================================================================

    def fn_aggregate(self, args, opts):
        """Render ``KIND(arg)`` (e.g. ``SUM(x)``) from ``opts["kind"]``."""
        kind = opts.get("kind", "")
        return "%s(%s)" % (kind, self.visit(args[0]))

    def fn_count(self, args, opts):
        """Render ``COUNT(arg)`` or ``COUNT(DISTINCT arg)``."""
        if opts.get("distinct"):
            return "COUNT(DISTINCT %s)" % self.visit(args[0])
        return "COUNT(%s)" % self.visit(args[0])

    def fn_cast(self, args, opts):
        """Render ``CAST(arg AS <type>)`` using ``opts["to"]``."""
        return "CAST(%s AS %s)" % (self.visit(args[0]), opts.get("to", ""))

    def fn_extract(self, args, opts):
        """Render ``EXTRACT(<unit> FROM arg)`` — SQLite overrides for ``web2py_extract``."""
        return "EXTRACT(%s FROM %s)" % (opts.get("unit", ""), self.visit(args[0]))

    def fn_replace(self, args, _):
        """Render ``REPLACE(arg0, arg1, arg2)``."""
        return "REPLACE(%s,%s,%s)" % (
            self.visit(args[0]),
            self.visit(args[1]),
            self.visit(args[2]),
        )

    def fn_coalesce(self, args, _):
        """Render ``COALESCE(arg0, arg1, ...)`` over an arbitrary arg list."""
        return "COALESCE(%s)" % ",".join(self.visit(a) for a in args)

    def fn_substring(self, args, _):
        """
        Render ``SUBSTR(field, pos, length)``.

        ``SQLDialect.substring`` emits the args directly (not via
        ``expand``); the translator stores ``pos``/``length`` as
        Literal/Raw nodes accordingly.
        """
        return "SUBSTR(%s,%s,%s)" % (
            self.visit(args[0]),
            self.visit(args[1]),
            self.visit(args[2]),
        )

    def fn_case(self, args, _):
        """Render ``CASE WHEN <query> THEN <true_val> ELSE <false_val> END``."""
        return "CASE WHEN %s THEN %s ELSE %s END" % (
            self.visit(args[0]),
            self._render_case_branch(args[1]),
            self._render_case_branch(args[2]),
        )

    def _render_case_branch(self, node):
        """SQLDialect picks the type for case branches off Python type."""
        if isinstance(node, ast.Literal) and node.type is None:
            v = node.value
            type_ = {bool: "boolean", int: "integer", float: "double"}.get(
                type(v), "string"
            )
            return str(self._represent(v, type_))
        return self.visit(node)


__all__ = ["SQLCompiler"]
