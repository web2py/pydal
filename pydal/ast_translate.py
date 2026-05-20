"""
Translate pydal's current Expression/Query/Field objects into ast.Node.

This is a layer-2a building block of the staged refactor: it lets us
materialize the AST representation of any DSL expression for inspection
and for forthcoming compilers, while the in-place DialectOp path stays
the source of truth for actual SQL emission.

The mapping is mechanical and key-driven: every Expression/Query carries
a ``DialectOp`` whose ``__name__`` matches one of the op vocabularies
documented on ``ast.BinOp``/``ast.UnaryOp``/``ast.FuncCall``. Translation
is one dispatch table plus a handful of structural fold-ins (``eq None``
collapses to ``is_null``, ``like`` carries its ``escape`` in opts, ...).

Subqueries (a pydal ``Select`` appearing on the right of ``belongs``) are
left as a placeholder until the statement-level translator lands.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence, Tuple

from . import ast
from .helpers.methods import merge_tablemaps, use_common_filters, xorify
from .objects import DialectOp, Expression, Field, Query, Select, Table


# Op names that translate as straight BinOp(name, left, right) with no
# opts and no structural transformation.
_PLAIN_BINOPS = frozenset(
    {
        "lt",
        "lte",
        "gt",
        "gte",
        "sub",
        "mul",
        "div",
        "mod",
        "comma",
        "startswith",
        "endswith",
        "st_contains",
        "st_equals",
        "st_intersects",
        "st_overlaps",
        "st_touches",
        "st_within",
        "st_distance",
        "st_simplify",
        "st_simplifypreservetopology",
        "st_transform",
        "json_key",
        "json_key_value",
        "json_path",
        "json_path_value",
        "json_contains",
    }
)

# Op names that translate as straight UnaryOp(name, operand).
_PLAIN_UNARYOPS = frozenset(
    {
        "invert",
        "lower",
        "upper",
        "length",
        "epoch",
        "coalesce_zero",
        "st_astext",
        "st_aswkb",
        "st_x",
        "st_y",
    }
)


def to_ast(value: Any, type_hint: Optional[str] = None) -> ast.Node:
    """
    Translate a pydal value into an AST node.

    Accepts ``Field``, ``Expression``, ``Query``, ``Select`` (pydal's
    ``nested_select`` object), an already-built ``ast.Node`` (from
    ``Set.subselect``), plus plain Python values (treated as
    ``Literal``). ``type_hint`` is the expected pydal type for primitive
    values; it propagates through to ``Literal.type`` so the compiler
    can pick the right representation.
    """
    if isinstance(value, ast.Node):
        # Already an AST node (e.g. from set.subselect()). Pass through.
        return value
    if isinstance(value, Field):
        # ``value.sqlsafe`` already encodes the table's sql_shortref +
        # ``field._rname``, honoring user rname= and table aliasing. We
        # bake it into the node so the compiler doesn't need to walk
        # back through pydal metadata to render the column reference.
        return ast.FieldRef(value._tablename, value.name, sqlsafe=value.sqlsafe)
    if isinstance(value, Select):
        return _select_to_ast(value)
    if isinstance(value, (Expression, Query)):
        return _expr_to_ast(value)
    return ast.Literal(value, type_hint)


def _select_to_ast(sel: Select) -> ast.Node:
    """
    Translate pydal's ``Select`` (from ``nested_select``) into ast.Select.

    Bridges the legacy ``set.nested_select(...) -> belongs(...)`` path
    onto the new AST pipeline. New code should prefer ``set.subselect``,
    which produces the ast.Select directly.
    """
    from .objects import Set as _Set
    s = _Set(sel._db, sel._query)
    node = set_to_select(s, sel._qfields, sel._attributes)
    # The pydal Select consumed ``correlated`` from attributes at
    # __init__; propagate it onto the AST so the compiler can prune
    # outer-scoped tables from this Select's FROM clause.
    if not getattr(sel, "_correlated", True):
        import dataclasses
        node = dataclasses.replace(node, correlated=False)
    return node


def _field_type(node: Any) -> Optional[str]:
    """Best-effort field-type hint for a left operand."""
    t = getattr(node, "type", None)
    return t if isinstance(t, str) else None


def _expr_to_ast(expr) -> ast.Node:
    op = expr.op
    f, s = expr.first, expr.second

    # Raw SQL string carried as an Expression with op=<sql>. The original
    # _expand path wraps it in parens and strips a trailing ";" — bake
    # that shape into the Raw node so the compiler can emit verbatim.
    if isinstance(op, str):
        sql = op[:-1] if op.endswith(";") else op
        return ast.Raw("(%s)" % sql)

    name = getattr(op, "__name__", None)
    if name is None:
        # Defensive: unknown op shape. Preserve as opaque function call.
        return ast.FuncCall(repr(op), _args_of(f, s))

    # ---------- logical ----------
    if name == "_and":
        return ast.BinOp("and", to_ast(f), to_ast(s))
    if name == "_or":
        return ast.BinOp("or", to_ast(f), to_ast(s))
    if name == "_not":
        return ast.UnaryOp("not", to_ast(f))

    # ---------- alias ----------
    if name == "_as":
        return ast.Aliased(
            to_ast(f),
            s if isinstance(s, str) else str(s),
            table=getattr(expr, "tablename", None),
        )

    # ---------- comparisons (fold None into is_null/is_not_null) ----------
    if name in ("eq", "ne"):
        if s is None:
            return ast.UnaryOp("is_null" if name == "eq" else "is_not_null", to_ast(f))
        return ast.BinOp(name, to_ast(f), to_ast(s, type_hint=_field_type(f)))

    # ---------- string match with opts ----------
    oa = getattr(expr, "optional_args", None) or {}
    if name in ("like", "ilike"):
        opts: ast.Opts = (("escape", oa["escape"]),) if "escape" in oa else ()
        return ast.BinOp(
            name, to_ast(f), to_ast(s, type_hint="string"), opts=opts
        )
    if name == "regexp":
        opts = (
            (("match_parameter", oa["match_parameter"]),)
            if "match_parameter" in oa
            else ()
        )
        return ast.BinOp("regexp", to_ast(f), to_ast(s), opts=opts)
    if name == "contains":
        opts2: ast.Opts = ()
        ftype = _field_type(f)
        if ftype:
            # SQLDialect.contains needs the left type to decide between
            # plain ``%pat%`` and the list:* pipe-wrapped ``%|pat|%`` —
            # bake it into the AST so the compiler doesn't need to look
            # up the schema.
            opts2 += (("left_type", ftype),)
        if "case_sensitive" in oa:
            opts2 += (("case_sensitive", oa["case_sensitive"]),)
        return ast.BinOp("contains", to_ast(f), to_ast(s), opts=opts2)

    # ---------- belongs / IN ----------
    if name == "belongs":
        ftype = _field_type(f)
        if isinstance(s, ast.Node):
            # AST-native subquery from Set.subselect(...).
            return ast.InList(to_ast(f), (s,))
        if isinstance(s, Select):
            # Legacy nested_select(...) — bridge to AST.
            return ast.InList(to_ast(f), (_select_to_ast(s),))
        if isinstance(s, str):
            # belongs() with a raw SQL fragment (rare, escape hatch).
            # SQLDialect.belongs drops the trailing char (typically a ";").
            return ast.InList(to_ast(f), (ast.Raw(s[:-1] if s.endswith(";") else s),))
        if isinstance(s, (list, tuple)):
            return ast.InList(
                to_ast(f), tuple(to_ast(v, type_hint=ftype) for v in s)
            )
        return ast.InList(to_ast(f), (to_ast(s, type_hint=ftype),))

    # ---------- arithmetic add: type-overloaded (numeric +, string ||) ----------
    if name == "add":
        ftype = _field_type(f)
        opts = (("left_type", ftype),) if ftype else ()
        return ast.BinOp("add", to_ast(f), to_ast(s, type_hint=ftype), opts=opts)

    # ---------- COUNT(field [, distinct]) ----------
    if name == "count":
        if s:
            return ast.FuncCall("count", (to_ast(f),), opts=(("distinct", True),))
        return ast.FuncCall("count", (to_ast(f),))

    # ---------- plain BinOps ----------
    if name in _PLAIN_BINOPS:
        return ast.BinOp(name, to_ast(f), to_ast(s, type_hint=_field_type(f)))

    # ---------- plain UnaryOps ----------
    if name in _PLAIN_UNARYOPS:
        return ast.UnaryOp(name, to_ast(f))

    # ---------- functions with structured `second` ----------
    if name == "aggregate":
        # second is a kind string: "SUM", "AVG", "MIN", "MAX", "ABS"
        return ast.FuncCall("aggregate", (to_ast(f),), opts=(("kind", s),))

    if name == "cast":
        # second is the destination SQL type fragment
        return ast.FuncCall("cast", (to_ast(f),), opts=(("to", s),))

    if name == "extract":
        # second is the unit: "year"|"month"|"day"|"hour"|"minute"|"second"|"epoch"
        return ast.FuncCall("extract", (to_ast(f),), opts=(("unit", s),))

    if name == "replace":
        a, b = s
        return ast.FuncCall(
            "replace",
            (to_ast(f), to_ast(a, type_hint="string"), to_ast(b, type_hint="string")),
        )

    if name == "coalesce":
        ftype = _field_type(f)
        others = s or ()
        return ast.FuncCall(
            "coalesce",
            (to_ast(f),) + tuple(to_ast(v, type_hint=ftype) for v in others),
        )

    if name == "substring":
        pos, length = s
        # Expression.__getitem__ pre-builds the length (and sometimes pos)
        # as a SQL fragment string for negative slices. Honor that by
        # wrapping bare strings as Raw rather than coercing them to a
        # typed Literal — until __getitem__ is rewritten to use the AST,
        # this is the only place the leak shows up.
        def _slice_arg(v):
            if isinstance(v, str):
                return ast.Raw(v)
            return to_ast(v, type_hint="integer")
        return ast.FuncCall(
            "substring", (to_ast(f), _slice_arg(pos), _slice_arg(length))
        )

    if name == "case":
        # first is the query, second is (true_value, false_value)
        t_val, f_val = s
        return ast.FuncCall("case", (to_ast(f), to_ast(t_val), to_ast(f_val)))

    if name == "st_asgeojson":
        # second is a dict {"precision": ..., "options": ...}
        opts = tuple(sorted(s.items())) if isinstance(s, dict) else ()
        return ast.FuncCall("st_asgeojson", (to_ast(f),), opts=opts)

    if name == "st_dwithin":
        other, distance = s
        return ast.FuncCall(
            "st_dwithin",
            (to_ast(f), to_ast(other), to_ast(distance, type_hint="double")),
        )

    # ---------- fallback: opaque function call ----------
    return ast.FuncCall(name, _args_of(f, s))


def _args_of(f, s):
    args = ()
    if f is not None:
        args += (to_ast(f),)
    if s is not None:
        args += (to_ast(s),)
    return args


# ---------------------------------------------------------------------------
# Statement-level translators (Set/Table -> ast.Select/Insert/Update/Delete/
# Count). Layer 2c. They reuse the adapter's table discovery, common-filter
# application, and field expansion so we stay on the same plumbing as the
# current path while the new pipeline is verified byte-for-byte against it.
# ---------------------------------------------------------------------------


_UNSUPPORTED_SELECT_ATTRS = ("with_cte", "cte_collector")


def set_to_select(
    s,
    fields=(),
    attrs: Optional[Mapping[str, Any]] = None,
    _in_cte_body: bool = False,
) -> ast.Select:
    """
    Translate ``Set._select(*fields, **attrs)`` into an ast.Select node.

    Supports: single-table SELECT, multi-table queries (implicit cross),
    ``join=`` with one or more ``.on()`` expressions, ``left=`` with one
    or more ``.on()`` expressions, CTEs (``set.cte(...)``), plus all the
    simple knobs (orderby/groupby/having/limit/distinct/for_update/
    common-filter).

    ``_in_cte_body`` is an internal flag: when True, CTE Selects in the
    tablemap are treated as plain TableRefs rather than materialized
    into ``with_cte``. The outermost translator extracts all CTEs; CTE
    bodies and recursive parts inherit them by name only.

    Out of scope (raise NotImplementedError):
      * bare tables in ``join=`` / ``left=`` (the rare/buggy weird forms)
      * simultaneous ``join=`` and ``left=`` (uncommon)
    """
    attrs = dict(attrs) if attrs else {}
    # ``correlated`` is a per-Select hint, not a SQL clause; pop it
    # before validation and apply at the end. Default True matches the
    # ast.Select default.
    correlated_flag = attrs.pop("correlated", True)
    for unsupported in _UNSUPPORTED_SELECT_ATTRS:
        if attrs.get(unsupported):
            raise NotImplementedError(
                "set_to_select: %r is not yet supported" % unsupported
            )

    db = s.db
    adapter = db._adapter
    query = s.query
    join_param = attrs.get("join")
    left_param = attrs.get("left")
    if join_param and left_param:
        raise NotImplementedError(
            "set_to_select: simultaneous join= and left= not yet supported"
        )

    # ---- 1) expand_all needs the WIDE tablemap (including joins) so
    # SQLALL placeholders can resolve. ----
    expand_scope = adapter.tables(
        query, join_param, left_param,
        attrs.get("orderby"), attrs.get("groupby"),
    )
    expanded_fields = list(adapter.expand_all(fields, expand_scope))

    # ---- 2) but ``query_tables`` (drives default-orderby-on-limit etc.)
    # is captured from JUST query+fields, mirroring _select_wcols. ----
    tablemap = adapter.tables(query)
    if use_common_filters(query):
        query = adapter.common_filter(query, list(tablemap.values()))
    tablemap = merge_tablemaps(tablemap, adapter.tables(*expanded_fields))

    if not tablemap:
        raise SyntaxError("Set: no tables selected")

    query_tables = list(tablemap)

    # ---- 2a) extract CTEs from the tablemap. Any pydal Select with
    # is_cte=True becomes an ast.Cte appended to ``with_cte``; the
    # corresponding source becomes a plain TableRef of the CTE name.
    # Skipped inside CTE bodies / recursive parts — those reference the
    # CTE by name only; the definition lives on the outer query. ----
    ctes = () if _in_cte_body else _extract_ctes(tablemap)

    # ---- 2b) compute join structure ----
    sources, joins = _build_from_clause(adapter, tablemap, query_tables, join_param, left_param)

    # ---- 3) expression-level pieces ----
    ast_fields = tuple(to_ast(_geofy(f)) for f in expanded_fields)
    ast_where = to_ast(query) if query else None
    ast_orderby = _xorify_to_ast(attrs.get("orderby"))
    ast_groupby = _xorify_to_ast(attrs.get("groupby"))
    ast_having = to_ast(attrs["having"]) if attrs.get("having") else None

    limitby = attrs.get("limitby")
    if (
        limitby
        and not ast_groupby
        and query_tables
        and attrs.get("orderby_on_limitby", True)
        and not ast_orderby
    ):
        ast_orderby = _default_orderby(tablemap, query_tables)

    distinct = attrs.get("distinct", False)
    if not distinct:
        # None, False, "" all mean "no DISTINCT" — match SQLDialect.select.
        ast_distinct: Any = False
    elif distinct is True:
        ast_distinct = True
    else:
        # DISTINCT ON (...). A list/tuple of fields/expressions is
        # flattened via ``xorify`` into a comma-chain BinOp (same trick
        # ``_count`` uses) so the compiler emits ``(a, b, c)``.
        if isinstance(distinct, (list, tuple)):
            distinct = xorify(distinct)
        ast_distinct = to_ast(distinct)

    return ast.Select(
        fields=ast_fields,
        sources=sources,
        joins=joins,
        where=ast_where,
        groupby=ast_groupby,
        having=ast_having,
        orderby=ast_orderby,
        limit=tuple(limitby) if limitby else None,
        distinct=ast_distinct,
        for_update=bool(attrs.get("for_update")),
        with_cte=ctes,
        correlated=bool(correlated_flag),
        outer_scope=tuple(attrs.get("outer_scoped") or ()),
    )


def _build_from_clause(adapter, tablemap, query_tables, join_param, left_param):
    """
    Compute (sources, joins) by replaying SQLAdapter._select_wcols' logic.

    Reuses ``adapter._build_joins_for_select`` so the table-classification
    is identical to the current path. The output is a pure-AST split:

      * ``sources``: anchor table list (comma-separated in the FROM clause)
      * ``joins``: ordered Join nodes appended after the anchor

    Bare tables in join= / left= are explicitly rejected with
    NotImplementedError to keep the supported surface clean.
    """
    if not (join_param or left_param):
        sources = tuple(_table_to_ref(tablemap[t]) for t in query_tables)
        return sources, ()

    if join_param:
        (
            ijoin_tables, ijoin_on, itables_to_merge, _ijoin_on_tables,
            _iimportant, iexcluded, itablemap,
        ) = adapter._build_joins_for_select(tablemap, join_param)
        tablemap = merge_tablemaps(tablemap, itables_to_merge)
        tablemap = merge_tablemaps(tablemap, itablemap)

        cross_pool = iexcluded + list(itables_to_merge)
        if not cross_pool:
            raise SyntaxError("Set: join= without an anchor table")
        sources = (_table_to_ref(tablemap[cross_pool[0]]),)
        joins = []
        for t in cross_pool[1:]:
            joins.append(ast.Join("cross", _table_to_ref(tablemap[t]), None))
        # Bare tables in join= -> explicit CROSS JOIN. (Legacy pydal
        # silently drops these; emitting CROSS JOIN is the standard
        # interpretation and matches what users almost certainly intend.)
        for tname in ijoin_tables:
            joins.append(ast.Join("cross", _table_to_ref(itablemap[tname]), None))
        for expr in ijoin_on:
            joins.append(_join_from_on("inner", expr, adapter))
        return sources, tuple(joins)

    # left_param
    (
        join_tables, join_on, tables_to_merge, _join_on_tables,
        _important, excluded, jtablemap,
    ) = adapter._build_joins_for_select(tablemap, left_param)
    tablemap = merge_tablemaps(tablemap, tables_to_merge)
    tablemap = merge_tablemaps(tablemap, jtablemap)

    cross_pool = excluded + list(tables_to_merge)
    if not cross_pool:
        raise SyntaxError("Set: left= without an anchor table")
    sources = (_table_to_ref(tablemap[cross_pool[0]]),)
    joins = []
    for t in cross_pool[1:]:
        joins.append(ast.Join("cross", _table_to_ref(tablemap[t]), None))
    # Bare tables in left= -> LEFT JOIN <table> ON TRUE (an unconstrained
    # LEFT JOIN). Legacy pydal emits ``LEFT JOIN t1,t2`` which is
    # invalid SQL on most backends; the standard form is much safer.
    for tname in join_tables:
        joins.append(
            ast.Join("left", _table_to_ref(jtablemap[tname]), ast.Literal(True))
        )
    for expr in join_on:
        joins.append(_join_from_on("left", expr, adapter))
    return sources, tuple(joins)


def _join_from_on(kind: str, expr, adapter) -> ast.Join:
    """
    Build a Join node from a ``table.on(condition)`` Expression.

    Mirrors SQLDialect.on: apply the join target's common_filter to the
    ON condition if needed, then translate.
    """
    target_table = expr.first
    on_cond = expr.second
    if use_common_filters(on_cond):
        on_cond = adapter.common_filter(on_cond, [target_table])
    return ast.Join(
        kind=kind,
        target=_table_to_ref(target_table),
        on=to_ast(on_cond),
    )


def table_to_insert(table: Table, op_values: Sequence) -> ast.Insert:
    """
    Translate ``Table._insert(...)``'s op_values list into ast.Insert.

    ``sqlsafe`` is set from ``table._rname`` — INSERT always targets the
    underlying physical table, ignoring any DSL-level alias (matching
    legacy ``SQLAdapter._insert`` which passes ``table._rname`` to the
    dialect directly).
    """
    cols = tuple(field.name for field, _ in op_values)
    row = tuple(to_ast(value, type_hint=field.type) for field, value in op_values)
    return ast.Insert(
        table=table._dalname,
        cols=cols,
        rows=(row,) if row else (),
        sqlsafe=table._rname,
    )


def set_to_update(s, op_values: Sequence) -> ast.Update:
    """
    Translate ``Set._update(**fields)`` into ast.Update.

    Mirrors ``SQLAdapter._update``: applies common filters; field types
    are propagated into Literal nodes so the compiler picks the right
    representation. ``sqlsafe`` is pre-baked from
    ``dialect.writing_alias`` so SQLite's "no aliased writes" rule
    fires at translation time, same as the legacy path.
    """
    db = s.db
    adapter = db._adapter
    table = adapter.get_table(s.query)
    # The dialect's writing_alias may raise (e.g. SQLite rejects
    # aliased UPDATE/DELETE). Capture it now so the AST carries the
    # correct identifier and the compiler never re-derives it.
    sqlsafe = adapter.dialect.writing_alias(table)
    query = s.query
    if use_common_filters(query):
        query = adapter.common_filter(query, [table])

    sets = tuple(
        (field.name, to_ast(value, type_hint=field.type))
        for field, value in op_values
    )
    return ast.Update(
        table=table._dalname,
        sets=sets,
        where=to_ast(query) if query else None,
        sqlsafe=sqlsafe,
    )


def set_to_delete(s) -> ast.Delete:
    """
    Translate ``Set._delete()`` into ast.Delete.

    ``sqlsafe`` follows the same rule as set_to_update: pre-baked from
    ``dialect.writing_alias``.
    """
    db = s.db
    adapter = db._adapter
    table = adapter.get_table(s.query)
    sqlsafe = adapter.dialect.writing_alias(table)
    query = s.query
    if use_common_filters(query):
        query = adapter.common_filter(query, [table])
    return ast.Delete(
        table=table._dalname,
        where=to_ast(query) if query else None,
        sqlsafe=sqlsafe,
    )


def set_to_count(s, distinct=None) -> ast.Count:
    """Translate ``Set._count(distinct=...)`` into ast.Count."""
    db = s.db
    adapter = db._adapter
    query = s.query
    tablemap = adapter.tables(query)
    tables = list(tablemap.values())
    if use_common_filters(query):
        query = adapter.common_filter(query, tables)

    if len(tablemap) != 1:
        raise NotImplementedError(
            "set_to_count: multi-table count lands in the next sub-layer"
        )
    table_node = _table_to_ref(tables[0])

    inner = ast.Select(
        fields=(ast.Star(),),
        sources=(table_node,),
        where=to_ast(query) if query else None,
    )

    if not distinct:
        return ast.Count(query=inner, distinct=None)

    if isinstance(distinct, (list, tuple)):
        distinct = xorify(distinct)
    return ast.Count(query=inner, distinct=to_ast(distinct))


# ---------- helpers ----------


def _table_to_ref(t) -> ast.Node:
    if isinstance(t, Select):
        # CTE: just reference by name; the definition lives in with_cte.
        if getattr(t, "is_cte", False):
            return ast.TableRef(t._tablename)
        # Non-CTE Select (aliased subquery as join source). Handled in
        # the compiler as ``(SELECT ...) AS alias``.
        if t._tablename:
            return _select_obj_to_subsource_ast(t)
        raise NotImplementedError(
            "set_to_select: unaliased Select as source — needs .with_alias(name)"
        )
    # Aliased tables: _tablename is the alias, _dalname is the original.
    # Carry both into the AST so the compiler can emit "orig AS alias".
    if t._tablename != t._dalname:
        return ast.TableRef(t._dalname, alias=t._tablename)
    return ast.TableRef(t._tablename)


def _extract_ctes(tablemap):
    """
    For each Select with is_cte=True in the tablemap, build an
    ast.Cte. The corresponding tablemap entry stays in place so the
    sources/joins builders see it as a (CTE-name) TableRef.
    """
    ctes = []
    for tname in list(tablemap):
        t = tablemap[tname]
        if isinstance(t, Select) and getattr(t, "is_cte", False):
            ctes.append(_cte_to_ast(t))
    return tuple(ctes)


def _cte_to_ast(sel) -> ast.Cte:
    """
    Translate a pydal Select with is_cte=True into ast.Cte.

    The body Select is translated with correlated=False so the
    compiler's outer-scope pruning leaves the CTE's FROM clause alone.
    Recursive union members are translated the same way.
    """
    from .objects import Set as _Set

    def _column_name(f):
        # Mirror legacy: alias if set, else field name, else str(expr).
        if isinstance(f, Field):
            return f.name
        if isinstance(f, Expression) and getattr(f, "op", None) is not None:
            opname = getattr(f.op, "__name__", "")
            if opname == "_as":
                return f.second
        return str(f)

    columns = tuple(_column_name(f) for f in sel._qfields)

    body_attrs = dict(sel._attributes)
    body_attrs.pop("cte", None)
    body_ast = set_to_select(
        _Set(sel._db, sel._query), sel._qfields, body_attrs, _in_cte_body=True
    )
    # CTE bodies are standalone — disable correlated-subquery pruning.
    import dataclasses as _dc
    body_ast = _dc.replace(body_ast, correlated=False)

    recursive_parts: Tuple[Tuple[str, ast.Select], ...] = ()
    if getattr(sel, "_cte_recursive", None):
        parts = []
        for union_type, rec in sel._cte_recursive:
            if isinstance(rec, Select):
                rec_attrs = dict(rec._attributes)
                rec_attrs.pop("cte", None)
                rec_ast = set_to_select(
                    _Set(rec._db, rec._query), rec._qfields, rec_attrs,
                    _in_cte_body=True,
                )
                rec_ast = _dc.replace(rec_ast, correlated=False)
                parts.append((union_type, rec_ast))
            else:
                # raw SQL recursive part (legacy after _compile)
                raise NotImplementedError(
                    "Cte recursive part is not a Select object"
                )
        recursive_parts = tuple(parts)

    return ast.Cte(
        name=sel._tablename,
        select=body_ast,
        columns=columns,
        recursive_parts=recursive_parts,
    )


def _select_obj_to_subsource_ast(sel) -> ast.Node:
    """
    Convert an aliased pydal Select (used as a FROM-clause source)
    into an ast.Aliased wrapping an ast.Select. The compiler renders
    this as ``(SELECT ...) AS alias``.
    """
    inner = _select_to_ast(sel)
    return ast.Aliased(inner, sel._tablename)


def _xorify_to_ast(value):
    if not value:
        return ()
    if isinstance(value, (list, tuple)):
        value = xorify(value)
    return (to_ast(value),)


def _default_orderby(tablemap, query_tables):
    """
    Mirror _select_wcols' default-orderby-on-limit fallback: primary
    keys of the participating tables.
    """
    out = []
    for tname in query_tables:
        t = tablemap[tname]
        if isinstance(t, Select):
            continue
        pks = getattr(t, "_primarykey", None) or ["_id"]
        for pk in pks:
            out.append(to_ast(t[pk]))
    return tuple(out)


def _geofy(field):
    """Mirror SQLAdapter._geoexpand: substitute st_astext() for geo fields."""
    if (
        isinstance(field, Field)
        and isinstance(field.type, str)
        and field.type.startswith("geo")
    ):
        return field.st_astext()
    return field


__all__ = [
    "to_ast",
    "set_to_select",
    "set_to_update",
    "set_to_delete",
    "set_to_count",
    "table_to_insert",
]
