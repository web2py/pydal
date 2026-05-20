"""
Backend-agnostic expression AST for pydal.

These nodes are pure data: no database handle, no SQL, no dialect. They are
produced by the DSL layer (Field/Expression/Query/Set, in objects.py) and
consumed by the compiler layer (SQLCompiler subclasses, forthcoming).

This module is the foundation of an in-progress refactor that moves SQL
emission out of the expression objects and into a single visitor per
backend. While the refactor is in flight this module is additive: nothing
in pydal imports from here yet. The current DialectOp-based path stays the
source of truth until the AST round-trips identically.

Design rules:

* Nodes are frozen dataclasses, hashable, picklable, equal by value.
* Op names ("eq", "and", "lower", ...) are stable strings. The compiler
  is the only place that knows how to render them.
* A node never carries a reference to ``db`` or ``adapter``. Anything that
  needs the db is part of the DSL, not the AST.
* Adding a new node or op is non-breaking. Renaming or removing one is a
  major-version change.

The op vocabulary mirrors the existing dialect method names so that the
forthcoming translator (current Expression/Query -> ast.Node) is mechanical.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Tuple


class Node:
    """
    Marker base class. Every AST node inherits from Node and is a frozen
    dataclass, so equality/hash/repr are derived from the fields.

    Node itself is intentionally empty - subclasses declare all state.
    """


# ---------------------------------------------------------------------------
# Leaves: values that don't recurse further.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldRef(Node):
    """
    Reference to a column.

    ``table`` and ``name`` are the logical (pydal) identifiers — useful
    for analysis, serialization, and round-tripping. ``sqlsafe`` is the
    pre-formatted SQL identifier (e.g. ``"t"."name"`` or
    ``"alias"."rname_value"``), captured by the translator from the
    Field object. The compiler emits ``sqlsafe`` verbatim when present,
    falling back to default quoting of ``table.name`` for hand-built
    nodes in tests.
    """

    table: str
    name: str
    sqlsafe: Optional[str] = None


@dataclass(frozen=True)
class Literal(Node):
    """
    A Python value to embed in the query. ``type`` is the pydal field
    type when known (``"string"``, ``"integer"``, ...), used by the
    compiler to pick the right representation/binding.

    The compiler decides whether to render inline (legacy pydal behavior)
    or as a bound parameter.
    """

    value: Any
    type: Optional[str] = None


@dataclass(frozen=True)
class Raw(Node):
    """
    Opaque SQL fragment, emitted verbatim.

    Backward-compat for ``Expression(db, "SOME SQL")`` and ``Set.where(str)``.
    Use sparingly; it bypasses the abstraction.
    """

    sql: str


@dataclass(frozen=True)
class Star(Node):
    """``*`` or ``<table>.*`` — wildcard column list."""

    table: Optional[str] = None


@dataclass(frozen=True)
class TableRef(Node):
    """A table appearing in a FROM/JOIN clause."""

    name: str
    alias: Optional[str] = None


# ---------------------------------------------------------------------------
# Operators.
# ---------------------------------------------------------------------------


# Tuple-of-pairs is used (rather than a dict) so the dataclass stays
# hashable and immutable. Values must be hashable primitives (str/int/bool/
# None); anything richer should be a separate node.
Opts = Tuple[Tuple[str, Any], ...]


@dataclass(frozen=True)
class BinOp(Node):
    """
    Binary operation.

    ``op`` is a lowercase identifier from this vocabulary:

    * comparison:        ``eq``, ``ne``, ``lt``, ``lte``, ``gt``, ``gte``
    * logical:           ``and``, ``or``
    * arithmetic:        ``add``, ``sub``, ``mul``, ``div``, ``mod``
    * string match:      ``like``, ``ilike``, ``regexp``,
                         ``startswith``, ``endswith``, ``contains``
    * list / structural: ``comma``, ``concat``
    * GIS predicates:    ``st_contains``, ``st_equals``, ``st_intersects``,
                         ``st_overlaps``, ``st_touches``, ``st_within``
    * GIS values:        ``st_distance``, ``st_simplify``,
                         ``st_simplifypreservetopology``, ``st_transform``
    * JSON predicates:   ``json_contains``
    * JSON accessors:    ``json_key``, ``json_key_value``,
                         ``json_path``, ``json_path_value``

    ``opts`` carries per-op modifiers (e.g. ``("escape", "\\\\")`` for
    ``like``, ``("case_sensitive", False)`` for ``contains``).
    """

    op: str
    left: Node
    right: Node
    opts: Opts = ()


@dataclass(frozen=True)
class UnaryOp(Node):
    """
    Unary operation.

    ``op`` is one of: ``not``, ``invert`` (DESC ordering),
    ``lower``, ``upper``, ``length``,
    ``epoch``, ``coalesce_zero``,
    ``st_astext``, ``st_aswkb``, ``st_x``, ``st_y``,
    ``is_null``, ``is_not_null``.
    """

    op: str
    operand: Node
    opts: Opts = ()


@dataclass(frozen=True)
class FuncCall(Node):
    """
    Named function with positional args.

    ``name`` is one of: ``cast``, ``replace``, ``extract``, ``coalesce``,
    ``substring``, ``aggregate`` (with ``("kind", "SUM"|"AVG"|...)`` in opts),
    ``case``, ``st_asgeojson``, ``st_dwithin``.
    """

    name: str
    args: Tuple[Node, ...] = ()
    opts: Opts = ()


@dataclass(frozen=True)
class InList(Node):
    """
    ``expr IN (v1, v2, ...)`` — produced by ``Field.belongs()``.

    For ``belongs(subquery)`` the single right-side node is a Select.
    """

    expr: Node
    values: Tuple[Node, ...]


@dataclass(frozen=True)
class Aliased(Node):
    """``expr AS alias`` — produced by ``with_alias()``."""

    node: Node
    alias: str
    table: Optional[str] = None


# ---------------------------------------------------------------------------
# Joins, CTEs.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Join(Node):
    """
    JOIN clause. ``kind`` is one of ``"inner"``, ``"left"``, ``"cross"``.
    ``on`` is None for cross joins.
    """

    kind: str
    target: Node              # TableRef or Select
    on: Optional[Node] = None


@dataclass(frozen=True)
class Cte(Node):
    """
    Common-table-expression: ``<name>(col1, col2, ...) AS (<select>)``.

    ``columns`` is the explicit column list emitted between the CTE
    name and the body. ``recursive_parts`` carries the UNION members
    of a recursive CTE as ``((union_type, sub_select), ...)``, where
    ``union_type`` is typically ``"UNION"`` or ``"UNION ALL"``. When
    ``recursive_parts`` is non-empty the WITH header is emitted as
    ``WITH RECURSIVE`` per dialect convention.
    """

    name: str
    select: "Select"
    columns: Tuple[str, ...] = ()
    recursive_parts: Tuple[Tuple[str, "Select"], ...] = ()


# ---------------------------------------------------------------------------
# Statements: the only nodes the compiler turns into top-level SQL.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Select(Node):
    """
    SELECT statement.

    The FROM clause is split into two parts:

    * ``sources``: tables that participate as a comma-separated table list
      at the head of the FROM clause (the "anchor cluster"). When ``joins``
      is empty this is just ``FROM t1, t2, t3``; when ``joins`` is present
      ``sources`` is conventionally a single TableRef (the anchor) and the
      cross joins are encoded explicitly in ``joins``.
    * ``joins``: ordered tuple of Join nodes appended after ``sources``,
      each rendered as ``CROSS JOIN t`` / ``JOIN t ON c`` / ``LEFT JOIN t ON c``.

    ``limit`` is ``(offset, end)`` matching pydal's existing ``limitby`` tuple.
    ``distinct`` is False, True, or a Node (for ``DISTINCT ON``).
    """

    fields: Tuple[Node, ...]
    sources: Tuple[Node, ...] = ()
    joins: Tuple["Join", ...] = ()
    where: Optional[Node] = None
    groupby: Tuple[Node, ...] = ()
    having: Optional[Node] = None
    orderby: Tuple[Node, ...] = ()
    limit: Optional[Tuple[int, int]] = None
    distinct: Any = False
    for_update: bool = False
    with_cte: Tuple[Cte, ...] = ()
    # ``correlated`` matters only when this Select is nested inside
    # another: True (default) lets the outer scope's tables be pruned
    # from this Select's FROM clause (the subquery may reference outer
    # tables in WHERE). False forces this Select to declare all its own
    # tables — used for fully independent subqueries.
    correlated: bool = True
    # ``outer_scope`` lets a caller pre-seed the compiler's scope stack
    # with extra tablenames that should be treated as already-in-scope
    # — used by ``Select._compile(outer_scoped=[...])`` so a nested
    # subquery doesn't re-declare the parent's tables in its FROM.
    # Distinct from ``correlated``: ``correlated`` controls whether
    # pruning happens at all; ``outer_scope`` adds names to prune with.
    outer_scope: Tuple[str, ...] = ()


@dataclass(frozen=True)
class Insert(Node):
    """
    INSERT statement. Multi-row when ``rows`` has more than one tuple.

    ``sqlsafe`` is the dialect-rendered identifier the compiler should
    use verbatim (matches ``table._rname``). When None, the compiler
    falls back to quoting ``table``. Required for aliased writes —
    INSERT always targets the underlying physical table, not the alias.
    """

    table: str
    cols: Tuple[str, ...]
    rows: Tuple[Tuple[Node, ...], ...]
    sqlsafe: Optional[str] = None


@dataclass(frozen=True)
class Update(Node):
    """
    UPDATE statement.

    ``sets`` is a tuple of ``(column_name, value_node)`` pairs.
    ``sqlsafe`` is the dialect-rendered identifier (per Insert.sqlsafe).
    UPDATE/DELETE typically pass through ``dialect.writing_alias`` so
    the alias is preserved (``"orig" AS "alias"``) or rejected on
    dialects that don't allow aliased writes (SQLite).
    """

    table: str
    sets: Tuple[Tuple[str, Node], ...]
    where: Optional[Node] = None
    sqlsafe: Optional[str] = None


@dataclass(frozen=True)
class Delete(Node):
    """DELETE statement."""

    table: str
    where: Optional[Node] = None
    sqlsafe: Optional[str] = None


@dataclass(frozen=True)
class Count(Node):
    """
    SELECT COUNT(...) wrapper around an inner Select.

    Modeled separately so the compiler can produce ``COUNT(*)`` /
    ``COUNT(DISTINCT ...)`` without round-tripping through Select.fields.
    """

    query: Select
    distinct: Optional[Node] = None


# ---------------------------------------------------------------------------
# Convenience: tuple of every public node type, for `isinstance` checks
# and exhaustive switches in compilers.
# ---------------------------------------------------------------------------

NODE_TYPES: Tuple[type, ...] = (
    FieldRef,
    Literal,
    Raw,
    Star,
    TableRef,
    BinOp,
    UnaryOp,
    FuncCall,
    InList,
    Aliased,
    Join,
    Cte,
    Select,
    Insert,
    Update,
    Delete,
    Count,
)

__all__ = [t.__name__ for t in NODE_TYPES] + ["Node", "NODE_TYPES", "Opts"]
