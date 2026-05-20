"""
Natural-language ``Query`` parser.

``QueryBuilder`` turns a free-text expression like
``"name is equal to Chair and price > 10"`` into a pydal ``Query`` you
can pass to ``db(...)``.

Recognized vocabulary (extensible via ``token_aliases``):

* logical operators: ``not``, ``and``, ``or``;
* comparison: ``==``, ``!=``, ``<``, ``>``, ``<=``, ``>=``,
  ``is``, ``is equal``, ``is greater than``, ``is less or equal``, ...;
* string: ``contains``, ``starts with``;
* list-membership: ``belongs``, ``in``;
* unary predicates: ``is null``, ``is not null``, ``is true``,
  ``is false``;
* field modifiers: ``upper``, ``lower``.

Values may be bare (no spaces, no quotes) or double-quoted to allow
spaces / commas.

See ``README.md`` for examples; the source-level regexes are the
authoritative grammar.
"""

import re
from typing import Any, Dict, Optional, Set

from .objects import Field
from .validators import (
    IS_DATE,
    IS_DATETIME,
    IS_FLOAT_IN_RANGE,
    IS_INT_IN_RANGE,
    IS_TIME,
)


class QueryParseError(RuntimeError):
    """Raised by ``QueryBuilder.parse`` when input can't be parsed."""


def validate(field: Field, value: Any) -> None:
    """
    Best-effort range/format check before building a ``Query``.

    Picks the appropriate ``IS_*`` validator for the field's pydal type
    (id/reference -> non-negative int; numeric -> in-range; date/time
    -> ISO-format). Raises ``QueryParseError`` on failure; types
    without a known validator pass through unchecked.
    """
    error = None
    if (
        field.type == "id"
        or field.type.startswith("reference")
        or field.type.startswith("list:reference")
    ):
        error = IS_INT_IN_RANGE(0)(value)[1]
    elif field.type == "integer" or field.type == "list:integer":
        error = IS_INT_IN_RANGE()(value)[1]
    elif field.type == "double" or field.type.startswith("decimal"):
        error = IS_FLOAT_IN_RANGE()(value)[1]
    elif field.type == "time":
        error = IS_TIME()(value)[1]
    elif field.type == "date":
        error = IS_DATE()(value)[1]
    elif field.type == "datetime":
        error = IS_DATETIME()(value)[1]
    if error:
        raise QueryParseError(f"{value}: {error}")


class QueryBuilder:
    """
    Parser for natural-language query expressions over a single table.

    Construction is cheap; you can reuse one builder across many
    ``parse`` calls. Token vocabularies are class-level constants that
    can be augmented per-instance via ``token_aliases`` (useful for
    localization).
    """

    tokens_not: Set[str] = {"not"}
    tokens_ops: Set[str] = {
        "upper",
        "lower",
        "is null",
        "is not null",
        "is true",
        "is false",
        "==",
        "!=",
        "<",
        ">",
        "<=",
        ">=",
        "belongs",
        "contains",
        "startswith",
    }
    tokens_and_or: Set[str] = {"and", "or"}
    default_token_aliases: Dict[str, str] = {
        "is": "==",
        "is equal": "==",
        "is equal to": "==",
        "is not equal": "!=",
        "is not equal to": "!=",
        "is less": "<",
        "is less than": "<",
        "is greater": ">",
        "is greater than": ">",
        "is less or equal": "<=",
        "is less or equal than": "<=",
        "is equal or less": "<=",
        "is equal or less than": "<=",
        "is greater or equal": ">=",
        "is greater or equal than": ">=",
        "is equal or greater": ">=",
        "is equal or greater than": ">=",
        "is in": "belongs",
        "in": "belongs",
        "starts with": "startswith",
    }
    # Matches a bare identifier token at the start of the input.
    re_token = re.compile(r"^(\w+)\s*(.*)$")
    # Matches a value: a double-quoted string (with ``\"`` escapes)
    # OR an unquoted run of non-space/comma characters.
    re_value = re.compile(r'^((?:")(?:[^"]*[\\]["])*[^"]*(?:")|[^" ,]*)\s*(.*)$')
    # Collapses runs of whitespace to a single space.
    re_spaces = re.compile(r"\s+")

    def __init__(
        self,
        table,
        field_aliases: Optional[Dict[str, str]] = None,
        token_aliases: Optional[Dict[str, str]] = None,
        debug: bool = False,
    ):
        """
        Construct a parser bound to ``table``.

        ``field_aliases``: map user-facing field names to their actual
        column names (e.g. for localization or for hiding the
        underlying column name). When provided, only the listed fields
        are searchable; otherwise every readable field is.

        ``token_aliases``: map natural-language phrases to one of the
        canonical operator tokens in ``tokens_ops``/``tokens_not``/
        ``tokens_and_or``. Defaults to ``default_token_aliases``.

        ``debug``: print each token match to stdout while parsing.
        """
        self.table = table
        # Either override all fields or none.
        if field_aliases:
            self.fields = {k: table[v] for k, v in field_aliases.items()}
        else:
            self.fields = {f.name.lower(): f for f in table if f.readable}
        if token_aliases is None:
            token_aliases = QueryBuilder.default_token_aliases
        # Build per-instance token dicts including aliases.
        self.tokens_not = self._augment(token_aliases, QueryBuilder.tokens_not)
        self.tokens_and_or = self._augment(token_aliases, QueryBuilder.tokens_and_or)
        self.tokens_ops = self._augment(token_aliases, QueryBuilder.tokens_ops)
        # Build token-driven regexes once per instance.
        self.re_not = re.compile(r"^(" + "|".join(self.tokens_not) + r")(\W.*)$")
        # Reverse-sort so longest tokens are tried first in the
        # alternation (so ``is null`` matches before ``is``).
        self.re_op = re.compile(
            "^("
            + "|".join(
                t.replace(" ", r"\s+") for t in sorted(self.tokens_ops, reverse=True)
            )
            + r")\s*(.*)$"
        )
        self.debug = debug

    @staticmethod
    def _augment(
        aliases: Optional[Dict[str, str]], original: Set[str]
    ) -> Dict[str, str]:
        """
        Build a ``token -> canonical`` map.

        Canonical tokens map to themselves; each alias that targets a
        canonical token is added.
        """
        output = {k: k for k in original}
        if aliases:
            output.update({k: v for k, v in aliases.items() if v in original})
        return output

    @staticmethod
    def _find_closing_bracket(text: str) -> int:
        """
        Return the index of the ``)`` matching the leading ``(``.

        Honors nested parentheses and skips ``)`` inside double-quoted
        strings (where ``\\"`` escapes a quote). Raises
        ``QueryParseError`` if brackets are unbalanced.
        """
        if text[:1] != "(":
            raise QueryParseError("Internal error: missing start bracket")
        level = 0
        quoted = False
        prev_c: Optional[str] = None
        for i, c in enumerate(text):
            if not quoted:
                if c == '"':
                    quoted = True
                elif c == "(":
                    level += 1
                elif c == ")":
                    level -= 1
                    if level == 0:
                        return i
            elif c == '"':
                if prev_c != "\\":
                    quoted = False
            prev_c = c
        raise QueryParseError("Unbalanced brackets")

    def parse(self, text: str):
        """
        Parse ``text`` into a pydal ``Query``.

        The grammar is roughly::

            query  := atom (and/or atom)*
            atom   := [not] (subexpr | predicate)
            subexpr := '(' query ')'
            predicate := field [lower|upper] op [value (',' value)*]

        Returns ``self.table.id > 0`` for empty input (i.e. "all rows").
        """
        if self.debug:
            print("PARSING", repr(text))
        # In the stack we keep queries and logical operators only.
        stack = []

        def next(text, regex, ignore=False):
            """Match one token; return ``(token, remaining)`` or raise."""
            text = text.strip()
            if not text:
                if ignore:
                    return None, text
                raise QueryParseError("Unable to parse truncated expression")
            match = regex.match(text)
            if not match:
                if ignore:
                    return None, text
                raise QueryParseError(f"Unable to parse {text}")
            is_quoted = text[:1] == '"'
            token, text = match.group(1), match.group(2)
            if is_quoted:
                token = token[1:-1]
            else:
                token = self.re_spaces.sub(" ", token)
            if self.debug:
                print("MATCH", repr(token), repr(text))
            return token, text.strip()

        while text.strip():
            negate = False
            # Optional leading ``not``.
            token, text = next(text, self.re_not, ignore=True)
            if token:
                negate = True
            # Nested expression OR a bare predicate.
            if text.startswith("("):
                i = QueryBuilder._find_closing_bracket(text)
                token, text = text[1:i], text[i + 1:].strip()
                query = self.parse(token)
            else:
                token, text = next(text, self.re_token)
                if token.lower() not in self.fields:
                    raise QueryParseError(
                        f"Unable to parse {token}, expected a field name"
                    )
                field = self.fields[token]
                is_text = field.type in ("string", "text", "blob")
                has_contains = is_text or field.type.startswith("list:")
                token, text = next(text, self.re_op)
                token = self.tokens_ops[token]
                # ``lower``/``upper`` modify the field, then expect another op.
                if is_text and token == "lower":
                    token, text = next(text, self.re_op)
                    token = self.tokens_ops.get(token)
                    field = field.lower()
                elif is_text and token == "upper":
                    token, text = next(text, self.re_op)
                    token = self.tokens_ops.get(token)
                    field = field.upper()
                if token == "is null":
                    query = field == None  # noqa: E711
                elif token == "is not null":
                    query = field != None  # noqa: E711
                elif field.type == "boolean" and token == "is true":
                    query = field == True  # noqa: E712
                elif field.type == "boolean" and token == "is false":
                    query = field == False  # noqa: E712
                else:
                    # The op requires a value — match it.
                    value, text = next(text, self.re_value)
                    validate(field, value)
                    if token == "==":
                        query = field == value
                    elif token == "!=":
                        query = field != value
                    elif token == "<":
                        query = field < value
                    elif token == ">":
                        query = field > value
                    elif token == "<=":
                        query = field <= value
                    elif token == ">=":
                        query = field >= value
                    elif has_contains and token == "contains":
                        query = field.contains(value)
                    elif is_text and token == "startswith":
                        query = field.startswith(value)
                    elif token == "belongs" and field:
                        # ``belongs`` accepts a comma-separated list.
                        value = [value]
                        while text.startswith(","):
                            token, text = next(text[1:], self.re_value)
                            validate(field, token)
                            value.append(token)
                        query = field.belongs(value)
                    else:
                        raise QueryParseError(
                            f"Unable to parse {token}, expected an operator"
                        )
            if negate:
                query = ~query
            # Combine into the running logical expression.
            if len(stack) > 1 and stack[-1] == "and":
                stack[-2:] = [stack[-2] & query]
            elif len(stack) > 1 and stack[-1] == "or":
                stack[-2:] = [stack[-2] | query]
            else:
                stack.append(query)

            # Trailing ``and``/``or`` connects the next predicate.
            token, text = next(text, self.re_token, ignore=True)
            if not token:
                break
            elif token in self.tokens_and_or and text:
                stack.append(self.tokens_and_or[token])
            else:
                raise QueryParseError(f"Unable to parse {token}, expected and/or")
        if len(stack) > 1:
            raise QueryParseError("Internal error: leftover stack")
        return stack[-1] if stack else self.table.id > 0
