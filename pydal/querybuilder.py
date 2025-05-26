from .base import DAL
from .objects import Field
from .validators import (
    IS_INT_IN_RANGE,
    IS_FLOAT_IN_RANGE,
    IS_TIME,
    IS_DATE,
    IS_DATETIME,
    IS_JSON,
)
import re


class QueryParseError(RuntimeError):
    pass


def validate(field, value):
    """Validate the value for the field"""
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
    token_not = "not"
    tokens_transform = {
        "upper": "upper",
        "lower": "lower",
    }
    tokens_op = {
        "is null": "is null",
        "is not null": "is not null",
        "==": "==",
        "!=": "!=",
        "<": "<",
        ">": ">",
        "<=": "<=",
        ">=": ">=",
        "belongs": "belongs",
        "contains": "contains",
        "startswith": "startswith",
    }
    tokens_aliases = {
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
    tokens_bool = {"and": "and", "or": "or"}
    # regex matching field names, and
    re_token = re.compile(r"^(\w+)\s*(.*)$")
    # regex matching a value or quoted value
    re_value = re.compile(r'^((?:")[^"]*(?:[\]["][^"]*)*(?:")|[^ ,]*)\s*(.*)$')
    # regex matching repeated spaces
    re_spaces = re.compile(r"\s+")

    def __init__(
        self,
        token_not=None,
        tokens_transform=None,
        tokens_op=None,
        tokens_aliases=None,
        tokens_bool=None,
        debug=False,
    ):
        self.token_not = token_not or QueryBuilder.token_not
        self.tokens_transform = tokens_transform or QueryBuilder.tokens_transform
        self.tokens_op = tokens_op or QueryBuilder.tokens_op
        self.tokens_aliases = (
            QueryBuilder.tokens_aliases if tokens_aliases is None else tokens_aliases
        )
        self.tokens_bool = tokens_bool or QueryBuilder.tokens_bool
        # regex matching a not
        self.re_not = re.compile(r"^(" + self.token_not + r")(\W.*)$")
        # regex matcing operators
        self.tokens_all = {
            **self.tokens_transform,
            **self.tokens_op,
            **self.tokens_aliases,
        }
        self.re_op = re.compile(
            "^("
            + "|".join(
                t.replace(" ", r"\s+") for t in sorted(self.tokens_all, reverse=True)
            )
            + r")\s*(.*)$"
        )
        self.debug = debug

    @staticmethod
    def _find_closing_bracket(text):
        """Finds the end of a bracketed expression"""
        if text[:1] != "(":
            raise QueryParseError("Internal error: missing start bracket")
        level = 0
        quoted = False
        prev_c = None
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

    def parse(self, table, text):
        """
        Builds a query from the table given and english text expression
        """
        if self.debug:
            print("PARSING", repr(text))
        # build names of possible searchable fields
        fields = {field.name.lower(): field for field in table if field.readable}
        # in the stack we put queries and logical operators only
        stack = []

        # match a token using the regex and return the token and left over text
        def next(text, regex, ignore=False):
            text = text.strip()
            if not text:
                return None, ""
            match = regex.match(text)
            if not match:
                if ignore:
                    return None, text
                raise QueryParseError(f"Unable to parse {text}")
            is_quoted = text[:1] == '"'
            token, text = match.group(1), match.group(2)
            # if text is not quoted, remove duplicated spaces
            if is_quoted:
                token = token[1:-1]
            else:
                token = self.re_spaces.sub(" ", token)
            if self.debug:
                print("MATCH", repr(token), repr(text))
            return token, text.strip()

        # loop until there is more text to process
        while text.strip():
            negate = False
            # match if we start with "not"
            token, text = next(text, self.re_not, ignore=True)
            if token:
                negate = True
            # deal with a nested expression or parse the current expression
            if text.startswith("("):
                i = QueryBuilder._find_closing_bracket(text)
                token, text = text[1:i], text[i + 1 :].strip()
                query = self.parse(table, token)
            else:
                token, text = next(text, self.re_token)
                # match a field name
                if token.lower() not in fields:
                    raise QueryParseError(
                        f"Unable to parse {token}, expected a field name"
                    )
                field = table[token]
                is_text = field.type in ("string", "text", "blob")
                has_contains = is_text or field.type.startswith("list:")
                # check an operator
                token, text = next(text, self.re_op)
                token = self.tokens_all[token]
                # if the operator is a field modifier, get the next operator
                if is_text and token == "lower":
                    token, text = next(text, self.re_op)
                    field = field.lower()
                elif is_text and token == "upper":
                    token, text = next(text, self.re_op)
                    field = field.upper()
                if token == "is null":
                    query = field == None
                elif token == "is not null":
                    query = field != None
                else:
                    # the operator requires a value, match a value
                    value, text = next(text, self.re_value)
                    validate(field, value)
                    token = self.tokens_all[token]
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
                        # the operator allows multiple values, macth next values
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
            # we mad matched a not so negate the whole expression
            if negate:
                query = ~query
            # we have a query, put it in stack or combine it into a logical expression
            if len(stack) > 1 and stack[-1] == "and":
                stack[-2:] = [stack[-2] & query]
            elif len(stack) > 1 and stack[-1] == "or":
                stack[-2:] = [stack[-2] | query]
            else:
                stack.append(query)

            # we have a query match next "and" or "or" and put them in stack
            token, text = next(text, self.re_token)
            if not token:
                break
            elif token in self.tokens_bool and text:
                stack.append(self.tokens_bool[token])
            else:
                raise QueryParseError(f"Unable to parse {token}, expected and/or")
        if len(stack) > 1:
            # this should never happen
            raise QueryParseError("Internal error: leftover stack")
        # return the one query left in stack
        return stack[-1] if stack else table.id > 0
