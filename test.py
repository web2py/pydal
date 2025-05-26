from pydal import Field, DAL
import re

class QueryParseError(Exception): pass

token_not = "not"
token_transformars = {
    "upper" : "upper",
    "lower": "lower",
}
token_ops = {
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
token_aliases = {
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
token_bool = {
    "and": "and",
    "or": "or"
}
# regex to match a leading not
re_not = re.compile(r"^(" + token_not + ")(\W.*)$")
# regex used to parse and, or, and fieldnames
re_token = re.compile(r"^(\w+)\s*(.*)$")
# regex matching modifiers and op tokens
token_all = {**token_transformars, **token_ops, **token_aliases}
re_op = re.compile("^("+"|".join(t.replace(" ",r"\s+") for t in sorted(token_all, reverse=True))+r")\s*(.*)$")
# rgex matching values and quoted values
re_value = re.compile(r'^((?:")[^"]*(?:[\]["][^"]*)*(?:")|[^ ,]*)\s*(.*)$')
# regex used to compress repeated spaces
re_spaces = re.compile(r"\s+")

def find_closing_bracket(text):
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


def build(table, text):
    """
    Builds a query from the table given and english text expression
    """
    print("PARSING", repr(text))
    # build names of possible searchable fields
    fields = {field.name.lower():field for field in table if field.readable}
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
        if not is_quoted:
            token = re_spaces.sub(" ", token)
        print("MATCH", repr(token), repr(text))
        return token, text

    # loop until there is more text to process
    while text.strip():
        negate = False
        # match if we start with "not"
        token, text = next(text, re_not, ignore=True)
        if token:
            negate = True
        # deal with a nested expression or parse the current expression
        if text.startswith("("):
            i = find_closing_bracket(text)
            token, text = text[1:i], text[i+1:].strip()
            query = build(table, token)
        else:
            token, text = next(text, re_token)
            # match a field name
            if token.lower() not in fields:
                raise QueryParseError(f"Unable to parse {token}, expected a field name")
            field = table[token]
            is_text = field.type in ('string', 'text', 'blob')
            has_contains = is_text or field.type.startswith("list:")
            # check an operator
            token, text = next(text, re_op)
            token = token_all[token]
            # if the operator is a field modifier, get the next operator
            if is_text and token == "lower":
                token, text = next(text, re_op)
                field = field.lower()
            elif is_text and token == "upper":
                token, text = next(text, re_op)
                field = field.upper()
            if token == 'is null':
                query = field == None
            elif token == 'is not null':
                query = field != None
            else:
                # the operator requires a value, match a value
                value, text = next(text, re_value)
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
                elif token == "belongs":
                    # the operator allows multiple values, macth next values
                    value = [value]
                    while text.startswith(","):
                        token, text = next(text[1:], re_value)
                        value.append(token)
                    query = field.belongs(value)
                else:
                    raise QueryParseError(f"Unable to parse {token}, expected an operator")
        # we mad matched a not so negate the whole expression
        if negate:
            query = ~query
        # we have a query, put it in stack or combine it into a logical expression
        if len(stack)>1 and stack[-1] == "and":
            stack[-2:] = [stack[-2] & query]
        elif len(stack)>1 and stack[-1] == "or":
            stack[-2:] = [stack[-2] | query]
        else:
            stack.append(query)

        # we have a query match next "and" or "or" and put them in stack
        token, text = next(text, re_token)
        if not token:
            break
        elif token in token_bool and text:
            stack.append(token_bool[token])
        else:
            raise QueryParseError(f"Unable to parse {token}, expected and/or")
    if len(stack) > 1:
        # this should never happen
        raise QueryParseError("Internal error: leftover stack")
    # return the one query left in stack
    return stack[-1] if stack else table.id > 0

db = DAL("sqlite://storage.sql", folder="/tmp")
db.define_table(
    "thing",
    Field("name"),
    Field("weight", "double"),
    Field("quantity", "integer"),
    Field("tags","list:string"))

print(build(db.thing, 'not weight is greater than 10.5 and quantity is not null or  name  lower startswith "do\\"g" or name belongs "one", "two", "three"'))

print(build(db.thing, 'not( not weight is greater than 10.5 and(quantity is not null or((name lower startswith "dog") or name belongs "one", "two", "three")))'))
