"""
URL-pattern parser backing ``DAL.parse_as_rest``.

A "pattern" describes a URL shape and the table/field it maps onto.
Tokens inside ``{...}`` are query predicates; tokens inside ``[...]``
are table joins. ``RestParser.parse`` walks a list of patterns,
matches the current request path against each, and returns a
status/response dict.

This module is consumed by ``pydal/restapi.py``.
"""

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from .regex import REGEX_SEARCH_PATTERN, REGEX_SQUARE_BRACKETS


def to_num(num: Any) -> int:
    """
    Coerce ``num`` to ``int`` (returns 0 if ``num`` is None).

    Raises ``ValueError`` if ``num`` isn't a parseable int.
    """
    if num is None:
        return 0
    return int(num)


class RestParser:
    """
    Walk a URL request through a list of REST patterns and dispatch
    against the bound DAL.

    Typically used via ``db.parse_as_rest(patterns, args, vars)``.
    """

    def __init__(self, db):
        self.db = db

    def auto_table(self, table: str, base: str = "", depth: int = 0) -> List[str]:
        """
        Generate REST patterns for every readable field of ``table``.

        Patterns include type-specific search predicates (``ge``/``lt``
        ranges for numerics, ``year``/``month``/``day``/``hour``/...
        for dates, ``contains`` for lists). ``depth`` walks back-
        references to other tables.
        """
        patterns: List[str] = []
        for field in self.db[table].fields:
            if base:
                tag = "%s/%s" % (base, field.replace("_", "-"))
            else:
                tag = "/%s/%s" % (table.replace("_", "-"), field.replace("_", "-"))
            f = self.db[table][field]
            if not f.readable:
                continue
            if f.type == "id" or "slug" in field or f.type.startswith("reference"):
                tag += "/{%s.%s}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
            elif f.type.startswith("boolean"):
                tag += "/{%s.%s}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
            elif f.type in ("float", "double", "integer", "bigint"):
                tag += "/{%s.%s.ge}/{%s.%s.lt}" % (table, field, table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
            elif f.type.startswith("list:"):
                tag += "/{%s.%s.contains}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
            elif f.type in ("date", "datetime"):
                tag += "/{%s.%s.year}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
                tag += "/{%s.%s.month}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
                tag += "/{%s.%s.day}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
            if f.type in ("datetime", "time"):
                tag += "/{%s.%s.hour}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
                tag += "/{%s.%s.minute}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
                tag += "/{%s.%s.second}" % (table, field)
                patterns.append(tag)
                patterns.append(tag + "/:field")
            if depth > 0:
                for f in self.db[table]._referenced_by:
                    tag += "/%s[%s.%s]" % (table, f.tablename, f.name)
                    patterns.append(tag)
                    patterns += self.auto_table(table, base=tag, depth=depth - 1)
        return patterns

    def parse(
        self,
        patterns: Union[str, Sequence[Union[str, Tuple]]],
        args: Sequence[str],
        vars: Dict[str, Any],
        queries: Optional[Any] = None,
        nested_select: bool = True,
    ):
        """
        Dispatch a request against the pattern list.

        ``patterns`` can be the string ``"auto"`` (generate patterns
        for every non-auth table) or a sequence of pattern entries.
        Each entry is either a plain pattern string, a
        ``(pattern, basequery)`` tuple, or a
        ``(pattern, basequery, exposedfields)`` tuple.

        ``args`` is the URL path split on ``/``; ``vars`` is the
        query-string dict.

        Returns a ``db.Row``-style dict with ``status`` /
        ``response`` / ``error`` / ``pattern``.

        Example::

            db.define_table('person', Field('name'), Field('info'))
            db.define_table('pet',
                Field('ownedby', db.person),
                Field('name'), Field('info'))

            @request.restful()
            def index():
                def GET(*args, **vars):
                    patterns = [
                        "/friends[person]",
                        "/{person.name}/:field",
                        "/{person.name}/pets[pet.ownedby]",
                        "/{person.name}/pets[pet.ownedby]/{pet.name}",
                        "/{person.name}/pets[pet.ownedby]/{pet.name}/:field",
                        ("/dogs[pet]", db.pet.info == 'dog'),
                        ("/dogs[pet]/{pet.name.startswith}", db.pet.info == 'dog'),
                    ]
                    parser = db.parse_as_rest(patterns, args, vars)
                    if parser.status == 200:
                        return dict(content=parser.response)
                    raise HTTP(parser.status, parser.error)
        """
        if patterns == "auto":
            patterns = []
            for table in self.db.tables:
                if not table.startswith("auth_"):
                    patterns.append("/%s[%s]" % (table, table))
                    patterns += self.auto_table(table, base="", depth=1)
        else:
            i = 0
            while i < len(patterns):
                pattern = patterns[i]
                if not isinstance(pattern, str):
                    pattern = pattern[0]
                tokens = pattern.split("/")
                if tokens[-1].startswith(":auto") and re.match(
                    REGEX_SQUARE_BRACKETS, tokens[-1]
                ):
                    new_patterns = self.auto_table(
                        tokens[-1][tokens[-1].find("[") + 1: -1],
                        "/".join(tokens[:-1]),
                    )
                    patterns = patterns[:i] + new_patterns + patterns[i + 1:]
                    i += len(new_patterns)
                else:
                    i += 1
        if "/".join(args) == "patterns":
            return self.db.Row(
                {"status": 200, "pattern": "list", "error": None, "response": patterns}
            )
        for pattern in patterns:
            basequery, exposedfields = None, []
            if isinstance(pattern, tuple):
                if len(pattern) == 2:
                    pattern, basequery = pattern
                elif len(pattern) > 2:
                    pattern, basequery, exposedfields = pattern[0:3]
            otable = table = None
            if not isinstance(queries, dict):
                dbset = self.db(queries)
                if basequery is not None:
                    dbset = dbset(basequery)
            i = 0
            tags = pattern[1:].split("/")
            if len(tags) != len(args):
                continue
            for tag in tags:
                if re.match(REGEX_SEARCH_PATTERN, tag):
                    tokens = tag[1:-1].split(".")
                    table, field = tokens[0], tokens[1]
                    if not otable or table == otable:
                        if len(tokens) == 2 or tokens[2] == "eq":
                            query = self.db[table][field] == args[i]
                        elif tokens[2] == "ne":
                            query = self.db[table][field] != args[i]
                        elif tokens[2] == "lt":
                            query = self.db[table][field] < args[i]
                        elif tokens[2] == "gt":
                            query = self.db[table][field] > args[i]
                        elif tokens[2] == "ge":
                            query = self.db[table][field] >= args[i]
                        elif tokens[2] == "le":
                            query = self.db[table][field] <= args[i]
                        elif tokens[2] == "year":
                            query = self.db[table][field].year() == args[i]
                        elif tokens[2] == "month":
                            query = self.db[table][field].month() == args[i]
                        elif tokens[2] == "day":
                            query = self.db[table][field].day() == args[i]
                        elif tokens[2] == "hour":
                            query = self.db[table][field].hour() == args[i]
                        elif tokens[2] == "minute":
                            query = self.db[table][field].minutes() == args[i]
                        elif tokens[2] == "second":
                            query = self.db[table][field].seconds() == args[i]
                        elif tokens[2] == "startswith":
                            query = self.db[table][field].startswith(args[i])
                        elif tokens[2] == "contains":
                            query = self.db[table][field].contains(args[i])
                        else:
                            raise RuntimeError("invalid pattern: %s" % pattern)
                        if len(tokens) == 4 and tokens[3] == "not":
                            query = ~query
                        elif len(tokens) >= 4:
                            raise RuntimeError("invalid pattern: %s" % pattern)
                        if not otable and isinstance(queries, dict):
                            dbset = self.db(queries[table])
                            if basequery is not None:
                                dbset = dbset(basequery)
                        dbset = dbset(query)
                    else:
                        raise RuntimeError("missing relation in pattern: %s" % pattern)
                elif (
                    re.match(REGEX_SQUARE_BRACKETS, tag)
                    and args[i] == tag[: tag.find("[")]
                ):
                    ref = tag[tag.find("[") + 1: -1]
                    if "." in ref and otable:
                        table, field = ref.split(".")
                        selfld = "_id"
                        if self.db[table][field].type.startswith("reference "):
                            refs = [
                                x.name
                                for x in self.db[otable]
                                if x.type == self.db[table][field].type
                            ]
                        else:
                            refs = [
                                x.name
                                for x in self.db[table]._referenced_by
                                if x.tablename == otable
                            ]
                        if refs:
                            selfld = refs[0]
                        if nested_select:
                            try:
                                dbset = self.db(
                                    self.db[table][field].belongs(
                                        dbset._select(self.db[otable][selfld])
                                    )
                                )
                            except ValueError:
                                return self.db.Row(
                                    {
                                        "status": 400,
                                        "pattern": pattern,
                                        "error": "invalid path",
                                        "response": None,
                                    }
                                )
                        else:
                            items = [
                                item.id
                                for item in dbset.select(self.db[otable][selfld])
                            ]
                            dbset = self.db(self.db[table][field].belongs(items))
                    else:
                        table = ref
                        if not otable and isinstance(queries, dict):
                            dbset = self.db(queries[table])
                        dbset = dbset(self.db[table])
                elif tag == ":field" and table:
                    field = args[i]
                    if field not in self.db[table]:
                        break
                    if not self.db[table][field].readable:
                        return self.db.Row(
                            {
                                "status": 418,
                                "pattern": pattern,
                                "error": "I'm a teapot",
                                "response": None,
                            }
                        )
                    try:
                        distinct = vars.get("distinct", False) == "True"
                        offset = to_num(vars.get("offset", None))
                        limits = (
                            offset,
                            to_num(vars.get("limit", None) or 1000) + offset,
                        )
                    except ValueError:
                        return self.db.Row(
                            {"status": 400, "error": "invalid limits", "response": None}
                        )
                    items = dbset.select(
                        self.db[table][field], distinct=distinct, limitby=limits
                    )
                    if items:
                        return self.db.Row(
                            {"status": 200, "response": items, "pattern": pattern}
                        )
                    return self.db.Row(
                        {
                            "status": 404,
                            "pattern": pattern,
                            "error": "no record found",
                            "response": None,
                        }
                    )
                elif tag != args[i]:
                    break
                otable = table
                i += 1
                if i == len(tags) and table:
                    if hasattr(self.db[table], "_id"):
                        ofields = vars.get("order", self.db[table]._id.name).split("|")
                    else:
                        ofields = vars.get(
                            "order", self.db[table]._primarykey[0]
                        ).split("|")
                    try:
                        orderby = [
                            (
                                self.db[table][f]
                                if not f.startswith("~")
                                else ~self.db[table][f[1:]]
                            )
                            for f in ofields
                        ]
                    except (KeyError, AttributeError):
                        return self.db.Row(
                            {
                                "status": 400,
                                "error": "invalid orderby",
                                "response": None,
                            }
                        )
                    if exposedfields:
                        fields = [
                            field
                            for field in self.db[table]
                            if str(field).split(".")[-1] in exposedfields
                            and field.readable
                        ]
                    else:
                        fields = [field for field in self.db[table] if field.readable]
                    count = dbset.count()
                    try:
                        offset = to_num(vars.get("offset", None))
                        limits = (
                            offset,
                            to_num(vars.get("limit", None) or 1000) + offset,
                        )
                    except ValueError:
                        return self.db.Row(
                            {
                                "status": 400,
                                "error": "invalid limits",
                                "response": None,
                            }
                        )
                    try:
                        response = dbset.select(
                            limitby=limits, orderby=orderby, *fields
                        )
                    except ValueError:
                        return self.db.Row(
                            {
                                "status": 400,
                                "pattern": pattern,
                                "error": "invalid path",
                                "response": None,
                            }
                        )
                    return self.db.Row(
                        {
                            "status": 200,
                            "response": response,
                            "pattern": pattern,
                            "count": count,
                        }
                    )
        return self.db.Row(
            {"status": 400, "error": "no matching pattern", "response": None}
        )
