# -*- coding: utf-8 -*-

"""
Shared regular-expression patterns used across pydal.

Patterns prefixed with ``REGEX_`` are either pre-compiled
``re.Pattern`` instances or bare strings, depending on how they were
historically used:

* ``REGEX_TYPE`` — extracts the base type out of a field type string
  (``"list:string"`` -> ``"list:string"``, ``"reference person"`` ->
  ``"reference"``).
* ``REGEX_DBNAME`` — extracts the dialect prefix from a DAL URI
  (``"sqlite:memory"`` -> ``"sqlite"``).
* ``REGEX_W`` — pure ``\\w+`` identifier match.
* ``REGEX_TABLE_DOT_FIELD`` — matches ``"table.field"``; groups
  ``(table, field)``.
* ``REGEX_TABLE_DOT_FIELD_OPTIONAL_QUOTES`` — same, but accepting
  ``"table"."field"`` quoting around either side.
* ``REGEX_UPLOAD_PATTERN`` — destructures the file name of an
  ``upload`` field into ``(table, field, uuidkey, name)``.
* ``REGEX_UPLOAD_CLEANUP`` — characters stripped from uploaded
  filenames.
* ``REGEX_UNPACK`` — splits a ``list:*`` field's pipe-encoded value
  into items (single ``|`` is a delimiter; ``||`` is an escaped pipe).
* ``REGEX_PYTHON_KEYWORDS`` — used by ``check_reserved`` to refuse
  Python keywords as field names.
* ``REGEX_SELECT_AS_PARSER`` — pulls the alias out of a colname like
  ``"x AS y"``.
* ``REGEX_CONST_STRING`` — matches single- or double-quoted literals
  inside a smart_query input.
* ``REGEX_SEARCH_PATTERN`` — REST API search-token shape
  ``{table.field.op}`` / ``{table.field.op.not}``.
* ``REGEX_SQUARE_BRACKETS`` — quick check for a ``foo[bar]`` token.
* ``REGEX_UPLOAD_EXTENSION`` — captures the trailing ``.ext`` of a
  filename.
* ``REGEX_ALPHANUMERIC`` — identifier-friendly start (letter/digit
  followed by word chars).
* ``REGEX_CREDENTIALS`` — masks the ``user:pass`` part of a DAL URI
  for safe logging.
* ``REGEX_VALID_TB_FLD`` — accepts a valid table or field name (must
  start with a letter; ``\\w+`` after).
"""

import re

REGEX_TYPE = re.compile(r"^(?:list:)?\w+")
REGEX_DBNAME = re.compile(r"^(\w+)(:\w+)*")
REGEX_W = re.compile(r"^\w+$")
REGEX_TABLE_DOT_FIELD = re.compile(r"^(\w+)\.(\w+)$")
REGEX_TABLE_DOT_FIELD_OPTIONAL_QUOTES = r'^"?(\w+)"?\."?(\w+)"?$'
REGEX_UPLOAD_PATTERN = (
    r"(?P<table>\w+)\.(?P<field>\w+)\.(?P<uuidkey>[\w-]+)(\.(?P<name>\S+))?\.\w+$"
)
REGEX_UPLOAD_CLEANUP = "['\"\\s;]+"
REGEX_UNPACK = r"(?<!\|)\|(?!\|)"
REGEX_PYTHON_KEYWORDS = re.compile(
    "^(False|True|and|as|assert|break|class|"
    "continue|def|del|elif|else|except|exec|finally|for|from|global|if|import|"
    "in|is|lambda|nonlocal|not|or|pass|print|raise|return|try|while|with|yield)$"
)
REGEX_SELECT_AS_PARSER = r"\s+AS\s+(\S+)$"
REGEX_CONST_STRING = "(\"[^\"]*\")|('[^']*')"
REGEX_SEARCH_PATTERN = r"^{[^.]+\.[^.]+(\.(lt|gt|le|ge|eq|ne|contains|startswith|year|month|day|hour|minute|second))?(\.not)?}$"
REGEX_SQUARE_BRACKETS = r"^.+\[.+\]$"
REGEX_UPLOAD_EXTENSION = r"\.(\w{1,5})$"
REGEX_ALPHANUMERIC = r"^[0-9a-zA-Z]\w*$"
REGEX_CREDENTIALS = r"(?<=//)[\w.-]+([:/][^@]*)?(?=@)"
REGEX_VALID_TB_FLD = re.compile(r"^[a-zA-Z]\w*\Z")
