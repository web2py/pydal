# -*- coding: utf-8 -*-

import re

REGEX_TYPE = re.compile(r"^(?:list:)?\w+")
REGEX_DBNAME = re.compile(r"^(\w+)(:\w+)*")
REGEX_W = re.compile(r"^\w+$")
REGEX_TABLE_DOT_FIELD = re.compile(r"^(\w+)\.(\w+)$")
REGEX_TABLE_DOT_FIELD_OPTIONAL_QUOTES = r'^"?(\w+)"?\."?(\w+)"?$'
REGEX_UPLOAD_PATTERN = (
    r"(?P<table>\w+)\.(?P<field>\w+)\.(?P<uuidkey>[\w-]+)(\.(?P<name>\w+))?\.\w+$"
)
REGEX_UPLOAD_CLEANUP = "['\"\\s;]+"
REGEX_UNPACK = r"(?<!\|)\|(?!\|)"
REGEX_PYTHON_KEYWORDS = re.compile(
    "^(False|True|and|as|assert|break|class|"
    "continue|def|del|elif|else|except|exec|finally|for|from|global|if|import|"
    "in|is|lambda|nonlocal|not|or|pass|print|raise|return|try|while|with|yield)$"
)
REGEX_SELECT_AS_PARSER = r"\s+AS\s+(\S+)$"
REGEX_CONST_STRING = '("[^"]*")|' "('[^']*')"
REGEX_SEARCH_PATTERN = r"^{[^.]+\.[^.]+(\.(lt|gt|le|ge|eq|ne|contains|startswith|year|month|day|hour|minute|second))?(\.not)?}$"
REGEX_SQUARE_BRACKETS = r"^.+\[.+\]$"
REGEX_UPLOAD_EXTENSION = r"\.(\w{1,5})$"
REGEX_ALPHANUMERIC = r"^[0-9a-zA-Z]\w*$"
REGEX_CREDENTIALS = r"(?<=//)[\w.-]+([:/][^@]*)?(?=@)"
REGEX_VALID_TB_FLD = re.compile(r"^[a-zA-Z]\w*\Z")
