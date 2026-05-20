# -*- coding: utf-8 -*-

"""
Default validator chains for ``Field`` types.

When a ``Field`` doesn't specify ``requires=...``, the DAL falls back
to ``DAL.validators_method`` which defaults to ``default_validators``.
The result is a validator (or list of validators) that enforces the
field's type at the form / ``validate_and_insert`` boundary.

Reference fields, list-of-reference fields, and option lists get
``IS_IN_DB`` / ``IS_IN_SET`` lookups; primitive types get the
corresponding range/format validators.
"""

from typing import Any, List, Optional, Union

from . import validators


def default_validators(db, field) -> Optional[Union[Any, List[Any]]]:
    """
    Build the default validator chain for ``field``.

    Returns:

    * ``None`` if no validator is appropriate (e.g. type ``id`` or
      ``upload`` with no explicit constraint).
    * A single validator instance when there's only one rule.
    * A list of validators when multiple rules apply.

    The chain reflects ``field.unique``, ``field.notnull``,
    ``field.length`` and the field's type. References point at the
    referenced table via ``IS_IN_DB``.
    """
    field_type = field.type
    field_unique = field.unique
    field_notnull = field.notnull

    is_ref = field_type.startswith("reference")
    is_list = field_type.startswith("list:")
    if is_ref or field_type.startswith("list:reference"):
        table_field = field_type[10 if is_ref else 15:].split(".", 1)
        table_name = table_field[0]
        field_name = table_field[-1]
        requires = None
        if table_name in db.tables:
            referenced = db[table_name]
            if len(table_field) == 1:
                requires = validators.IS_IN_DB(
                    db,
                    referenced._id,
                    label=getattr(referenced, "_format", None),
                    multiple=is_list,
                )
            elif field_name in referenced.fields:
                requires = validators.IS_IN_DB(
                    db,
                    getattr(referenced, field_name),
                    label=getattr(referenced, "_format", None),
                    multiple=is_list,
                )
        if requires:
            if field_unique:
                requires._and = validators.IS_NOT_IN_DB(db, field)
            if not field_notnull:
                requires = validators.IS_EMPTY_OR(requires)
        return requires

    if isinstance(field.options, (list, tuple)):
        requires = validators.IS_IN_SET(field.options, multiple=is_list)
    else:
        requires = []
        if field_type in ("string", "text", "password"):
            requires.append(validators.IS_LENGTH(field.length))
        elif field_type == "json":
            requires.append(validators.IS_EMPTY_OR(validators.IS_JSON()))
        elif field_type in ("double", "float"):
            requires.append(validators.IS_FLOAT_IN_RANGE(-1e100, 1e100))
        elif field_type == "integer":
            requires.append(validators.IS_INT_IN_RANGE(-(2**31), 2**31))
        elif field_type == "bigint":
            requires.append(validators.IS_INT_IN_RANGE(-(2**63), 2**63))
        elif field_type.startswith("decimal"):
            requires.append(validators.IS_DECIMAL_IN_RANGE(-(10**10), 10**10))
        elif field_type == "date":
            requires.append(validators.IS_DATE())
        elif field_type == "time":
            requires.append(validators.IS_TIME())
        elif field_type == "datetime":
            requires.append(validators.IS_DATETIME())

        if field_unique:
            requires.insert(0, validators.IS_NOT_IN_DB(db, field))
        if (field_notnull or field_unique) and field_type not in (
            "boolean",
            "password",
            "string",
            "text",
            "upload",
        ):
            requires.insert(0, validators.IS_NOT_EMPTY())
        elif not field_notnull and not field_unique and requires:
            null = "" if field.type == "password" else None
            requires[0] = validators.IS_EMPTY_OR(requires[0], null=null)

        if len(requires) == 1:
            requires = requires[0]

    return requires or None
