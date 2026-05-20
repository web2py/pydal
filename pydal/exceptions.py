# -*- coding: utf-8 -*-

"""
pydal exception hierarchy.

* ``NotFoundException`` — raised when a referenced record doesn't exist
  (e.g. an upload's file is missing on disk).
* ``NotAuthorizedException`` — raised when a Field's ``authorize``
  callback rejects an upload retrieval.
* ``NotOnNOSQLError`` — raised when a SQL-only feature (nested
  subqueries, certain joins, ...) is invoked against a NoSQL backend.
  Inherits from ``NotImplementedError`` so callers that already catch
  it keep working.
"""

from typing import Optional


class NotFoundException(Exception):
    """A referenced record or resource could not be found."""


class NotAuthorizedException(Exception):
    """Access to a record or resource was denied by an authorize hook."""


class NotOnNOSQLError(NotImplementedError):
    """A SQL-only feature was invoked against a NoSQL adapter."""

    def __init__(self, message: Optional[str] = None):
        if message is None:
            message = "Not supported on NoSQL databases"
        super().__init__(message)
