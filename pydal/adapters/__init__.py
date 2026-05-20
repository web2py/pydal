"""
Adapter registration framework — URI prefix → adapter class lookup.

``@adapters.register_for("postgres", "postgres:psycopg2")`` is the
decorator pattern; ``adapters.get_for("postgres")`` is the lookup
called by ``DAL.__init__``. Each prefix corresponds to a URI scheme
(or ``scheme:driver`` pair).

The ``AdapterMeta`` metaclass also intercepts ``entity_quoting`` and
``uploads_in_blob`` constructor kwargs so they can be applied as
class-level overrides without touching every adapter's ``__init__``.
"""

import re

from ..helpers._internals import Dispatcher


class Adapters(Dispatcher):
    """
    URI-keyed registry of adapter classes.

    Unlike the generic ``Dispatcher``, registration is by URI string
    (``"postgres"``, ``"postgres:psycopg2"``, ``"sqlite:memory"``,
    ...), and ``get_for`` takes the URI prefix rather than walking
    the class MRO.
    """

    def register_for(self, *uris):
        """Decorator: register a class for one or more URI prefixes."""

        def wrap(dispatch_class):
            for uri in uris:
                self._registry_[uri] = dispatch_class
            return dispatch_class

        return wrap

    def get_for(self, uri):
        """Look up an adapter class by URI prefix; raises ``SyntaxError`` if missing."""
        try:
            return self._registry_[uri]
        except KeyError:
            raise SyntaxError("Adapter not found for %s" % uri)


adapters: "Adapters" = Adapters("adapters")


class AdapterMeta(type):
    """
    Metaclass that intercepts adapter-level kwargs at construction.

    Currently picks up ``entity_quoting`` (disables identifier quoting
    when False) and ``uploads_in_blob`` (forces all uploads into the
    DB as BLOBs). Both are removed from kwargs before they reach the
    adapter's ``__init__``.
    """

    def __call__(cls, *args, **kwargs):
        uploads_in_blob = kwargs.get("adapter_args", {}).get(
            "uploads_in_blob", cls.uploads_in_blob
        )
        cls.uploads_in_blob = uploads_in_blob

        entity_quoting = kwargs.get("entity_quoting", True)
        if "entity_quoting" in kwargs:
            del kwargs["entity_quoting"]

        obj = super(AdapterMeta, cls).__call__(*args, **kwargs)

        regex_ent = r"(\w+)"
        if not entity_quoting:
            obj.dialect.quote_template = "%s"
        else:
            regex_ent = obj.dialect.quote_template % regex_ent
        # FIXME: this regex should NOT be compiled
        obj.REGEX_TABLE_DOT_FIELD = re.compile(r"^%s\.%s$" % (regex_ent, regex_ent))

        return obj


def with_connection(f):
    """
    Decorator: only run ``f`` when the adapter has a live connection.

    Returns ``None`` when there's no connection — used for cleanup
    paths that should silently no-op on a closed DAL.
    """

    def wrap(*args, **kwargs):
        if args[0].connection:
            return f(*args, **kwargs)
        return None

    return wrap


def with_connection_or_raise(f):
    """
    Decorator: raise instead of silently no-op'ing when no connection.

    The error is ``ValueError(args[1])`` when an extra positional arg
    is present (so callers can surface the failed statement), otherwise
    a generic ``RuntimeError``.
    """

    def wrap(*args, **kwargs):
        if not args[0].connection:
            if len(args) > 1:
                raise ValueError(args[1])
            raise RuntimeError("no connection available")
        return f(*args, **kwargs)

    return wrap


from .base import NoSQLAdapter, SQLAdapter
from .couchdb import CouchDB
from .db2 import DB2
from .firebird import FireBird
from .google import GoogleSQL
from .informix import Informix
from .ingres import Ingres
from .mongo import Mongo
from .mssql import MSSQL
from .mysql import MySQL
from .oracle import Oracle
from .postgres import Postgre, PostgrePsyco
from .sap import SAPDB
from .snowflake import Snowflake
from .sqlite import SQLite
from .teradata import Teradata
