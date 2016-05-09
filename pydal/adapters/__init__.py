import re
from .._gae import gae
from ..helpers._internals import Dispatcher
from ..helpers.regex import REGEX_NO_GREEDY_ENTITY_NAME


class Adapters(Dispatcher):
    def register_for(self, *uris):
        def wrap(dispatch_class):
            for uri in uris:
                self._registry_[uri] = dispatch_class
            return dispatch_class
        return wrap

    def get_for(self, uri):
        try:
            return self._registry_[uri]
        except KeyError:
            raise SyntaxError(
                'Adapter not found for %s' % uri
            )

adapters = Adapters('adapters')


class AdapterMeta(type):
    """Metaclass to support manipulation of adapter classes.

    At the moment is used to intercept `entity_quoting` argument passed to DAL.
    """
    def __call__(cls, *args, **kwargs):
        uploads_in_blob = kwargs.get('adapter_args', {}).get(
            'uploads_in_blob', cls.uploads_in_blob)
        cls.uploads_in_blob = uploads_in_blob

        entity_quoting = kwargs.get('entity_quoting', True)
        if 'entity_quoting' in kwargs:
            del kwargs['entity_quoting']

        obj = super(AdapterMeta, cls).__call__(*args, **kwargs)
        if not entity_quoting:
            quot = obj.dialect.quote_template = '%s'
            regex_ent = r'(\w+)'
        else:
            quot = obj.dialect.quote_template
            regex_ent = REGEX_NO_GREEDY_ENTITY_NAME
        obj.REGEX_TABLE_DOT_FIELD = re.compile(
            r'^' + quot % regex_ent + r'\.' + quot % regex_ent + r'$')

        return obj


def with_connection(f):
    def wrap(*args, **kwargs):
        if args[0].connection:
            return f(*args, **kwargs)
        return None
    return wrap


def with_connection_or_raise(f):
    def wrap(*args, **kwargs):
        if not args[0].connection:
            if len(args) > 1:
                raise ValueError(args[1])
            raise RuntimeError('no connection available')
        return f(*args, **kwargs)
    return wrap


from .base import SQLAdapter, NoSQLAdapter
from .sqlite import SQLite
from .postgres import Postgre, PostgrePsyco, PostgrePG8000
from .mysql import MySQL
from .mssql import MSSQL
from .mongo import Mongo
from .db2 import DB2
from .firebird import FireBird
from .informix import Informix
from .ingres import Ingres
from .oracle import Oracle
from .sap import SAPDB
from .teradata import Teradata
from .couchdb import CouchDB

if gae is not None:
    from .google import GoogleSQL
