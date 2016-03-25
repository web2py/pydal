import re
from ..helpers._internals import Dispatcher
from ..helpers.regex import REGEX_NO_GREEDY_ENTITY_NAME


class Adapters(Dispatcher):
    def register_for(self, *drivers):
        def wrap(dispatch_class):
            for driver in drivers:
                self._registry_[driver] = dispatch_class
            return dispatch_class
        return wrap

    def get_for(self, driver_name):
        try:
            return self._registry_[driver_name]
        except KeyError:
            raise SyntaxError(
                'Adapter not found for specified driver %s' % driver_name
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

        ## TODO: avoid entity quoting disable
        entity_quoting = kwargs.get('entity_quoting', False)
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
