from ._adapt import IS_NOSQL

# Backend-specific suites: the SQL tests and the NoSQL tests cover the same
# surface; only one runs per `DB=` env var.
if IS_NOSQL:
    from .nosql import *
else:
    from .sql import *
    from .indexes import *

# Backend-agnostic suites.
from .ast_advanced import *
from .ast_compile import *
from .ast_joins import *
from .ast_params import *
from .ast_statements import *
from .ast_subselect import *
from .ast_translate import *
from .cross_dialect import *
from .driver_io import *
from .tier2_units import *
from .tier4_units import *
from .tier5_units import *
from .base import *
from .caching import TestCache
from .contribs import *
from .is_url_validators import *
from .querybuilder import *
from .restapi import *
from .scheduler import *
from .smart_query import *
from .tags import *
from .validation import *
from .validators import *
