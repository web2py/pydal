from ._adapt import IS_NOSQL

# Backend-specific suites: the SQL tests and the NoSQL tests cover the same
# surface; only one runs per `DB=` env var.
if IS_NOSQL:
    from .nosql import *
else:
    from .sql import *
    from .indexes import *

# Backend-agnostic suites.
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
