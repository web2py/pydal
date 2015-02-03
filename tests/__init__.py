from _adapt import NOSQL

if NOSQL:
    from nosql import *
else:
    from sql import *

from validation import TestValidateAndInsert
from caching import TestCache
