import os

#NOSQL = any([name in (os.getenv("DB") or "")
#            for name in ("datastore", "mongodb", "imap")])

#if NOSQL:
#    from test_dal_nosql import *
#else:
from sql import *
