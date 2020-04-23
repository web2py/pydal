# -*- coding: utf-8 -*-

try:
    from new import classobj
except ImportError:
    classobj = type

try:
    from google.appengine.ext import db as gae
except ImportError:

   try:
      # classobj = type
       from google.cloud import ndb
       from google.cloud.ndb.polymodel import PolyModel as NDBPolyModel
       gae = ndb
       Key = ndb.Key

       class DummyNameSpaceManager:
           def set_namespace(self, _):
               raise NotImplementedError()

       namespace_manager = DummyNameSpaceManager()

       class DummyRdbms:

           def connect(self, *a, **b):
               pass

       rdbms = DummyRdbms()
       print ("GAE link established")

   except ImportError:

       gae = None
       Key = None


else:
    from google.appengine.ext import ndb
    from google.appengine.api import namespace_manager, rdbms
    from google.appengine.api.datastore_types import Key  # for belongs on ID
    from google.appengine.ext.ndb.polymodel import PolyModel as NDBPolyModel
