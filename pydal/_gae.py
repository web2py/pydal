# -*- coding: utf-8 -*-
import decimal

try:
    from new import classobj
    from google.appengine.ext import db as gae
    from google.appengine.ext import ndb
    from google.appengine.api import namespace_manager, rdbms
    from google.appengine.api.datastore_types import Key  # for belongs on ID
    from google.appengine.ext.db.polymodel import PolyModel
    from google.appengine.ext.ndb.polymodel import PolyModel as NDBPolyModel

    class GAEDecimalProperty(gae.Property):
        """
        GAE decimal implementation
        """
        data_type = decimal.Decimal

        def __init__(self, precision, scale, **kwargs):
            super(GAEDecimalProperty, self).__init__(self, **kwargs)
            d = '1.'
            for x in range(scale):
                d += '0'
            self.round = decimal.Decimal(d)

        def get_value_for_datastore(self, model_instance):
            value = super(GAEDecimalProperty, self)\
                .get_value_for_datastore(model_instance)
            if value is None or value == '':
                return None
            else:
                return str(value)

        def make_value_from_datastore(self, value):
            if value is None or value == '':
                return None
            else:
                return decimal.Decimal(value).quantize(self.round)

        def validate(self, value):
            value = super(GAEDecimalProperty, self).validate(value)
            if value is None or isinstance(value, decimal.Decimal):
                return value
            elif isinstance(value, basestring):
                return decimal.Decimal(value)
            raise gae.BadValueError("Property %s must be a Decimal or string."
                                    % self.name)

    #TODO Needs more testing
    class NDBDecimalProperty(ndb.StringProperty):
        """
        NDB decimal implementation
        """
        data_type = decimal.Decimal

        def __init__(self, precision, scale, **kwargs):
            d = '1.'
            for x in range(scale):
                d += '0'
            self.round = decimal.Decimal(d)

        def _to_base_type(self, value):
            if value is None or value == '':
                return None
            else:
                return str(value)

        def _from_base_type(self, value):
            if value is None or value == '':
                return None
            else:
                return decimal.Decimal(value).quantize(self.round)

        def _validate(self, value):
            if value is None or isinstance(value, decimal.Decimal):
                return value
            elif isinstance(value, basestring):
                return decimal.Decimal(value)
            raise TypeError("Property %s must be a Decimal or string."
                            % self._name)
except ImportError:
    gae = None
    Key = None
