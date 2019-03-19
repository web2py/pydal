from ..adapters.postgres import Postgre, PostgreNew
from .base import SQLRepresenter, JSONRepresenter
from . import representers, before_type, for_type
from ..helpers.serializers import serializers


@representers.register_for(Postgre)
class PostgreRepresenter(SQLRepresenter, JSONRepresenter):
    def _make_geoextra(self, field_type):
        srid = 4326
        geotype, params = field_type[:-1].split('(')
        params = params.split(',')
        if len(params) >= 2:
            schema, srid = params[:2]
        return {'srid': srid}

    @before_type('geometry')
    def geometry_extras(self, field_type):
        return self._make_geoextra(field_type)

    @for_type('geometry', adapt=False)
    def _geometry(self, value, srid):
        # If the value starts with a zero, treat as a WKB string
        # and pass back flagged as hex data as is. Otherwise treat
        # the value as WKT and pass it through the converter.
        if value[0] == '0':
            return "E'%s'" % value
        else:
            return "ST_GeomFromText('%s',%s)" % (value, srid)

    @before_type('geography')
    def geography_extras(self, field_type):
        return self._make_geoextra(field_type)

    @for_type('geography', adapt=False)
    def _geography(self, value, srid):
        return "ST_GeogFromText('SRID=%s;%s')" % (srid, value)

    @for_type('jsonb', encode=True)
    def _jsonb(self, value):
        return serializers.json(value)


@representers.register_for(PostgreNew)
class PostgreArraysRepresenter(PostgreRepresenter):
    def _listify_elements(self, elements):
        return "{" + ",".join(str(el) for el in elements) + "}"
