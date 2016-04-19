from ..adapters.mssql import MSSQL
from .base import SQLRepresenter, JSONRepresenter
from . import representers, before_type, for_type


@representers.register_for(MSSQL)
class MSSQLRepresenter(SQLRepresenter, JSONRepresenter):
    def _make_geoextra(self, field_type, srid):
        geotype, params = field_type[:-1].split('(')
        if params:
            srid = params
        return {'srid': srid}

    @before_type('geometry')
    def geometry_extras(self, field_type):
        return self._make_geoextra(field_type, 0)

    @for_type('geometry', adapt=False)
    def _geometry(self, value, srid):
        return "geometry::STGeomFromText('%s',%s)" % (value, srid)

    @before_type('geography')
    def geography_extras(self, field_type):
        return self._make_geoextra(field_type, 4326)

    @for_type('geography', adapt=False)
    def _geography(self, value, srid):
        return "geography::STGeomFromText('%s',%s)" % (srid, value)
