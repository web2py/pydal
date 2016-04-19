from ..adapters.sqlite import SQLite, Spatialite
from .base import SQLRepresenter, JSONRepresenter
from . import representers, before_type, for_type


@representers.register_for(SQLite)
class SQLiteRepresenter(SQLRepresenter, JSONRepresenter):
    pass


@representers.register_for(Spatialite)
class SpatialiteRepresenter(SQLRepresenter):
    @before_type('geometry')
    def geometry_extras(self, field_type):
        srid = 4326
        geotype, params = field_type[:-1].split('(')
        params = params.split(',')
        if len(params) >= 2:
            schema, srid = params[:2]
        return {'srid': srid}

    @for_type('geometry', adapt=False)
    def _geometry(self, value, srid):
        return "ST_GeomFromText('%s',%s)" % (value, srid)
