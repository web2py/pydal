from ..adapters.sqlite import Spatialite, SQLite
from . import before_type, for_type, representers
from .base import JSONRepresenter, SQLRepresenter


@representers.register_for(SQLite)
class SQLiteRepresenter(SQLRepresenter, JSONRepresenter):
    pass


@representers.register_for(Spatialite)
class SpatialiteRepresenter(SQLRepresenter):
    @before_type("geometry")
    def geometry_extras(self, field_type):
        srid = 4326
        geotype, params = field_type[:-1].split("(")
        params = params.split(",")
        if len(params) >= 2:
            schema, srid = params[:2]
        return {"srid": srid}

    @for_type("geometry", adapt=False)
    def _geometry(self, value, srid):
        return "ST_GeomFromText('%s',%s)" % (value, srid)
