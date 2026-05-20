"""SQLite + SpatiaLite representers."""

from ..adapters.sqlite import Spatialite, SQLite
from . import before_type, for_type, representers
from .base import JSONRepresenter, SQLRepresenter


@representers.register_for(SQLite)
class SQLiteRepresenter(SQLRepresenter, JSONRepresenter):
    """Plain SQL + JSON; SQLite has no per-type overrides."""


@representers.register_for(Spatialite)
class SpatialiteRepresenter(SQLRepresenter):
    """SpatiaLite representer with PostGIS-style ``ST_GeomFromText``."""

    @before_type("geometry")
    def geometry_extras(self, field_type):
        """Extract the SRID from a ``geometry(POINT,4326)``-style type string."""
        srid = 4326
        geotype, params = field_type[:-1].split("(")
        params = params.split(",")
        if len(params) >= 2:
            schema, srid = params[:2]
        return {"srid": srid}

    @for_type("geometry", adapt=False)
    def _geometry(self, value, srid):
        """Render WKT geometry via ``ST_GeomFromText('<wkt>', srid)``."""
        return "ST_GeomFromText('%s',%s)" % (value, srid)
