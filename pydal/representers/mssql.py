"""MSSQL representer — SQL + JSON + GIS (geometry/geography) handlers."""

from ..adapters.mssql import MSSQL
from . import before_type, for_type, representers
from .base import JSONRepresenter, SQLRepresenter


@representers.register_for(MSSQL)
class MSSQLRepresenter(SQLRepresenter, JSONRepresenter):
    """MSSQL representer with GIS WKT → STGeomFromText conversion."""

    def _make_geoextra(self, field_type, srid):
        """Extract SRID from ``geometry(...)`` / ``geography(...)`` type strings."""
        geotype, params = field_type[:-1].split("(")
        if params:
            srid = params
        return {"srid": srid}

    @before_type("geometry")
    def geometry_extras(self, field_type):
        """Default geometry SRID is 0 (no projection)."""
        return self._make_geoextra(field_type, 0)

    @for_type("geometry", adapt=False)
    def _geometry(self, value, srid):
        """Render WKT via ``geometry::STGeomFromText(...)``."""
        return "geometry::STGeomFromText('%s',%s)" % (value, srid)

    @before_type("geography")
    def geography_extras(self, field_type):
        """Default geography SRID is 4326 (WGS 84 lat/lon)."""
        return self._make_geoextra(field_type, 4326)

    @for_type("geography", adapt=False)
    def _geography(self, value, srid):
        """Render WKT via ``geography::STGeomFromText(...)``."""
        # Argument order matches geometry: (value, srid). Previously
        # the args were swapped, producing invalid SQL.
        return "geography::STGeomFromText('%s',%s)" % (value, srid)
