"""PostgreSQL representers — SQL + JSON + JSONB + GIS (PostGIS) + arrays."""

from ..adapters.postgres import Postgre, PostgreNew
from ..helpers.serializers import serializers
from . import before_type, for_type, representers
from .base import JSONRepresenter, SQLRepresenter


@representers.register_for(Postgre)
class PostgreRepresenter(SQLRepresenter, JSONRepresenter):
    """PostgreSQL representer with PostGIS geometry/geography + JSONB."""

    def _make_geoextra(self, field_type):
        """
        Extract the SRID from a PostGIS type string like
        ``geometry(POINT,4326)``. Defaults to 4326 (WGS 84).
        """
        srid = 4326
        geotype, params = field_type[:-1].split("(")
        params = params.split(",")
        if len(params) >= 2:
            schema, srid = params[:2]
        return {"srid": srid}

    @before_type("geometry")
    def geometry_extras(self, field_type):
        """Extract SRID for ``geometry`` columns."""
        return self._make_geoextra(field_type)

    @for_type("geometry", adapt=False)
    def _geometry(self, value, srid):
        """Render either WKB hex (when value starts with ``0``) or WKT via ST_GeomFromText."""
        # If the value starts with a ``0`` we treat it as a WKB hex
        # blob and pass it through; otherwise we treat it as WKT and
        # route through ``ST_GeomFromText``.
        if value[0] == "0":
            return "E'%s'" % value
        return "ST_GeomFromText('%s',%s)" % (value, srid)

    @before_type("geography")
    def geography_extras(self, field_type):
        """Extract SRID for ``geography`` columns."""
        return self._make_geoextra(field_type)

    @for_type("geography", adapt=False)
    def _geography(self, value, srid):
        """Render WKT via ``ST_GeogFromText('SRID=...;<wkt>')``."""
        return "ST_GeogFromText('SRID=%s;%s')" % (srid, value)

    @for_type("jsonb", encode=True)
    def _jsonb(self, value):
        """Serialize Python values to a JSONB string literal."""
        return serializers.json(value)


@representers.register_for(PostgreNew)
class PostgreArraysRepresenter(PostgreRepresenter):
    """
    PostgreSQL representer for adapters with array support.

    Adds the ``{a,b,c}`` array-literal serialization used by
    Postgres' ``ARRAY`` columns.
    """

    def _listify_elements(self, elements):
        """Render a Python sequence as a Postgres array literal ``{a,b,c}``."""
        return "{" + ",".join(str(el) for el in elements) + "}"
