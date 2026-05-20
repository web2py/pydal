"""
pydal helper subpackage.

Contains modules used across the codebase that don't belong to a
specific layer:

* ``classes`` — bespoke container classes (``BasicStorage``, ``OpRow``,
  ``Reference``, ``Serializable``, ``SQLCallableList``,
  ``SQLCustomType``, etc.).
* ``methods`` — free functions (``geoPoint``/``geoLine``/``geoPolygon``,
  ``xorify``, ``merge_tablemaps``, ``bar_encode``/``bar_decode_*``,
  cleanup/upload helpers, ...).
* ``regex`` — shared compiled-and-uncompiled regular expressions.
* ``rest`` — request-parsing helpers for ``restapi.py``.
* ``serializers`` — JSON/XML/CSV serializer registry.
* ``_internals`` — class-registry ``Dispatcher`` used by the
  dialect/parser/representer/compiler axes.
"""
