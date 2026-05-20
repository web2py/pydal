"""
Centralized late-binding imports.

``OrderedDict`` and ``portalocker`` are imported from here by the rest
of pydal so the import sites stay short. ``OrderedDict`` is always
the stdlib's (it's been in Python since 3.1). ``portalocker`` is
vendored under ``pydal.contrib`` because not every deployment has the
PyPI package available.
"""

from collections import OrderedDict
from .contrib import portalocker

__all__ = ["OrderedDict", "portalocker"]
