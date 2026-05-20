"""
Process-wide globals.

* ``GLOBAL_LOCKER`` — reentrant lock used by the connection pool and a
  few schema-creation paths to serialize side-effects across threads.
* ``THREAD_LOCAL`` — per-thread scratch namespace; adapters store
  their current connection here.
* ``DEFAULT`` — sentinel-as-callable used as a default parameter value
  by ``Field`` so ``default=None`` and "no default given" can be
  distinguished.
* ``IDENTITY`` — ``lambda x: x``, used as a default credential decoder.
* ``OR`` / ``AND`` — bound versions of the bitwise operators, used by
  ``functools.reduce`` to combine a list of subqueries.
"""

import threading

GLOBAL_LOCKER: threading.RLock = threading.RLock()
THREAD_LOCAL: threading.local = threading.local()

# Sentinel marking "no value supplied" for Field defaults. Implemented
# as a callable so ``DEFAULT()`` returns ``None`` — matches the legacy
# behavior of Field's "compute default lazily" path.
DEFAULT = lambda: None


def IDENTITY(x):
    """
    Identity function: ``IDENTITY(x) == x``.

    Used as a default no-op transformer (e.g. for credential decoding).
    """
    return x


def OR(a, b):
    """Bitwise-OR ``a`` and ``b`` — query combinator for ``reduce``."""
    return a | b


def AND(a, b):
    """Bitwise-AND ``a`` and ``b`` — query combinator for ``reduce``."""
    return a & b
