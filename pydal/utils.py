# -*- coding: utf-8 -*-

"""
pydal misc utilities — deprecation helpers, UTC clock, URI parsing,
and string/byte coercion.

* ``utcnow`` — naive (tz-stripped) current UTC datetime.
* ``RemovedInNextVersionWarning`` / ``warn_of_deprecation`` /
  ``@deprecated`` — flag old method names that still exist for
  back-compat but should not appear in new code.
* ``split_uri_args`` — parse the ``?key=val&...`` portion of a DAL
  connection string into a dict.
* ``to_bytes`` / ``to_native`` / ``to_unicode`` — defensive coercion
  helpers tolerant of None and already-decoded values.
* ``hashlib_md5`` — convenience wrapper around ``hashlib.md5`` that
  accepts a ``str`` directly.
"""

import datetime
import hashlib
import re
import warnings
from typing import Any, Callable, Dict, Optional


class RemovedInNextVersionWarning(DeprecationWarning):
    """Deprecation warning subclass used by ``@deprecated``."""


warnings.simplefilter("always", RemovedInNextVersionWarning)


def utcnow() -> datetime.datetime:
    """
    Return the current UTC time as a naive ``datetime`` (tz-stripped).

    Naive UTC is what pydal's ``datetime`` columns store on most
    backends, so this helper keeps timestamps comparable to fetched
    rows without any tz-info bookkeeping.
    """
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def warn_of_deprecation(
    old_name: str,
    new_name: str,
    prefix: Optional[str] = None,
    stack: int = 2,
) -> None:
    """
    Emit a ``RemovedInNextVersionWarning`` describing the rename.

    ``stack`` is the ``stacklevel`` forwarded to ``warnings.warn`` so
    the warning points at the caller's frame, not this helper.
    """
    msg = "%(old)s is deprecated, use %(new)s instead."
    if prefix:
        msg = "%(prefix)s." + msg
    warnings.warn(
        msg % {"old": old_name, "new": new_name, "prefix": prefix},
        RemovedInNextVersionWarning,
        stack,
    )


class deprecated:
    """
    Decorator: mark a method as deprecated.

    Usage::

        @deprecated("old_name", "new_name", "ClassName")
        def old_name(self, ...):
            ...

    When the wrapped function is called, a
    ``RemovedInNextVersionWarning`` is emitted naming the replacement.
    """

    def __init__(
        self,
        old_method_name: str,
        new_method_name: str,
        class_name: Optional[str] = None,
        s: int = 0,
    ):
        self.class_name = class_name
        self.old_method_name = old_method_name
        self.new_method_name = new_method_name
        self.additional_stack = s

    def __call__(self, f: Callable) -> Callable:
        def wrapped(*args, **kwargs):
            warn_of_deprecation(
                self.old_method_name,
                self.new_method_name,
                self.class_name,
                3 + self.additional_stack,
            )
            return f(*args, **kwargs)

        return wrapped


def split_uri_args(
    query: str,
    separators: str = "&?",
    need_equal: bool = False,
) -> Dict[str, str]:
    """
    Parse the query portion of a DAL connection string into a dict.

    ``query`` is the substring after ``?`` in a DAL URI (or anywhere
    keys are joined by ``&`` / ``?``).

    ``separators`` is the set of characters treated as key/value
    delimiters.

    ``need_equal`` — when True, only ``key=value`` pairs are matched;
    when False (default), bare ``key`` keys are also accepted (with
    value ``None``).
    """
    if need_equal:
        regex_arg_val = "(?P<argkey>[^=]+)=(?P<argvalue>[^%s]*)[%s]?" % (
            separators,
            separators,
        )
    else:
        regex_arg_val = "(?P<argkey>[^=%s]+)(=(?P<argvalue>[^%s]*))?[%s]?" % (
            separators,
            separators,
            separators,
        )
    return dict(
        [m.group("argkey", "argvalue") for m in re.finditer(regex_arg_val, query)]
    )


# ---------------------------------------------------------------------------
# String / bytes coercion. Used liberally across pydal — keep
# tolerant of None and already-decoded values.
# ---------------------------------------------------------------------------


def to_bytes(obj: Any, charset: str = "utf-8", errors: str = "strict") -> Optional[bytes]:
    """
    Coerce ``obj`` to ``bytes``.

    * ``None`` passes through.
    * Buffer-like objects (``bytes`` / ``bytearray`` / ``memoryview``)
      are converted to ``bytes``.
    * ``str`` is encoded with ``charset``.
    * Anything else raises ``TypeError``.
    """
    if obj is None:
        return None
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return bytes(obj)
    if isinstance(obj, str):
        return obj.encode(charset, errors)
    raise TypeError("Expected bytes")


def to_native(obj: Any, charset: str = "utf8", errors: str = "strict") -> Optional[str]:
    """
    Coerce a bytes-or-str ``obj`` to the platform-native string type.

    On Python 3 that's ``str``: bytes are decoded with ``charset``,
    strings pass through, and ``None`` passes through.
    """
    if obj is None or isinstance(obj, str):
        return obj
    return obj.decode(charset, errors)


def to_unicode(obj: Any, charset: str = "utf-8", errors: str = "strict") -> Optional[str]:
    """
    Decode ``obj`` to ``str``.

    * ``None`` passes through.
    * Bytes-like objects with ``.decode`` are decoded.
    * Anything else is wrapped via ``str(obj)``.
    """
    if obj is None:
        return None
    if not hasattr(obj, "decode") or not callable(obj.decode):
        return str(obj)
    return obj.decode(charset, errors)


def hashlib_md5(s: str):
    """``hashlib.md5(s.encode('utf8'))`` — convenience wrapper."""
    return hashlib.md5(s.encode("utf8"))
