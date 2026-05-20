"""
Internal helpers — not part of the public pydal API.
"""

from typing import Any, Callable, Dict, Type


class Dispatcher:
    """
    Class-registry helper used to wire dialects, parsers, representers,
    and compilers to their adapters.

    A ``Dispatcher`` is created once per axis (one for dialects, one
    for parsers, ...). Each registered class is associated with an
    adapter class. ``get_for(obj)`` walks the MRO of ``type(obj)``
    and returns the first registered match, constructed with ``obj``
    as its single argument.

    Example::

        dialects = Dispatcher("dialect")

        @dialects.register_for(SQLAdapter)
        class SQLDialect(...):
            def __init__(self, adapter): ...

        # later:
        d = dialects.get_for(some_adapter)
    """

    namespace: str = "dispatcher"

    def __init__(self, namespace: str = None):
        self._registry_: Dict[Type, Type] = {}
        if namespace:
            self.namespace = namespace

    def register_for(self, target: Type) -> Callable[[Type], Type]:
        """
        Return a decorator that registers a class for the given
        ``target`` adapter class.

        Usage::

            @dispatcher.register_for(SQLAdapter)
            class MyThing(...): ...
        """
        def wrap(dispatch_class: Type) -> Type:
            self._registry_[target] = dispatch_class
            return dispatch_class

        return wrap

    def get_for(self, obj: Any) -> Any:
        """
        Resolve the registered class for ``obj`` and instantiate it
        with ``obj``.

        Walks ``type(obj).__mro__`` so subclasses inherit their
        ancestor's registration. Raises ``ValueError`` if no match
        is found in the chain.
        """
        targets = type(obj).__mro__
        for target in targets:
            if target in self._registry_:
                return self._registry_[target](obj)
        raise ValueError("no %s found for object: %s" % (self.namespace, obj))
