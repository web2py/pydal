import sys
import hashlib
import os

PY2 = sys.version_info[0] == 2

_identity = lambda x: x

if PY2:
    import cPickle as pickle
    from cStringIO import StringIO
    import copy_reg as copyreg
    reduce = reduce
    hashlib_md5 = hashlib.md5
    iterkeys = lambda d: d.iterkeys()
    itervalues = lambda d: d.itervalues()
    iteritems = lambda d: d.iteritems()
    integer_types = (int, long)
    string_types = (str, unicode)
    basestring = basestring
    xrange = xrange

    def to_unicode(obj):
        if isinstance(obj, str):
            return obj.decode('utf8')
        if not isinstance(obj, unicode):
            return unicode(obj)
        return obj

    def implements_iterator(cls):
        cls.next = cls.__next__
        del cls.__next__
        return cls

    def implements_bool(cls):
        cls.__nonzero__ = cls.__bool__
        del cls.__bool__
        return cls
else:
    import pickle
    from io import StringIO
    import copyreg
    from functools import reduce
    hashlib_md5 = lambda s: hashlib.md5(bytes(s, 'utf8'))
    iterkeys = lambda d: iter(d.keys())
    itervalues = lambda d: iter(d.values())
    iteritems = lambda d: iter(d.items())
    integer_types = (int,)
    string_types = (str,)
    basestring = str
    xrange = range

    def to_unicode(obj):
        if isinstance(obj, bytes):
            return obj.decode('utf8')
        if not isinstance(obj, str):
            return str(obj)
        return obj

    implements_iterator = _identity
    implements_bool = _identity


def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""
    # This requires a bit of explanation: the basic idea is to make a dummy
    # metaclass for one level of class instantiation that replaces itself with
    # the actual metaclass.
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)
    return metaclass('temporary_class', None, {})


# shortcuts
pjoin = os.path.join
exists = os.path.exists
