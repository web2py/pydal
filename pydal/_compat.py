import hashlib
import os
import sys

PY2 = sys.version_info[0] == 2

_identity = lambda x: x

if PY2:
    from email import Charset, Encoders
    from email.Charset import QP as charset_QP
    from email.Charset import add_charset
    from email.Header import Header
    from email.MIMEBase import MIMEBase
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText
    from string import maketrans
    from types import ClassType
    from urllib import FancyURLopener
    from urllib import quote as urllib_quote
    from urllib import quote_plus as urllib_quote_plus
    from urllib import unquote
    from urllib import unquote as urllib_unquote
    from urllib import urlencode

    import __builtin__ as builtin
    import ConfigParser as configparser
    import Cookie
    import cookielib
    import copy_reg as copyreg
    import cPickle as pickle
    import Queue
    import thread
    import urllib2
    import urlparse
    from cStringIO import StringIO
    from htmlentitydefs import entitydefs, name2codepoint
    from HTMLParser import HTMLParser
    from urllib2 import urlopen
    from xmlrpclib import ProtocolError

    from .contrib import ipaddress

    BytesIO = StringIO
    reduce = reduce
    reload = reload
    hashlib_md5 = hashlib.md5
    iterkeys = lambda d: d.iterkeys()
    itervalues = lambda d: d.itervalues()
    iteritems = lambda d: d.iteritems()
    integer_types = (int, long)
    string_types = (str, unicode)
    text_type = unicode
    basestring = basestring
    xrange = xrange
    long = long
    unichr = unichr
    unicodeT = unicode

    def implements_bool(cls):
        cls.__nonzero__ = cls.__bool__
        del cls.__bool__
        return cls

    def implements_iterator(cls):
        cls.next = cls.__next__
        del cls.__next__
        return cls

    def to_bytes(obj, charset="utf-8", errors="strict"):
        if obj is None:
            return None
        if isinstance(obj, (bytes, bytearray, buffer)):
            return bytes(obj)
        if isinstance(obj, unicode):
            return obj.encode(charset, errors)
        raise TypeError("Expected bytes")

    def to_native(obj, charset="utf8", errors="strict"):
        if obj is None or isinstance(obj, str):
            return obj
        return obj.encode(charset, errors)

else:
    import _thread as thread
    import builtins as builtin
    import configparser
    import copyreg
    import html  # warning, this is the python3 module and not the web2py html module
    import ipaddress
    import pickle
    import queue as Queue
    from email import encoders as Encoders
    from email.charset import QP as charset_QP
    from email.charset import Charset, add_charset
    from email.header import Header
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from functools import reduce
    from html.entities import entitydefs, name2codepoint
    from html.parser import HTMLParser
    from http import cookiejar as cookielib
    from http import cookies as Cookie
    from importlib import reload
    from io import BytesIO, StringIO
    from urllib import parse as urlparse
    from urllib import request as urllib2
    from urllib.parse import quote as urllib_quote
    from urllib.parse import quote_plus as urllib_quote_plus
    from urllib.parse import unquote
    from urllib.parse import unquote as urllib_unquote
    from urllib.parse import urlencode
    from urllib.request import FancyURLopener, urlopen
    from xmlrpc.client import ProtocolError

    hashlib_md5 = lambda s: hashlib.md5(bytes(s, "utf8"))
    iterkeys = lambda d: iter(d.keys())
    itervalues = lambda d: iter(d.values())
    iteritems = lambda d: iter(d.items())
    integer_types = (int,)
    string_types = (str,)
    text_type = str
    basestring = str
    xrange = range
    long = int
    unichr = chr
    unicodeT = str
    maketrans = str.maketrans
    ClassType = type

    implements_iterator = _identity
    implements_bool = _identity

    def to_bytes(obj, charset="utf-8", errors="strict"):
        if obj is None:
            return None
        if isinstance(obj, (bytes, bytearray, memoryview)):
            return bytes(obj)
        if isinstance(obj, str):
            return obj.encode(charset, errors)
        raise TypeError("Expected bytes")

    def to_native(obj, charset="utf8", errors="strict"):
        if obj is None or isinstance(obj, str):
            return obj
        return obj.decode(charset, errors)


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

    return metaclass("temporary_class", None, {})


def to_unicode(obj, charset="utf-8", errors="strict"):
    if obj is None:
        return None
    if not hasattr(obj, "decode") or not callable(obj.decode):
        return text_type(obj)
    return obj.decode(charset, errors)


# shortcuts
pjoin = os.path.join
exists = os.path.exists
