# -*- coding: utf-8 -*-
"""
    pydal.utils
    -----------

    Provides some utilities for pydal.

    :copyright: (c) 2017 by Giovanni Barillari and contributors
    :license: BSD, see LICENSE for more details.
"""

import binascii
import hashlib
import warnings
from ._compat import to_bytes, to_native, basestring

class RemovedInNextVersionWarning(DeprecationWarning):
    pass


warnings.simplefilter('always', RemovedInNextVersionWarning)


def warn_of_deprecation(old_name, new_name, prefix=None, stack=2):
    msg = "%(old)s is deprecated, use %(new)s instead."
    if prefix:
        msg = "%(prefix)s." + msg
    warnings.warn(
        msg % {'old': old_name, 'new': new_name, 'prefix': prefix},
        RemovedInNextVersionWarning, stack)


class deprecated(object):
    def __init__(self, old_method_name, new_method_name, class_name=None, s=0):
        self.class_name = class_name
        self.old_method_name = old_method_name
        self.new_method_name = new_method_name
        self.additional_stack = s

    def __call__(self, f):
        def wrapped(*args, **kwargs):
            warn_of_deprecation(
                self.old_method_name, self.new_method_name, self.class_name,
                3 + self.additional_stack)
            return f(*args, **kwargs)
        return wrapped


def pbkdf2_hex(data, salt, iterations=1000, keylen=24, hashfunc=None):
    hashfunc = hashfunc or sha1
    hmac = hashlib.pbkdf2_hmac(hashfunc().name, to_bytes(data),
                               to_bytes(salt), iterations, keylen)
    return binascii.hexlify(hmac)

def simple_hash(text, key='', salt='', digest_alg='md5'):
    """Generate hash with the given text using the specified digest algorithm."""
    text = to_bytes(text)
    key = to_bytes(key)
    salt = to_bytes(salt)
    if not digest_alg:
        raise RuntimeError("simple_hash with digest_alg=None")
    elif not isinstance(digest_alg, str):  # manual approach
        h = digest_alg(text + key + salt)
    elif digest_alg.startswith('pbkdf2'):  # latest and coolest!
        iterations, keylen, alg = digest_alg[7:-1].split(',')
        digest_alg = getattr(hashlib, alg) if isinstance(alg, basestring) else alg 
        return to_native(pbkdf2_hex(text, salt, int(iterations), int(keylen), digest_alg))
    elif key:  # use hmac
        if isinstance(digest_alg, basestring):
            digest_alg = getattr(hashlib, digest_alg)
        h = hmac.new(key + salt, text, digest_alg)
    else:  # compatible with third party systems                                                                                         
        h = get_digest(digest_alg)()
        h.update(text + salt)
    return h.hexdigest()

DIGEST_ALG_BY_SIZE = {
    128 // 4: 'md5',
    160 // 4: 'sha1',
    224 // 4: 'sha224',
    256 // 4: 'sha256',
    384 // 4: 'sha384',
    512 // 4: 'sha512',
}
