# -*- coding: utf-8 -*-
"""
    pydal.utils
    -----------

    Provides some utilities for pydal.

    :copyright: (c) 2016 by Giovanni Barillari and contributors
    :license: BSD, see LICENSE for more details.
"""

import warnings


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
