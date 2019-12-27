# -*- coding: utf-8 -*-
# load modules with contrib fallback

try:
    from collections import OrderedDict
except:
    from .contrib.ordereddict import OrderedDict

from .contrib import portalocker

# TODO: uncomment the lines below when contrib/portalocker will be
# inline with the one shipped with pip
# try:
#    import portalocker
# except ImportError:
#    from .contrib import portalocker
