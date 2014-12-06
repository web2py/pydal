# -*- coding: utf-8 -*-
# load modules with contrib fallback

try:
    from collections import OrderedDict
except:
    from .contrib.ordereddict import OrderedDict
try:
    import json
except:
    from .contrib import simplejson as json
try:
    import portalocker
except ImportError:
    from .contrib import portalocker
