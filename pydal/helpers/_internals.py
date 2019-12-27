class Dispatcher(object):
    namespace = "dispatcher"

    def __init__(self, namespace=None):
        self._registry_ = {}
        if namespace:
            self.namespace = namespace

    def register_for(self, target):
        def wrap(dispatch_class):
            self._registry_[target] = dispatch_class
            return dispatch_class

        return wrap

    def get_for(self, obj):
        targets = type(obj).__mro__
        for target in targets:
            if target in self._registry_:
                return self._registry_[target](obj)
        else:
            raise ValueError("no %s found for object: %s" % (self.namespace, obj))
