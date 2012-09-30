"""The base Simon models"""

__all__ = ('MongoModel',)


class MongoModelMetaClass(type):
    """Define :class:`MongoModel`"""

    def __new__(cls, name, bases, attrs):
        module = attrs.pop('__module__')
        new_class = super(MongoModelMetaClass, cls).__new__(
            cls, name, bases, {'__module__': module})

        # Associate all attributes with the new class.
        for k, v in attrs.items():
            setattr(new_class, k, v)

        # Get the Meta class. It's at least attached to MongoModel,
        # so it will exist somewhere in the stack.
        meta = attrs.pop('Meta', None)
        if not meta:
            meta = getattr(new_class, 'Meta', None)

        # All models need a collection. The base model doesn't though.
        # Rather than pinning the check to the class name, it's
        # better to check the bases since this should never change.
        if not getattr(meta, 'collection', None) and bases != (object,):
            raise AttributeError(
                "'{0}' must have a 'collection'".format(new_class.__name__))

        # Parent classes may define their own Meta class, so subclasses
        # should inherit any of the settings they haven't overridden.
        base_meta = getattr(new_class, '_meta', None)
        if base_meta:
            for k, v in base_meta.__dict__.items():
                if not hasattr(meta, k):
                    setattr(meta, k, v)

        # All models need the ability to map document keys to the
        # Python names. If no map has been provided, use the default.
        if not hasattr(meta, 'field_map'):
            meta.field_map = {'id': '_id'}

        # The document dictionary is used to contain the actual
        # document record that has been read from/will be written to
        # the collection. It is buried away inside of Meta to keep it
        # from interfering with the real attributes of classes.
        meta.document = {}

        # In order to allow __setattr__() to properly ignore real
        # attributes of the class when passing values through to the
        # core document, a list of the class's attributes needs to be
        # stored.
        meta.core_attributes = new_class.__dict__.keys()

        # Associate Meta with the new class.
        setattr(new_class, '_meta', meta)

        return new_class


class MongoModel(object):
    """The base class for all Simon models"""

    __metaclass__ = MongoModelMetaClass

    class Meta:
        field_map = {'id': '_id'}

    def __delattr__(self, name):
        """Remove a key from the document"""

        # Normally name can be used here. In this instance, however,
        # name is needed to remove the attribute from the object.
        # key is used in case there is a different key used for the
        # document than for the object.
        key = self._meta.field_map.get(name, name)
        if key in self._meta.document:
            del self._meta.document[key]
            try:
                delattr(self, name)
            except AttributeError:
                # This will happen if the attribute hasn't been directly
                # accessed yet.
                pass

            # The deletion of the attribute is now complete, get out
            # before an AttributeError is raised by the super delete.
            return

        object.__delattr__(self, name)

    def __getattr__(self, name):
        """Retrieve a value from the document"""

        # If the attribute is a real attribute of the object, use it.
        if name in self.__dict__:
            return self.__dict__[name]

        # If the attribute is a key in the document, use it.
        name = self._meta.field_map.get(name, name)
        if not name in self._meta.document:
            raise AttributeError("'{0}' object has no attribute '{1}'.".format(
                self.__class__.__name__, name))
        return self._meta.document[name]

    def __setattr__(self, name, value):
        """Set a document value"""

        # Do not allow _meta to be overwritten
        if name == '_meta':
            raise AttributeError(
                "The '{0}' attribute cannot be overwritten.".format(name))

        # Set the attribute on the object. Trying to do this with a
        # simple assignment would result in recursion error.
        object.__setattr__(self, name, value)

        if name not in self._meta.core_attributes:
            name = self._meta.field_map.get(name, name)
            self._meta.document[name] = value
