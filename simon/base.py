"""The base Simon models"""

__all__ = ('MongoModel',)

from bson.objectid import ObjectId
from datetime import datetime

from .connection import get_database
from .decorators import requires_database
from .exceptions import MultipleDocumentsFound, NoDocumentFound


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

        # All models also need a database. This is easier to handle than
        # the collection, though. Only the name is needed here. If no
        # name has been specified, the default is what should be used.
        if not getattr(meta, 'database', None):
            meta.database = 'default'

        # Associate the custom exceptions with the new class.
        setattr(new_class, 'MultipleDocumentsFound', MultipleDocumentsFound)
        setattr(new_class, 'NoDocumentFound', NoDocumentFound)

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

    def __init__(self, **fields):
        """Assigns all keyword arguments to the object's document.

        :param **fields: Keyword arguments to add to the document.
        :type **fields: **kwargs.
        :raises: ConnectionError

        .. versionadded:: 0.1.0
        """

        # Add the fields to the document
        for k, v in fields.items():
            k = self._meta.field_map.get(k, k)
            setattr(self, k, v)

    @classmethod
    @requires_database
    def get(cls, **fields):
        """Gets a single document from the database.

        This will find and return a single document matching the
        query specified through ``**fields``. An exception will be
        raised if any number of documents other than one is found.

        :param fields: Keywords arguments specifying the query.
        :type fields: kwargs.
        :returns: :class:`~simon.MongoModel` -- object matching ``query``.
        :raises: :class:`~simon.MongoModel.MultipleDocumentsFound`,
                 :class:`~simon.MongoModel.NoDocumentFound`

        .. versionadded:: 0.1.0
        """

        # Convert the field spec into a query by mapping any necessary
        # fields.
        query = {}
        for k, v in fields.items():
            k = cls._meta.field_map.get(k, k)
            query[k] = v

        # If querying by the _id, make sure it's an Object ID
        if '_id' in query and not isinstance(query['_id'], ObjectId):
            query['_id'] = ObjectId(query['_id'])

        # Find all of the matching documents. find_one() could be used
        # here instead, but that would return the *first* matching
        # document, not the *only* matching document. In order to know
        # if a number of documents other than one was found, find()
        # and count() must be used instead. Because MongoDB uses
        # cursors, not data needs to be transferred until the result
        # set is sliced later on.
        docs = cls._meta.db.find(query)
        count = docs.count()
        if not count:
            raise cls.NoDocumentFound(
                "'{0}' matching query does not exist.".format(cls.__name__))
        elif count > 1:
            raise cls.MultipleDocumentsFound(
                '`get()` returned more than one "{0}". It returned {1}! The '
                'document spec was: {2}'.format(
                    cls.__name__, count, fields))

        # Return an instantiated object for the retrieved document
        return cls(**docs[0])

    @requires_database
    def save(self, safe=False, upsert=False):
        """Saves the object to the database.

        When saving a new document, unless already provided, ``created``
        will be added with the current datetime in UTC. ``modified``
        will always be set with the current datetime in UTC.

        :param safe: Whether to perform the save in safe mode.
        :type safe: bool.
        :param upsert: Whether to perform the save as an upsert.
        :type upsert: bool.

        .. versionadded:: 0.1.0
        """

        # Associate the current datetime (in UTC) with the created
        # and modified fields.
        now = datetime.utcnow()
        if not (hasattr(self, 'id') and hasattr(self, 'created')):
            self.created = now
        self.modified = now

        # _id should never be overwritten. In order to do that, it's a
        # good idea to pop it out of the document. Popping it out of
        # the real document could lead to bad things happening. Instead,
        # capture a reference to the document and pop it out of that.
        doc = self._meta.document
        id = doc.pop('_id', None)
        if upsert or id:
            self._meta.db.update({'_id': id}, doc, safe=safe,
                                 upsert=upsert)
        else:
            self.id = self._meta.db.insert(doc, safe=safe)

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
