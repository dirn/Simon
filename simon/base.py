"""The base Simon models"""

__all__ = ('MongoModel',)

from bson.objectid import ObjectId
from datetime import datetime

from .connection import get_database
from .exceptions import MultipleDocumentsFound, NoDocumentFound


class Property(property):
    """Overrides the @property decorator

    This is necessary for :class:`MongoModel` to be able to able to call
    the collection methods from its ``_meta.db`` attribute. Without
    this custom ``__get__()`` method, an AttributeError would be raised.
    """

    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


class MongoModelMetaClass(type):
    """Define :class:`MongoModel`"""

    def __new__(cls, name, bases, attrs):
        module = attrs.pop('__module__')
        new_class = super(MongoModelMetaClass, cls).__new__(
            cls, name, bases, {'__module__': module})

        # Associate all attributes with the new class.
        for k, v in attrs.items():
            setattr(new_class, k, v)

        # Associate the custom exceptions with the new class.
        setattr(new_class, 'MultipleDocumentsFound', MultipleDocumentsFound)
        setattr(new_class, 'NoDocumentFound', NoDocumentFound)

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

        # Associate the database collection with the new class. A
        # lambda is used so that the collection reference isn't grabbed
        # until it's actually needed. property by itself is not
        # sufficient here. The custom Property subclass is needed in
        # conjunction with classmethod in order to expose db as a
        # property of the Meta class.
        meta.db = Property(classmethod(
            lambda cls: get_database(cls.database)[cls.collection]))

        # In order to allow __setattr__() to properly ignore real
        # attributes of the class when passing values through to the
        # core document, a list of the class's attributes needs to be
        # stored. At the time of instantiation, _document will be added
        # to the class. By adding it to the list now, it doesn't need
        # to be done each time a new list is instantiated.
        meta.core_attributes = new_class.__dict__.keys() + ['_document']

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
        :raises: :class:`~simon.exceptions.ConnectionError`

        .. versionadded:: 0.1.0
        """

        # Assign an empty dictionary to _document so that it can be
        # used to store the real document's values
        self._document = {}

        # Add the fields to the document
        for k, v in fields.items():
            k = self._meta.field_map.get(k, k)
            setattr(self, k, v)

    def delete(self, safe=False):
        """Aliases :meth:`~simon.MongoModel.remove`.

        .. versionadded:: 0.1.0
        """

        self.remove(safe=safe)

    @classmethod
    def get(cls, **fields):
        """Gets a single document from the database.

        This will find and return a single document matching the
        query specified through ``**fields``. An exception will be
        raised if any number of documents other than one is found.

        :param fields: Keyword arguments specifying the query.
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
                '`get()` returned more than one "{0}". It returned {1}!'
                ' The document spec was: {2}'.format(
                    cls.__name__, count, fields))

        # Return an instantiated object for the retrieved document
        return cls(**docs[0])

    def remove(self, safe=False):
        """Removes a single document from the database.

        This will remove the document associated with the instance
        object. If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        :param safe: Whether to perform the removal in safe mode.
        :type safe: bool.
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        id = getattr(self, 'id', None)
        if not id:
            raise TypeError("The '{0}' object cannot be deleted because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, 'id'))

        self._meta.db.remove({'_id': id}, safe=safe)
        self._document = {}

    def remove_fields(self, fields, safe=False):
        """Removes the specified fields from the document.

        The specified fields will be removed from the document in the
        database as well as the object. This operation cannot be
        undone.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        Unlike :meth:`~simon.MongoModel.save`, ``modified`` will not be
        updated.

        :param fields: The names of the fields to remove.
        :type fields: str, list, or tuple.
        :param safe: Whether to perform the save in safe mode.
        :type safe: bool.
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        id = getattr(self, 'id', None)
        if not id:
            raise TypeError("The '{0}' object cannot be updated because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, 'id'))

        # fields can contain a single item as a string. If it's not a
        # list or tuple, make it one. Otherwise the generators below
        # would iterate over each character in the string rather than
        # treating it as a single item list.
        if not isinstance(fields, (list, tuple)):
            fields = (fields,)

        doc = dict((k, 1) for k in fields)
        self._meta.db.update({'_id': id}, {'$unset': doc}, safe=safe)

        # The fields also need to be removed from the object
        for field in fields:
            try:
                delattr(self, field)
            except AttributeError:
                # Silently handle attributes that don't exist
                pass

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
        doc = self._document.copy()
        id = doc.pop('_id', None)

        if upsert or id:
            result = self._meta.db.update({'_id': id}, doc, safe=safe,
                                          upsert=upsert)

            # When upserting, the instance will need its _id. The way
            # to obtain that varies based on whether or not the upsert
            # happened in safe mode.
            #
            # When not in safe mode, the newly created document must be
            # retrieved from the database. It can be obtained through
            # a call to find_one() with the same query.
            #
            # When in safe mode, however, the database will return a
            # document which includes the upserted _id and the
            # updatedExisting field set to False.
            if safe:
                if not result.get('updatedExisting', True):
                    self.id = result.get('upserted', None)
            elif upsert:
                doc = self._meta.db.find_one(doc, {'_id': 1})
                self.id = doc['_id']
        else:
            self.id = self._meta.db.insert(doc, safe=safe)

    def save_fields(self, fields, safe=False):
        """Saves only the specified fields.

        If only a select number of fields need to be updated, an atomic
        update is preferred over a document replacement.
        ``save_fields()`` takes either a single field name or a list of
        field names to update.

        All of the specified fields must exist or an
        :class:`AttributeError` will be raised. To add a field to the
        document with a blank value, make sure to assign it through
        ``object.attribute = ''`` or something similar before calling
        ``save_fields()``.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        Unlike :meth:`~simon.MongoModel.save`, ``modified`` will not be
        updated.

        :param fields: The names of the fields to update.
        :type fields: str, list, or tuple.
        :param safe: Whether to perform the save in safe mode.
        :type safe: bool.
        :raises: :class:`AttributeError`, :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        # Make sure the document has already been saved
        id = getattr(self, 'id', None)
        if not id:
            raise TypeError("The '{0}' object cannot be updated because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, 'id'))

        # fields can contain a single item as a string. If it's not a
        # list or tuple, make it one. Otherwise the generators below
        # would iterate over each character in the string rather than
        # treating it as a single item list.
        if not isinstance(fields, (list, tuple)):
            fields = (fields,)

        # Failing to make sure all of the fields exist before saving
        # them would result in assigning blank values to keys. In some
        # cases this could result in unexpected documents being returned
        # in queries.
        if any(k not in self._document for k in fields):
            raise AttributeError("The '{0}' object does not have all of the "
                                 "specified fields.".format(
                                     self.__class__.__name__))

        doc = dict((k, getattr(self, k)) for k in fields)
        self._meta.db.update({'_id': id}, {'$set': doc}, safe=safe)

    def __delattr__(self, name):
        """Remove a key from the document"""

        # Normally name can be used here. In this instance, however,
        # name is needed to remove the attribute from the object.
        # key is used in case there is a different key used for the
        # document than for the object.
        key = self._meta.field_map.get(name, name)
        if key in self._document:
            del self._document[key]
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
        if not name in self._document:
            raise AttributeError("'{0}' object has no attribute '{1}'.".format(
                self.__class__.__name__, name))
        return self._document[name]

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
            self._document[name] = value
