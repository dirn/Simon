"""The base Simon models"""

__all__ = ('Model',)

from bson.objectid import ObjectId
from datetime import datetime

from .connection import get_database
from .exceptions import MultipleDocumentsFound, NoDocumentFound
from .query import Q, QuerySet
from .utils import (get_nested_key, map_fields,
                    remove_nested_key, update_nested_keys)


class Property(property):
    """Overrides the @property decorator

    This is necessary for :class:`Model` to be able to able to call the
    collection methods from its ``_meta.db`` attribute. Without this
    custom ``__get__()`` method, an AttributeError would be raised.

    .. versionadded:: 0.1.0
    """

    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


class ModelMetaClass(type):
    """Define :class:`Model`

    .. versionadded:: 0.1.0
    """

    def __new__(cls, name, bases, attrs):
        module = attrs.pop('__module__')
        new_class = super(ModelMetaClass, cls).__new__(
            cls, name, bases, {'__module__': module})

        # Associate all attributes with the new class.
        for k, v in attrs.items():
            setattr(new_class, k, v)

        # Associate the custom exceptions with the new class.
        setattr(new_class, 'MultipleDocumentsFound', MultipleDocumentsFound)
        setattr(new_class, 'NoDocumentFound', NoDocumentFound)

        # Get the Meta class. It's at least attached to Model, so it
        # will exist somewhere in the stack.
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

        # All models need the ability to map document keys to different
        # attribute names. If no map has been provided, add a
        # placeholder. If map_id has not been set to False, add a map
        # from id to _id.
        if not hasattr(meta, 'field_map'):
            meta.field_map = {}
        if getattr(meta, 'map_id', True) and 'id' not in meta.field_map:
            meta.field_map['id'] = '_id'

        # Find out whether or not to associate timestamps with documents
        # when they are saved.
        if not hasattr(meta, 'auto_timestamp'):
            meta.auto_timestamp = True

        # Find out whether or not the model should always perform writes
        # in safe mode
        if not hasattr(meta, 'safe'):
            meta.safe = False

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


class Model(object):
    """The base class for all Simon models

    .. versionadded:: 0.1.0
    """

    __metaclass__ = ModelMetaClass

    class Meta:
        """Default settings for a :class:`~simon.Model`.

        :param auto_timestamp: (optional) If ``False`` ``created`` and
                               ``modified`` fields won't be added.
        :type auto_timestamp: bool.
        :param collection: Name of the collection in the database.
        :type collection: str.
        :param database: (optional) Alias of the database.
        :type database: str.
        :param field_map: (optional) Map defining aliases for document
                          keys.
        :type field_map: dict.
        :param map_id: (optional) If ``False`` won't include
                       ``{'id': '_id'}`` in ``field_map``.
        :type map_id: bool.
        :param safe: (optional) If ``True`` all writes will use safe
                     mode.
        :type safe: bool.

        .. versionadded:: 0.1.0
        """

    def __init__(self, **fields):
        """Assigns all keyword arguments to the object's document.

        :param \*\*fields: Keyword arguments to add to the document.
        :type \*\*fields: **kwargs.
        :raises: :class:`~simon.exceptions.ConnectionError`

        .. versionadded:: 0.1.0
        """

        # Assign an empty dictionary to _document so that it can be
        # used to store the real document's values
        self._document = {}

        fields = map_fields(self.__class__, fields)

        # Add the fields to the document
        for k, v in fields.items():
            setattr(self, k, v)

    @classmethod
    def create(cls, safe=False, **fields):
        """Creates a new document and saves it to the database.

        This is a convenience method to create a new document. It will
        instantiate a new ``Model`` from the keyword arguments,
        call ``save()``, and return the instance.

        :param safe: (optional) Whether to perform the create in safe
                     mode.
        :type safe: bool.
        :param \*\*fields: Keyword argument to add to the document.
        :type \*\*fields: **kwargs.
        :returns: :class:`~simon.Model` -- the new document.

        .. versionadded:: 0.1.0
        """

        new = cls(**fields)
        new.save(safe=safe)

        return new

    def delete(self, safe=False):
        """Deletes a single document from the database.

        This will delete the document associated with the instance
        object. If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        :param safe: (optional) Whether to perform the delete in safe
                     mode.
        :type safe: bool.
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        id = getattr(self, 'id', None)
        if not id:
            raise TypeError("The '{0}' object cannot be deleted because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, 'id'))

        safe = safe or self._meta.safe
        self._meta.db.remove({'_id': id}, safe=safe)

        self._document = {}

    @classmethod
    def find(cls, *qs, **fields):
        """Gets multiple documents from the database.

        This will find a return multiple documents matching the query
        specified through ``**fields``.

        :param \*qs: :class:`~simon.query.Q` objects to use with the query.
        :type \*qs: \*args.
        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.base.QuerySet` -- query set containing
                  objects matching ``query``.

        .. versionadded:: 0.1.0
        """

        # Add all Q objects to the filter
        for q in qs:
            if isinstance(q, Q):
                fields.update(q._filter)

        query = map_fields(cls, fields, with_comparisons=True)

        # If querying by the _id, make sure it's an Object ID
        if '_id' in query and not isinstance(query['_id'], ObjectId):
            query['_id'] = ObjectId(query['_id'])

        # Find all of the matching documents.
        docs = cls._meta.db.find(query)

        return QuerySet(docs, cls)

    @classmethod
    def get(cls, *qs, **fields):
        """Gets a single document from the database.

        This will find and return a single document matching the
        query specified through ``**fields``. An exception will be
        raised if any number of documents other than one is found.

        :param \*qs: :class:`~simon.query.Q` objects to use with the query.
        :type \*qs: \*args.
        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.Model` -- object matching ``query``.
        :raises: :class:`~simon.Model.MultipleDocumentsFound`,
                 :class:`~simon.Model.NoDocumentFound`

        .. versionadded:: 0.1.0
        """

        # Add all Q objects to the filter
        for q in qs:
            if isinstance(q, Q):
                fields.update(q._filter)

        # Convert the field spec into a query by mapping any necessary
        # fields.
        query = map_fields(cls, fields, with_comparisons=True)

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

    @classmethod
    def get_or_create(cls, safe=False, **fields):
        """Gets an existing or creates a new document.

        This will find and return a single document matching the
        query specified through ``**fields``. If no document is found,
        a new one will be created.

        Along with returning the ``Model`` instance, a boolean value
        will also be returned to indicate whether or not the document
        was created.

        :param safe: (optional) Whether to perform the create in safe
                     mode.
        :type safe: bool.
        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: tuple -- the :class:`~simon.Model` and whether the
                  document was created.
        :raises: :class:`~simon.Model.MultipleDocumentsFound`

        .. versionadded:: 0.1.0
        """

        try:
            return cls.get(**fields), False
        except cls.NoDocumentFound:
            return cls.create(safe=safe, **fields), True

    def increment(self, field=None, value=1, safe=False, **fields):
        """Performs an atomic increment.

        This can be used to update a single field::

            >>> obj.increment(field, value)

        or to update multiple fields at a time::

            >>> obj.increment(field1=value1, field2=value2)

        Note that the latter does **not** set the values of the fields,
        but rather specifies the values they should be incremented by.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        If no fields are indicated--either through ``field`` or through
        ``**fields``, a :class:`ValueError` will be raised.

        :param field: (optional) Name of the field to increment.
        :type field: str.
        :param value: (optional) Value to increment ``field`` by.
        :type value: int.
        :param safe: (optional) Whether to perform the update in safe
                     mode.
        :type safe: bool.
        :param \*\*fields: Keyword arguments specifying fields and
                           increment values.
        :type \*\*fields: \*\*kwargs.
        :raises: :class:`TypeError`, :class:`ValueError`

        .. versionadded:: 0.1.0
        """

        id = getattr(self, 'id', None)
        if not id:
            raise TypeError("The '{0}' object cannot be updated because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, 'id'))

        # There needs to be something to update
        if field is None and not fields:
            raise ValueError('No fields have been specified.')

        update = {}

        # Both the field/value parameters and **fields can be used for
        # the update, so build a dictionary containing all of the fields
        # to increment and the value to increment each by
        if field is not None:
            update[field] = value
        for k, v in fields.items():
            update[k] = v

        update = map_fields(self.__class__, update, flatten_keys=True)

        safe = safe or self._meta.safe
        self._meta.db.update({'_id': id}, {'$inc': update}, safe=safe)

        # After updating the document in the database, the instance
        # needs to be updated as well. Depending on the size of the
        # document, it may be time consuming to reload the entire thing.
        # Fortunately there is a way to just load the fields that have
        # been updated. Build a dictionary containing the keys of the
        # fields that need to be reloaded and retrieve them from the
        # database. Then apply the new values to the instance
        fields = dict((k, 1) for k in update.keys())
        doc = self._meta.db.find_one({'_id': id}, fields)

        doc = update_nested_keys(self._document, doc)

        for k, v in doc.items():
            setattr(self, k, v)

    def raw_update(self, fields, safe=False, upsert=False):
        """Performs an update using a raw document.

        This method should be used carefully as it will perform the
        update exactly, potentially performing a full document
        replacement.

        Also, for simple updates, it is preferred to use the
        :meth:`~simon.Model.save` or
        :meth:`~simon.Model.update` methods as they will usually
        result in less data being transferred back from the database.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        Unlike :meth:`~simon.Model.save`, ``modified`` will not be
        updated.

        :param fields: The document to save to the database.
        :type fields: dict.
        :param safe: (optional) Whether to perform the save in safe
                     mode.
        :type safe: bool.
        :param upsert: (optional) Whether to perform the update as an
                       upsert.
        :type upsert: bool.
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        id = getattr(self, 'id', None)
        if not (id or upsert):
            raise TypeError("The '{0}' object cannot be updated because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, 'id'))

        safe = safe or self._meta.safe

        result = self._meta.db.update({'_id': id}, fields, safe=safe,
                                      upsert=upsert)

        # When upserting, the instance will need its _id. The way
        # to obtain that varies based on whether or not the upsert
        # happened in safe mode.
        #
        # When not in safe mode, the newly created document must be
        # retrieved from the database. It can be obtained through
        # a call to find_one() with the same query. A sort needs
        # to be provided to make sure the newest document comes
        # back first.
        #
        # When in safe mode, however, the database will return a
        # document which includes the upserted _id and the
        # updatedExisting field set to False.
        #
        # At the same time, the entire document needs to be reloaded
        # in order to update the existence. When performing an unsafe
        # upsert, the document needs to be retrieved in order to get the
        # _id. Rather than only getting the one field, the whole
        # document can be requested to avoid two trips to the database.
        doc = None
        if safe:
            if not result.get('updatedExisting', True):
                self.id = result.get('upserted', None)
        elif upsert:
            doc = self._meta.db.find_one(fields, sort=[('_id', -1)])
            self.id = doc['_id']

        if not doc:
            doc = self._meta.db.find_one({'_id': self.id})

        for k, v in doc.items():
            setattr(self, k, v)

    def remove_fields(self, fields, safe=False):
        """Removes the specified fields from the document.

        The specified fields will be removed from the document in the
        database as well as the object. This operation cannot be
        undone.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        Unlike :meth:`~simon.Model.save`, ``modified`` will not be
        updated.

        :param fields: The names of the fields to remove.
        :type fields: str, list, or tuple.
        :param safe: (optional) Whether to perform the save in safe
                     mode.
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

        update = dict((k, 1) for k in fields)
        update = map_fields(self.__class__, update, flatten_keys=True)

        safe = safe or self._meta.safe
        self._meta.db.update({'_id': id}, {'$unset': update}, safe=safe)

        # The fields also need to be removed from the object
        for k, v in update.items():
            if '.' in k:
                # If the key contains a ., it is pointing to an embedded
                # document, so remove the nested key from the
                # dictionary; there is no attribute to remove from the
                # instance
                self._document = remove_nested_key(self._document, k)
            else:
                try:
                    delattr(self, k)
                except AttributeError:
                    # Silently handle attributes that don't exist
                    pass

    def save(self, safe=False, upsert=False):
        """Saves the object to the database.

        When saving a new document for a model with ``auto_timestamp``
        set to ``True``, unless already provided, ``created`` will be
        added with the current datetime in UTC. ``modified`` will
        always be set with the current datetime in UTC.

        :param safe: (optional) Whether to perform the save in safe
                     mode.
        :type safe: bool.
        :param upsert: (optional) Whether to perform the save as an
                       upsert.
        :type upsert: bool.

        .. versionadded:: 0.1.0
        """

        # Associate the current datetime (in UTC) with the created
        # and modified fields. While Python can store datetimes with
        # microseconds, BSON only supports milliseconds. Rather than
        # having different data at the time of save, drop the precision
        # from the Python datetime before associating it with the
        # instance.
        if self._meta.auto_timestamp:
            now = datetime.utcnow()
            now = now.replace(microsecond=(now.microsecond / 1000 * 1000))
            if not (hasattr(self, 'id') and hasattr(self, 'created')):
                self.created = now
            self.modified = now

        # _id should never be overwritten. In order to do that, it's a
        # good idea to pop it out of the document. Popping it out of
        # the real document could lead to bad things happening. Instead,
        # capture a reference to the document and pop it out of that.
        doc = self._document.copy()
        id = doc.pop('_id', None)

        doc = map_fields(self.__class__, doc)

        safe = safe or self._meta.safe

        if upsert or id:
            result = self._meta.db.update({'_id': id}, doc, safe=safe,
                                          upsert=upsert)

            # When upserting, the instance will need its _id. The way
            # to obtain that varies based on whether or not the upsert
            # happened in safe mode.
            #
            # When not in safe mode, the newly created document must be
            # retrieved from the database. It can be obtained through
            # a call to find_one() with the same query. A sort needs
            # to be provided to make sure the newest document comes
            # back first.
            #
            # When in safe mode, however, the database will return a
            # document which includes the upserted _id and the
            # updatedExisting field set to False.
            if safe:
                if not result.get('updatedExisting', True):
                    self.id = result.get('upserted', None)
            elif upsert:
                doc = self._meta.db.find_one(doc, {'_id': 1},
                                             sort=[('_id', -1)])
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

        Unlike :meth:`~simon.Model.save`, ``modified`` will not be
        updated.

        :param fields: The names of the fields to update.
        :type fields: str, list, or tuple.
        :param safe: (optional) Whether to perform the save in safe
                     mode.
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

        update = dict((k, 1) for k in fields)
        update = map_fields(self.__class__, update, flatten_keys=True)

        # Failing to make sure all of the fields exist before saving
        # them would result in assigning blank values to keys. In some
        # cases this could result in unexpected documents being returned
        # in queries.
        try:
            update = dict((k, get_nested_key(self._document, k)) for
                          k, v in update.items())
        except KeyError:
            raise AttributeError("The '{0}' object does not have all of the "
                                 "specified fields.".format(
                                     self.__class__.__name__))

        safe = safe or self._meta.safe
        self._meta.db.update({'_id': id}, {'$set': update}, safe=safe)

    def update(self, safe=False, **fields):
        """Performs an atomic update.

        If only a select number of fields need to be updated, an atomic
        update is preferred over a document replacement.
        ``update()`` takes a series of fields and values through its
        keyword arguments. This fields will be updated both in the
        database and on the instance.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        Unlike :meth:`~simon.Model.save`, ``modified`` will not be
        updated.

        :param safe: (optional) Whether to perform the save in safe
                     mode.
        :type safe: bool.
        :param \*\*fields: The fields to update.
        :type \*\*fields: \*\*kwargs.
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        id = getattr(self, 'id', None)
        if not id:
            raise TypeError("The '{0}' object cannot be updated because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, 'id'))

        update = map_fields(self.__class__, fields, flatten_keys=True)

        safe = safe or self._meta.safe
        self._meta.db.update({'_id': id}, {'$set': update}, safe=safe)

        # After updating the document in the database, the instance
        # needs to be updated as well. Depending on the size of the
        # document, it may be time consuming to reload the entire thing.
        # Fortunately there is a way to just load the fields that have
        # been updated. Build a dictionary containing the keys of the
        # fields that need to be reloaded and retrieve them from the
        # database. Then apply the new values to the instance
        fields = dict((k, 1) for k in update.keys())
        doc = self._meta.db.find_one({'_id': id}, fields)

        doc = update_nested_keys(self._document, doc)

        for k, v in doc.items():
            setattr(self, k, v)

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

        # The first thing to look for is nested keys
        if '__' in name or '.' in name:
            if '__' in name:
                # map_fields() requires a dict, so make a simple one and
                # then capture the first (only) key in the resulting
                # dict
                mapped_name = map_fields(self.__class__, {name: 1},
                                         flatten_keys=True)
                mapped_name = mapped_name.keys()[0]
            else:
                mapped_name = name
            try:
                return get_nested_key(self._document, mapped_name)
            except AttributeError:
                # Give it a go the normal way
                pass

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
