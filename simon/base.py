"""The base Simon models"""

import warnings

from .connection import get_database, pymongo_supports_mongoclient
from .exceptions import MultipleDocumentsFound, NoDocumentFound
from .query import Q, QuerySet
from .utils import (current_datetime, get_nested_key, guarantee_object_id,
                    is_atomic, map_fields, remove_nested_key,
                    update_nested_keys)

__all__ = ('Model',)


# In version 2.4, PyMongo introduced MongoClient as a
# replcaement for Connection. Part of the new class is that the
# safe argument was deprecated in favor of the w argument.
# Whereas safe was a boolean, w is an integer representing the
# number of servers in a replica set that must receive the
# update before the write is considered successful.

def _set_write_concern_as_safe(options, force_safe):
    safe = options.pop('safe', None)
    w = options.pop('w', None)

    if force_safe:
        safe = True
    else:
        if safe is None and w:
            safe = True
        else:
            safe = safe or False
    options['safe'] = safe


def _set_write_concern_as_w(options, force_safe):
    safe = options.pop('safe', None)
    w = options.pop('w', None)

    if force_safe:
        w = int(force_safe)  # use int() in case force_safe is True
    else:
        if w is None and safe:
            w = 1
        else:
            w = w or 0
    options['w'] = w


if pymongo_supports_mongoclient:
    _set_write_concern = _set_write_concern_as_w
else:
    _set_write_concern = _set_write_concern_as_safe


class Meta(object):
    """Custom options for a :class:`~simon.Model`.

    .. versionadded:: 0.1.0
    """

    _db = None

    def __init__(self, meta):
        self.meta = meta

        # Set all the default option values.
        self.auto_timestamp = True
        self.database = 'default'
        self.field_map = {}
        self.map_id = True
        self.required_fields = None
        self.safe = True
        self.sort = None

    def add_to_original(self, cls, name):
        """Adds the ``Meta`` object to another class.

        .. versionadded:: 0.1.0
        """

        cls._meta = self

        if self.meta:
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                # Remove any "private" attributes.
                if name.startswith('_'):
                    del meta_attrs[name]

            # Add the known attributes to the instance
            for name in ('auto_timestamp', 'collection', 'database',
                         'field_map', 'map_id', 'required_fields', 'safe',
                         'sort'):
                if name in meta_attrs:
                    setattr(self, name, meta_attrs.pop(name))

            if not hasattr(self, 'collection'):
                # If there's no collection name, generate one from the
                # other class's name.
                self.collection = ''.join([cls.__name__.lower(), 's'])

        else:
            # If there's no real instance to add, we just have to
            # generate the collection name.
            self.collection = ''.join([cls.__name__.lower(), 's'])
        del self.meta

        # Store the name of the class to make __repr__() and __str__()
        # more useful.
        self.class_name = cls.__name__

        # Get the list of the attributes associated with the class.
        # These will make up the list of reserved words that cannot be
        # used for keys.
        self.core_attributes = cls.__dict__.keys() + ['_document']

        if self.map_id and 'id' not in self.field_map:
            # If map_id is True and id isn't in field_map, add it.
            self.field_map['id'] = '_id'

        # Any of the methods that check for required fields are looking
        # for something like a list or tuple of fields, not a string
        # of one field. If a single field name has been provided as a
        # string, switch it to a tuple now.
        if self.required_fields and not isinstance(self.required_fields,
                                                   (list, tuple)):
            self.required_fields = (self.required_fields,)

        # When calling QuerySet.sort(), it accepts the sort fields as
        # *args. Forcing Meta.sort into an iterable now allows it to
        # be specified as either a single field or a list when defining
        # the model and saves the trouble of having to worry about it
        # later.
        if self.sort and not isinstance(self.sort, (list, tuple)):
            self.sort = (self.sort,)

    @property
    def db(self):
        """Contains the :class:`~pymongo.collection.Collection`.

        .. versionadded:: 0.1.0
        """

        if self._db is None:
            # Only make the call to get_database once for each class.
            self._db = get_database(self.database)[self.collection]
        return self._db

    def __repr__(self):
        return '<Meta options for {0}>'.format(self.class_name)

    def __str__(self):
        return '{0}.Meta'.format(self.class_name)

    def __unicode__(self):
        return u'{0}.Meta'.format(self.class_name)


class ModelMetaClass(type):
    """Define :class:`Model`

    .. versionadded:: 0.1.0
    """

    def __new__(cls, name, bases, attrs):
        new_new = super(ModelMetaClass, cls).__new__
        if bases == (object,):
            # If this class isn't a subclass of anything besides object
            # there's no need to do anything.
            return new_new(cls, name, bases, attrs)

        module = attrs.pop('__module__')
        new_class = new_new(cls, name, bases, {'__module__': module})

        # Associate all attributes with the new class.
        for k, v in attrs.items():
            new_class.addattr(k, v)

        # Associate the custom exceptions with the new class.
        new_class.addattr('MultipleDocumentsFound', MultipleDocumentsFound)
        new_class.addattr('NoDocumentFound', NoDocumentFound)

        # Get the Meta class if it exists. If not, try to get it from
        # the new class.
        meta = attrs.pop('Meta', None)
        if not meta:
            meta = getattr(new_class, 'Meta', None)

        # Associate _meta with the new class.
        new_class.addattr('_meta', Meta(meta))

        return new_class

    def addattr(self, name, value):
        """Assigns attributes to the class.

        .. versionadded:: 0.1.0
        """

        if hasattr(value, 'add_to_original'):
            value.add_to_original(self, name)
        else:
            setattr(self, name, value)


class Model(object):
    """The base class for all Simon models

    .. versionadded:: 0.1.0
    """

    __metaclass__ = ModelMetaClass

    def __init__(self, **fields):
        """Assigns all keyword arguments to the object's document.

        :param \*\*fields: Keyword arguments to add to the document.
        :type \*\*fields: \*\*kwargs.
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
    def all(self):
        """Returns all documents in the collection.

        If ``sort`` has been defined on the ``Meta`` class it will be
        used to order the records.

        .. versionadded:: 0.1.0
        """

        # All of the functionality already exists inside find(), so
        # just call that with no parameters.
        return self.find()

    @classmethod
    def create(cls, safe=False, **fields):
        """Creates a new document and saves it to the database.

        This is a convenience method to create a new document. It will
        instantiate a new ``Model`` from the keyword arguments,
        call ``save()``, and return the instance.

        If the model has the ``required_fields`` options set, a
        :class:`TypeError` will be raised if any of the fields are not
        provided.

        :param safe: (optional) Whether to perform the create in safe
                     mode.
        :type safe: bool.
        :param \*\*fields: Keyword arguments to add to the document.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.Model` -- the new document.
        :raises: :class:`TypeError`

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

        id = self._document.get('_id')
        if not id:
            raise TypeError("The '{0}' object cannot be deleted because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, '_id'))

        kwargs = {'safe': safe}
        _set_write_concern(kwargs, self._meta.safe)

        self._meta.db.remove({'_id': id}, **kwargs)

        self._document = {}

    @classmethod
    def find(cls, q=None, *qs, **fields):
        """Gets multiple documents from the database.

        This will find a return multiple documents matching the query
        specified through ``**fields``. If ``sort`` has been defined on
        the ``Meta`` class it will be used to order the records.

        :param q: (optional) A logical query to use with the query.
        :type q: :class:`~simon.query.Q`.
        :param \*qs: **DEPRECATED** Use ``q`` instead.
        :type \*qs: \*args.
        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.base.QuerySet` -- query set containing
                  objects matching ``query``.

        .. versionchanged:: 0.3.0
           Deprecating ``qs`` in favor of ``q``

        .. versionadded:: 0.1.0
        """

        if qs:
            warnings.warn('qs has been deprecated. Please use q instead.',
                          DeprecationWarning)
            for filter in qs:
                q._filter.update(filter._filter)

        return cls._find(q=q, **fields)

    @classmethod
    def get(cls, q=None, *qs, **fields):
        """Gets a single document from the database.

        This will find and return a single document matching the
        query specified through ``**fields``. An exception will be
        raised if any number of documents other than one is found.

        :param q: (optional) A logical query to use with the query.
        :type q: :class:`~simon.query.Q`.
        :param \*qs: **DEPRECATED** Use ``q`` instead.
        :type \*qs: \*args.
        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.Model` -- object matching ``query``.
        :raises: :class:`~simon.Model.MultipleDocumentsFound`,
                 :class:`~simon.Model.NoDocumentFound`

        .. versionchanged:: 0.3.0
           Deprecating ``qs`` in favor of ``q``

        .. versionadded:: 0.1.0
        """

        if qs:
            warnings.warn('qs has been deprecated. Please use q instead.',
                          DeprecationWarning)
            for filter in qs:
                q._filter.update(filter._filter)

        return cls._find(find_one=True, q=q, **fields)

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

        self._update({'$inc': update}, safe=safe)

    def raw_update(self, fields, safe=False):
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
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        self._update(fields, safe=safe)

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

        If the model has the ``required_fields`` options set, a
        :class:`TypeError` will be raised if attempting to remove one of
        the required fields.

        :param fields: The names of the fields to remove.
        :type fields: str, list, or tuple.
        :param safe: (optional) Whether to perform the save in safe
                     mode.
        :type safe: bool.
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        # fields can contain a single item as a string. If it's not a
        # list or tuple, make it one. Otherwise the generators below
        # would iterate over each character in the string rather than
        # treating it as a single item list.
        if not isinstance(fields, (list, tuple)):
            fields = (fields,)

        fields = dict((k, 1) for k in fields)

        self._update({'$unset': fields}, safe=safe)

    def save(self, safe=False):
        """Saves the object to the database.

        When saving a new document for a model with ``auto_timestamp``
        set to ``True``, unless already provided, ``created`` will be
        added with the current datetime in UTC. ``modified`` will
        always be set with the current datetime in UTC.

        If the model has the ``required_fields`` options set, a
        :class:`TypeError` will be raised if any of the fields have not
        been associated with the instance.

        :param safe: (optional) Whether to perform the save in safe
                     mode.
        :type safe: bool.
        :raises: :class:`TypeError`

        .. versionadded:: 0.1.0
        """

        # Associate the current datetime (in UTC) with the created
        # and modified fields. While Python can store datetimes with
        # microseconds, BSON only supports milliseconds. Rather than
        # having different data at the time of save, drop the precision
        # from the Python datetime before associating it with the
        # instance.
        if self._meta.auto_timestamp:
            now = current_datetime()
            if not ('_id' in self._document or 'created' in self):
                self._document['created'] = now
            self._document['modified'] = now

        # Use a copy of the internal document so _id can be safely
        # removed.
        fields = self._document.copy()
        fields.pop('_id', None)

        # Use upsert=True so new documents can be inserted.
        self._update(fields, upsert=True, safe=safe)

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

        # fields can contain a single item as a string. If it's not a
        # list or tuple, make it one. Otherwise the generators below
        # would iterate over each character in the string rather than
        # treating it as a single item list.
        if not isinstance(fields, (list, tuple)):
            fields = (fields,)

        update = dict((k, 1) for k in fields)

        # Set use_internal so that the real values will come from
        # self._document instead of the document passed to _update().
        self._update({'$set': update}, use_internal=True, safe=safe)

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

        self._update({'$set': fields}, safe=safe)

    # Database interaction methods

    @classmethod
    def _find(cls, q=None, find_one=False, **fields):
        """Find documents in the database.

        This method will find documents in the database matching the
        specified query. When ``find_one`` is set to ``False``, a
        :class:`~simon.query.QuerySet` will be returned containing all
        of the matching documents. When it is set to ``True``, the
        single matching document will be returned as an instance of the
        model.

        When querying with ``find_one`` set to ``True``,
        ``NoDocumentFound`` will be raised when no documents match the
        query, ``MultipleDocumentsFound`` will be raised when more than
        one matches.

        :param q: (optional) A logical query to use with the query.
        :type q: :class:`~simon.query.Q`.
        :param find_one: Whether or not the query should only return one
                         document.
        :type find_one: bool.
        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.base.Model` or
                  :class:`~simon.base.QuerySet` -- the document or query
                  set containing objects matching ``query``.
        :raises: :class:`~simon.Model.MultipleDocumentsFound`,
                 :class:`~simon.Model.NoDocumentFound`

        .. versionadded:: 0.3.0
        """

        # If there is a Q object, add it to the spec document.
        if isinstance(q, Q):
            fields.update(q._filter)

        query = map_fields(cls, fields, flatten_keys=True, with_operators=True)

        # If querying by the _id, make sure it's an Object ID.
        if '_id' in query:
            query['_id'] = guarantee_object_id(query['_id'])

        # Find all of the matching documents.
        docs = cls._meta.db.find(query)

        if find_one:
            count = docs.count()

            exception = None
            if not count:
                exception = cls.NoDocumentFound
                message = "'{0}' matching query does not exist."
                message = message.format(cls.__name__)
            elif count > 1:
                exception = cls.MultipleDocumentsFound
                message = ("The query returned more than one '{0}'. It "
                           "returned {1}! The document spec was: {2}.")
                message = message.format(cls.__name__, count, fields)

            if exception:
                raise exception(message)

            result = cls(**docs[0])
        else:
            result = QuerySet(docs, cls)

            if cls._meta.sort:
                # Apply the default sort for the model.
                result = result.sort(*cls._meta.sort)

        return result

    def _update(self, fields, upsert=False, use_internal=False, **kwargs):
        """Update documents in the database.

        There are a few actions that need to be done in conjunction with
        performing an update. Other methods can call this one to ensure
        that:

        - ``_id`` is checked (can be overridden by ``upsert``)
        - ``Meta.required_fields`` is enforced
        - field names are mapped according to ``Meta.field_map``
        - ``w`` and ``safe`` are correctly handled
        - atomic updates that go directly to the database also update
          the instance

        This method currently supports the following optional settings:

        ======== =======================================================
        Name     Description
        ======== =======================================================
        ``safe`` (optional) **DEPRECATED** use ``w`` instead
        ``w``    (optional) int -- the number of servers in a replica
                 set that must receive the update for it to be
                 successful
        ======== =======================================================

        :param fields: The update to perform.
        :type fields: dict.
        :param upsert: (optional) Whether or not to insert new
                       documents.
        :type upsert: bool.
        :param use_internal: (optional) Whether or not to use the values
                             in the internal document.
        :type use_internal: bool.
        :param \*\*kwargs: The optional settings.
        :type \*\*kwargs: \*\*kwargs.
        :raises: :class:`TypeError`

        .. versionadded:: 0.3.0
        """

        # Save characters
        cls = self.__class__

        def map_field_names_and_values(fields):
            """Maps field names and values.

            This method will take care of mapping the keys of ``fields``
            to the correct syntax for the query.

            When the  ``use_internal`` argument of
            :meth:`~simon.Model._update()` is set to ``True``, it will
            also update the values of ``fields`` from the internal
            document. If a field does not exist in the internal
            document, :class:`AttributeError` will be raised.

            :param fields: The document containing keys to map.
            :type fields: dict.
            :raises: :class:`AttributeError`

            .. versionadded:: 0.3.0
            """

            fields = map_fields(cls, fields, flatten_keys=True)
            if use_internal:
                try:
                    fields = dict((k, get_nested_key(self._document, k))
                                  for k in fields.keys())
                except KeyError:
                    # KeyError will be raised by get_nested_key() when a
                    # field isn't part of the internal document.
                    message = ("The '{0}' object does not have all of the "
                               "specified fields.".format(cls.__name__))
                    raise AttributeError(message)
            return fields

        # _id is required for updates unless the upsert flag has been
        # set by the caller.
        if not (upsert or '_id' in self._document):
            message = ("The '{0}' object cannot be updated because its '_id' "
                       "attribute has not been set.".format(cls.__name__))
            raise TypeError(message)

        # Capture the _id and make sure it's an Object ID
        id = self._document.get('_id')
        if id:
            id = guarantee_object_id(id)
            # Updates (as opposed to inserts) need spec to match against
            kwargs['spec'] = {'_id': id}

        # Map all the field names and values
        if is_atomic(fields):
            for k, v in fields.iteritems():
                fields[k] = map_field_names_and_values(v)
        else:
            fields = map_field_names_and_values(fields)
        # When placing fields in kwargs, make a copy so changes to
        # fields don't affect kwargs
        kwargs['document'] = fields.copy()

        # Enforce the required fields
        if self._meta.required_fields:
            has_required_fields = True

            if is_atomic(fields):
                # If performing an update that would result in fields
                # being removed, make sure that none of the required
                # fields are being removed.
                if '$unset' in fields:
                    if any(k in self._meta.required_fields for k
                            in fields['$unset']):
                        has_required_fields = False
            else:
                # For a document replacement, all of the required fields
                # must appear in the update.
                if any(k not in fields for k in self._meta.required_fields):
                    has_required_fields = False

            if not has_required_fields:
                message = ("The '{0}' object cannot be updated because it "
                           "must contain all of the required fields: {1}.")
                message = message.format(cls.__name__,
                                         ', '.join(self._meta.required_fields))
                raise TypeError(message)

        # Handling the write concern argument has been pushed off to
        # another method that is aware of what PyMongo supports.
        _set_write_concern(kwargs, self._meta.safe)

        # Which function are we calling?
        if not id:
            f = self._meta.db.insert
        else:
            f = self._meta.db.update

        result = f(**kwargs)

        if not id:
            # insert() will return the _id
            self._document['_id'] = result

        # For atomic updates, make sure the updates find their way back
        # to the internal document.
        # Right now this happens for all atomic updates, including when
        # called from save_fields(). I'd like to turn that one off.
        if is_atomic(fields):
            # After updating the document in the database, the instance
            # needs to be updated as well. Depending on the size of the
            # document, it may be time consuming to reload the entire
            # thing. Fortunately there is a way to just load the fields
            # that have been updated. Build a dictionary containing the
            # keys of the fields that need to be reloaded and retrieve
            # them from the database. Then apply the new values to the
            # instance's interal document.

            unset = fields.pop('$unset', None)
            if unset:
                # The fields also need to be removed from the object
                for k, v in unset.items():
                    if '.' in k:
                        # If the key contains a ., it is pointing to an
                        # embedded document, so remove the nested key
                        # from the dictionary; there is no attribute to
                        # remove from the instance.
                        self._document = remove_nested_key(self._document, k)
                    else:
                        self._document.pop(k, None)

            # If the only operation was an $unset, we're done.
            if not fields:
                return

            fields = dict((k, 1) for v in fields.values() for k in v.keys())
            doc = self._meta.db.find_one({'_id': id}, fields)

            # There's no need to update the _id
            doc.pop('_id', None)

            self._document = update_nested_keys(self._document, doc)

    # String representation methods

    def __repr__(self):
        return '<{0}: {1}>'.format(self.__class__.__name__, self)

    def __str__(self):
        return '{0} object'.format(self.__class__.__name__)

    def __unicode__(self):
        return u'{0} object'.format(self.__class__.__name__)

    # Attribute access methods

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
                # dict.
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

        if name in self._meta.core_attributes:
            # Set the attribute on the object. Trying to do this with a
            # simple assignment would result in recursion error.
            object.__setattr__(self, name, value)
        else:
            # Set the attribute in the internal document.

            # map_fields() requires a dict, so make a simple one and
            # then capture the first (only) key in the resulting
            # dict.
            mapped_name = map_fields(self.__class__, {name: 1},
                                     flatten_keys=True)
            mapped_name = mapped_name.keys()[0]

            # Build a dictionary that can be applied to the internal
            # document dictionary with update_nested_keys().  Do this
            # by iterating through the fields in mapped_name from right
            # to left.
            keys = mapped_name.split('.')
            keys.reverse()
            for x in keys:
                value = {x: value}

            self._document = update_nested_keys(self._document, value)

    # Rich comparison methods

    def __eq__(a, b):
        """Check equality of two instances"""

        # If either b isn't of the right type, a and b use different
        # database connections, or a and b use different collections,
        # the two cannot be equal.
        if not (isinstance(a, b.__class__) or isinstance(b, a.__class__)):
            return False
        if a._meta.database != b._meta.database:
            return False
        if a._meta.collection != b._meta.collection:
            return False

        a_id = a._document.get('_id', None)
        b_id = b._document.get('_id', None)

        # If either one of the instances has a value of _id, the two
        # cannot be equal.
        if not (a_id and b_id):
            return False

        return a_id == b_id

    def __ne__(a, b):
        """Check inequality of two instances"""

        return not a.__eq__(b)

    # Container methods

    def __contains__(self, name):
        """Check for a key in a document"""

        key = self._meta.field_map.get(name, name)
        return key in self._document
