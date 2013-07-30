"""The base Simon models"""

from collections import defaultdict
import sys
import warnings

from bson import ObjectId

from ._compat import get_next, iterkeys, itervalues, reraise, with_metaclass
from .exceptions import MultipleDocumentsFound, NoDocumentFound
from .meta import Meta
from .query import Q, QuerySet
from .utils import (current_datetime, get_nested_key, guarantee_object_id,
                    ignored, is_atomic, map_fields, remove_nested_key,
                    set_write_concern, update_nested_keys)

__all__ = ('Model',)


# In version 2.4, PyMongo introduced MongoClient as a
# replacement for Connection. Part of the new class is that the
# safe argument was deprecated in favor of the w argument.
# Whereas safe was a boolean, w is an integer representing the
# number of servers in a replica set that must receive the
# update before the write is considered successful.

class ModelMetaClass(type):

    """Define :class:`Model`."""

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
        """Assign attributes to the class."""

        if hasattr(value, 'add_to_original'):
            value.add_to_original(self, name)
        else:
            setattr(self, name, value)


class Model(with_metaclass(ModelMetaClass)):

    """Base class for all Simon models."""

    def __init__(self, **fields):
        """Assign all keyword arguments to the object's document.

        :param \*\*fields: Keyword arguments to add to the document.
        :type \*\*fields: \*\*kwargs.

        """

        # Assign an empty dictionary to _document so that it can be
        # used to store the real document's values
        self._document = {}

        fields = map_fields(self.__class__._meta.field_map, fields)

        # Add the fields to the document
        for k, v in fields.items():
            setattr(self, k, v)

    @classmethod
    def all(self):
        """Return all documents in the collection.

        If ``sort`` has been defined on the ``Meta`` class it will be
        used to order the records.

        """

        # All of the functionality already exists inside find(), so
        # just call that with no parameters.
        return self.find()

    @classmethod
    def create(cls, **fields):
        """Create a new document and saves it to the database.

        This is a convenience method to create a new document. It will
        instantiate a new ``Model`` from the keyword arguments,
        call ``save()``, and return the instance.

        If the model has the ``required_fields`` options set, a
        :class:`TypeError` will be raised if any of the fields are not
        provided.

        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :param \*\*fields: Keyword arguments to add to the document.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.Model` -- the new document.
        :raises: :class:`TypeError`

        """

        write_concern = {
            'safe': fields.pop('safe', None),
            'w': fields.pop('w', None),
        }

        new = cls(**fields)
        new.save(**write_concern)

        return new

    def delete(self, **kwargs):
        """Delete a single document from the database.

        This will delete the document associated with the instance
        object. If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :raises: :class:`TypeError`

        """

        id = self._document.get('_id')
        if not id:
            raise TypeError("The '{0}' object cannot be deleted because its "
                            "'{1}' attribute has not been set.".format(
                                self.__class__.__name__, '_id'))

        write_concern = {
            'safe': kwargs.pop('safe', None),
            'w': kwargs.pop('w', None),
        }

        set_write_concern(write_concern, self._meta.write_concern)

        self._meta.db.remove({'_id': id}, **write_concern)

        self._document = {}

    @classmethod
    def find(cls, q=None, *qs, **fields):
        """Return multiple documents from the database.

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
           ``qs`` is being deprecated in favor of ``q``

        """

        if qs:
            warnings.warn('qs has been deprecated. Please use q instead.',
                          DeprecationWarning)
            for filter in qs:
                q._filter.update(filter._filter)

        return cls._find(q=q, **fields)

    @classmethod
    def get(cls, q=None, *qs, **fields):
        """Return a single document from the database.

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
           ``qs`` is being deprecated in favor of ``q``

        """

        if qs:
            warnings.warn('qs has been deprecated. Please use q instead.',
                          DeprecationWarning)
            for filter in qs:
                q._filter.update(filter._filter)

        return cls._find(find_one=True, q=q, **fields)

    @classmethod
    def get_or_create(cls, **fields):
        """Return an existing or create a new document.

        This will find and return a single document matching the
        query specified through ``**fields``. If no document is found,
        a new one will be created.

        Along with returning the ``Model`` instance, a boolean value
        will also be returned to indicate whether or not the document
        was created.

        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: tuple -- the :class:`~simon.Model` and whether the
                  document was created.
        :raises: :class:`~simon.Model.MultipleDocumentsFound`

        """

        write_concern = {
            'safe': fields.pop('safe', None),
            'w': fields.pop('w', None),
        }

        try:
            return cls.get(**fields), False
        except cls.NoDocumentFound:
            return cls.create(safe=write_concern['safe'], w=write_concern['w'],
                              **fields), True

    def increment(self, field=None, value=1, **fields):
        """Perform an atomic increment.

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
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :param \*\*fields: Keyword arguments specifying fields and
                           increment values.
        :type \*\*fields: \*\*kwargs.
        :raises: :class:`TypeError`, :class:`ValueError`

        """

        write_concern = {
            'safe': fields.pop('safe', None),
            'w': fields.pop('w', None),
        }

        # There needs to be something to update.
        if field is None and not fields:
            raise ValueError('No fields have been specified.')

        update = {}

        # Both the field/value parameters and **fields can be used for
        # the update, so build a dictionary containing all of the fields
        # to increment and the value to increment each by.
        if field is not None:
            update[field] = value
        for k, v in fields.items():
            update[k] = v

        self._update({'$inc': update}, **write_concern)

    def pop(self, fields, **kwargs):
        """Perform an atomic pop.

        Values can be popped from either the end or the beginning of a
        list. To pop a value from the end of a list, specify the name of
        the field. The pop a  value from the beginning of a list,
        specify the name of the field with a ``-`` in front of it.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        :param fields: The names of the fields to pop from.
        :type fields: str, list, or tuple.
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :raises: :class:`TypeError`

        .. versionadded:: 0.5.0

        """

        if not fields:
            raise ValueError

        write_concern = {
            'safe': kwargs.pop('safe', None),
            'w': kwargs.pop('w', None),
        }

        if not isinstance(fields, (list, tuple)):
            fields = (fields,)

        update = {}

        for field in fields:
            if field[0] == '-':
                field = field[1:]
                direction = -1
            else:
                direction = 1

            update[field] = direction

        self._update({'$pop': update}, **write_concern)

    def pull(self, field=None, value=None, **fields):
        """Perform an atomic pull.

        With MongoDB there are two types of pull operations: ``$pull``
        and ``$pullAll``. As the name implies, ``$pullAll`` is intended
        to pull all values in a list from the field, while ``$pull`` is
        meant for single values.

        This method will determine the correct operator(s) to use based
        on the value(s) being pulled. Updates can consist of either
        operator alone or both together.

        This can be used to update a single field::

            >>> obj.pull(field, value)

        or to update multiple fields at a time::

            >>> obj.pull(field1=value1, field2=value2)

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        If no fields are indicated--either through ``field`` or through
        ``**fields``, a :class:`ValueError` will be raised.

        :param field: (optional) Name of the field to pull from.
        :type field: str.
        :param value: (optional) Value to pull from ``field``.
        :type value: scalar or list.
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :param \*\*fields: Keyword arguments specifying fields and
                           the values to pull.
        :type \*\*fields: \*\*kwargs.
        :raises: :class:`TypeError`, :class:`ValueError`

        .. versionadded:: 0.5.0

        """

        write_concern = {
            'safe': fields.pop('safe', None),
            'w': fields.pop('w', None),
        }

        # There needs to be something to update.
        if not (field and value) and not fields:
            raise ValueError('No fields have been specified.')

        # Most of the atomic methods treat update as a dict. A
        # defaultdict of dicts is being used here because, unlike most
        # of these, not only are two operators possible, but they can be
        # used together. defaultdict saves the trouble of having to
        # check for each operator in update before adding new keys to
        # the operator's dict.
        update = defaultdict(dict)

        # Both the field/value parameters and **fields can be used for
        # the update, so build a dictionary containing all of the fields
        # to pull from and the value(s) to pull from each.
        if not (field is None or value is None):
            fields[field] = value

        for k, v in fields.items():
            if isinstance(v, (list, tuple)):
                update['$pullAll'][k] = v
            else:
                update['$pull'][k] = v

        self._update(update, **write_concern)

    def push(self, field=None, value=None, allow_duplicates=True, **fields):
        """Perform an atomic push.

        With MongoDB there are three types of push operations:
        ``$push``, ``$pushAll``, add ``$addToSet``. As the name implies,
        ``$pushAll`` is intended to push all values from a list to the
        field, while ``$push`` is meant for single values. ``$addToSet``
        can be used with either type of value, but it will only add a
        value to the list if it doesn't already contain the value.

        This method will determine the correct operator(s) to use based
        on the value(s) being pushed. Setting ``allow_duplicates`` to
        ``False`` will use ``$addToSet`` instead of ``$push`` and
        ``$pushAll``. Updates that allow duplicates can combine
        ``$push`` and ``$pushAll`` together.

        This can be used to update a single field::

            >>> obj.push(field, value)

        or to update multiple fields at a time::

            >>> obj.push(field1=value1, field2=value2)

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        If no fields are indicated--either through ``field`` or through
        ``**fields``, a :class:`ValueError` will be raised.

        :param field: (optional) Name of the field to push to.
        :type field: str.
        :param value: (optional) Value to push to ``field``.
        :type value: scalar or list.
        :param allow_duplicates: (optional) Whether to allow duplicate
                                 values to be added to the list
        :type allow_duplicates: bool.
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :param \*\*fields: Keyword arguments specifying fields and
                           the values to push.
        :type \*\*fields: \*\*kwargs.
        :raises: :class:`TypeError`, :class:`ValueError`

        .. versionadded:: 0.5.0

        """

        write_concern = {
            'safe': fields.pop('safe', None),
            'w': fields.pop('w', None),
        }

        # There needs to be something to update.
        if not (field and value) and not fields:
            raise ValueError('No fields have been specified.')

        # Most of the atomic methods treat update as a dict. A
        # defaultdict of dicts is being used here because, unlike most
        # of these, not only are two operators possible, but they can be
        # used together. defaultdict saves the trouble of having to
        # check for each operator in update before adding new keys to
        # the operator's dict.
        update = defaultdict(dict)

        # Both the field/value parameters and **fields can be used for
        # the update, so build a dictionary containing all of the fields
        # to push to and the value(s) to push to each.
        if not (field is None or value is None):
            fields[field] = value

        for k, v in fields.items():
            if isinstance(v, (list, tuple)):
                if allow_duplicates:
                    update['$pushAll'][k] = v
                else:
                    update['$addToSet'][k] = {'$each': v}
            else:
                update['$push' if allow_duplicates else '$addToSet'][k] = v

        self._update(update, **write_concern)

    def raw_update(self, fields, **kwargs):
        """Perform an update using a raw document.

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
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :raises: :class:`TypeError`

        """

        write_concern = {
            'safe': kwargs.pop('safe', None),
            'w': kwargs.pop('w', None),
        }

        self._update(fields, **write_concern)

    def remove_fields(self, fields, **kwargs):
        """Remove the specified fields from the document.

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
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :raises: :class:`TypeError`

        """

        write_concern = {
            'safe': kwargs.pop('safe', None),
            'w': kwargs.pop('w', None),
        }

        # fields can contain a single item as a string. If it's not a
        # list or tuple, make it one. Otherwise the generators below
        # would iterate over each character in the string rather than
        # treating it as a single item list.
        if not isinstance(fields, (list, tuple)):
            fields = (fields,)

        fields = dict((k, 1) for k in fields)

        self._update({'$unset': fields}, **write_concern)

    def rename(self, field_from=None, field_to=None, **fields):
        """Perform an atomic rename.

        This can be used to update a single field::

            >>> obj.rename(original, new)

        or to update multiple fields at a time::

            >>> obj.increment(original1=new1, original2=new2)

        Note that the latter does **not** set the values of the fields,
        but rather specifies the name they should be renamed to.

        If the document does not have an ``_id``--this will
        most likely indicate that the document has never been saved--
        a :class:`TypeError` will be raised.

        If no fields are indicated--either through ``field_from`` and
        ``field_to`` or through ``**fields``, a :class:`ValueError` will
        be raised.

        :param field_from: (optional) Name of the field to rename.
        :type field_from: str.
        :param field_to: (optional) New name for ``field_from``.
        :type field_to: int.
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :param \*\*fields: Keyword arguments specifying fields and their
                           new names.
        :type \*\*fields: \*\*kwargs.
        :raises: :class:`TypeError`, :class:`ValueError`

        .. versionadded:: 0.5.0

        """

        write_concern = {
            'safe': fields.pop('safe', None),
            'w': fields.pop('w', None),
        }

        # There needs to be something to update.
        if not (field_from and field_to) and not fields:
            raise ValueError('No fields have been specified.')

        update = {}

        # Both the field/value parameters and **fields can be used for
        # the update, so build a dictionary containing all of the fields
        # to rename and the names to rename each to.
        if not (field_from is None or field_to is None):
            update[field_from] = field_to
        for k, v in fields.items():
            update[k] = v

        self._update({'$rename': update}, **write_concern)

    def save(self, **kwargs):
        """Save the document to the database.

        When saving a new document for a model with ``auto_timestamp``
        set to ``True``, ``created`` will be added with the current
        datetime in UTC. ``modified`` will always be set with the
        current datetime in UTC.

        If the model has the ``required_fields`` options set, a
        :class:`TypeError` will be raised if any of the fields have not
        been associated with the instance.

        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :raises: :class:`TypeError`

        .. versionchanged:: 0.4.0
           ``created`` is always added to inserted documents when
           ``auto_timestamp`` is ``True``

        """

        write_concern = {
            'safe': kwargs.pop('safe', None),
            'w': kwargs.pop('w', None),
        }

        # Use a copy of the internal document so _id can be safely
        # removed.
        fields = self._document.copy()
        fields.pop('_id', None)

        # Associate the current datetime (in UTC) with the created and
        # modified fields. created will only be added to documents that
        # are being inserted. The values are associated with the copy of
        # the internal document so that the internal document will be in
        # a consistent state if _update() raises an exception.
        if self._meta.auto_timestamp:
            now = current_datetime()
            if '_id' not in self._document:
                fields['created'] = now
            fields['modified'] = now

        try:
            # Use upsert=True so new documents can be inserted.
            self._update(fields, upsert=True, **write_concern)
        except:
            # Raise the exception that was caught
            e = sys.exc_info()
            reraise(e[0], e[1], e[2])
        else:
            # Because _update() didn't raise an exception, the created
            # and modified values can be associated with the internal
            # document.
            if self._meta.auto_timestamp:
                if 'created' in fields:
                    self._document['created'] = fields['created']
                if 'modified' in fields:
                    self._document['modified'] = fields['modified']

    def save_fields(self, fields, **kwargs):
        """Save the specified fields.

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
        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :raises: :class:`AttributeError`, :class:`TypeError`

        """

        write_concern = {
            'safe': kwargs.pop('safe', None),
            'w': kwargs.pop('w', None),
        }

        # fields can contain a single item as a string. If it's not a
        # list or tuple, make it one. Otherwise the generators below
        # would iterate over each character in the string rather than
        # treating it as a single item list.
        if not isinstance(fields, (list, tuple)):
            fields = (fields,)

        update = dict((k, 1) for k in fields)

        # Set use_internal so that the real values will come from
        # self._document instead of the document passed to _update().
        self._update({'$set': update}, use_internal=True, **write_concern)

    def update(self, **fields):
        """Perform an atomic update.

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

        :param safe: (optional) **DEPRECATED** Use ``w`` instead.
        :type safe: bool.
        :param w: (optional) The number of servers that must receive the
                  update for it to be successful.
        :type w: int.
        :param \*\*fields: The fields to update.
        :type \*\*fields: \*\*kwargs.
        :raises: :class:`TypeError`

        """

        write_concern = {
            'safe': fields.pop('safe', None),
            'w': fields.pop('w', None),
        }

        self._update({'$set': fields}, **write_concern)

    # Database interaction methods

    @classmethod
    def _find(cls, q=None, find_one=False, **fields):
        """Return documents in the database.

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

        .. versionchanged:: 0.6.0
           ``_id`` can be a type other than :class:`~pymongo.ObjectId`

        .. versionadded:: 0.3.0

        """

        # If there is a Q object, add it to the spec document.
        if isinstance(q, Q):
            fields.update(q._filter)

        query = map_fields(cls._meta.field_map, fields, flatten_keys=True,
                           with_operators=True)

        # If querying by the _id, make sure it's an Object ID, but only
        # if it's typed as one.
        if '_id' in query and cls._meta.typed_fields['_id'] == ObjectId:
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
        :raises: :class:`AttributeError`, :class:`TypeError`

        .. versionchanged:: 0.6.0
           ``typed_fields`` is enforced
           ``_id`` can be a type other than :class:`~pymongo.ObjectId`
           ``safe`` is being deprecated in favor of ``w`` (PyMongo 2.4+)

        .. versionadded:: 0.3.0

        """

        # Save characters
        cls = self.__class__

        def check_typed_fields(fields):
            """Check that fields are of the correct type.

            This function checks the fields that are being saved and
            makes sure they are of the correct type. If a field isn't
            of the right type, :class:`TypeError` will be raised.

            :param fields: The document to check.
            :type fields: dict.
            :raises: :class:`TypeError`

            .. versionadded:: 0.6.0

            """

            for k, v in cls._meta.typed_fields.items():
                if v is None:
                    # None means the field can be any type. This will
                    # probably be used most often with _id.
                    continue

                try:
                    value = get_nested_key(fields, k)
                except KeyError:
                    # Nothing to see here
                    continue

                if isinstance(v, list):
                    if all(isinstance(x, v[0]) for x in value):
                        # All values in the list are of the right type.
                        continue
                elif isinstance(value, v):
                    # The value is of the right type.
                    continue

                message = ("The '{0}' object cannot be updated because "
                           "its '{1}' field must be {2}.")
                message = message.format(cls.__name__, k, v)
                raise TypeError(message)

        def map_field_names_and_values(fields):
            """Map field names and values.

            This function will take care of mapping the keys of
            ``fields`` to the correct syntax for the query.

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

            fields = map_fields(cls._meta.field_map, fields, flatten_keys=True)
            if use_internal:
                try:
                    fields = dict((k, get_nested_key(self._document, k))
                                  for k in fields)
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

        # Capture the _id.
        id = self._document.get('_id')
        if id:
            # Make sure it's an Object ID, but only if it's typed as
            # one.
            if cls._meta.typed_fields['_id'] == ObjectId:
                id = guarantee_object_id(id)
            # Updates (as opposed to inserts) need spec to match against
            kwargs['spec'] = {'_id': id}

        # Map all the field names and values
        if is_atomic(fields):
            for k, v in fields.items():
                fields[k] = map_field_names_and_values(v)
                if k == '$rename':
                    for field_from, field_to in fields[k].items():
                        mapped = map_fields(cls._meta.field_map, {field_to: 1},
                                            flatten_keys=True)
                        fields[k][field_from] = get_next(iterkeys(mapped))()
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
                elif '$rename' in fields:
                    if any(k in self._meta.required_fields for k
                            in fields['$rename']):
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

        # Enforce the typed fields
        if cls._meta.typed_fields:
            if is_atomic(fields):
                for k, v in fields.items():
                    if k == '$rename':
                        # $rename doesn't alter the value stored in the
                        # database. Therefore the value associated with
                        # the instance is not a reliable way to tell if
                        # a renamed field will be of the wrong type. The
                        # only way to really know would be to load the
                        # value from the database. Rather than incurring
                        # this blocking operation, renaming to a typed
                        # field will trigger a UserWarning instead.
                        if any(v in cls._meta.typed_fields
                               for v in itervalues(v)):
                            message = ('You are renaming a typed field. Its '
                                       'value may not be of the correct type.')
                            warnings.warn(message, UserWarning)
                    elif k != '$unset':
                        # Fields don't need to be checked if they're
                        # being removed.
                        check_typed_fields(v)
            else:
                check_typed_fields(fields)

        # Handling the write concern argument has been pushed off to
        # another method that is aware of what PyMongo supports.
        set_write_concern(kwargs, self._meta.write_concern)

        # Which function are we calling?
        if not id:
            # PyMongo's insert() method calls the argument doc_or_docs
            # instead of document.
            kwargs['doc_or_docs'] = kwargs.pop('document')
            f = cls._meta.db.insert
        else:
            if cls._meta.typed_fields['_id'] != ObjectId:
                # In order to handle inserting documents with custom
                # values for _id an upsert is needed.
                kwargs['upsert'] = True
            f = cls._meta.db.update

        result = f(**kwargs)

        if not id:
            # insert() will return the _id
            self._document['_id'] = result

        # For atomic updates, make sure the updates find their way back
        # to the internal document.
        if not use_internal and is_atomic(fields):
            # After updating the document in the database, the instance
            # needs to be updated as well. Depending on the size of the
            # document, it may be time consuming to reload the entire
            # thing. Fortunately there is a way to just load the fields
            # that have been updated. Build a dictionary containing the
            # keys of the fields that need to be reloaded and retrieve
            # them from the database. Then apply the new values to the
            # instance's internal document.

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

            rename = fields.get('$rename')
            if rename:
                # The old fields need to be removed. The new fields will
                # be added through the find_one() below.
                for k in rename:
                    if '.' in k:
                        self._document = remove_nested_key(self._document, k)
                    else:
                        self._document.pop(k, None)
                fields['$rename'] = dict((v, 1) for v in itervalues(rename))

            # If the only operation was an $unset, we're done.
            if not fields:
                return

            fields = dict((k, 1) for v in itervalues(fields) for k in v)
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
        """Remove a key from the document."""

        # Normally name can be used here. In this instance, however,
        # name is needed to remove the attribute from the object.
        # key is used in case there is a different key used for the
        # document than for the object.
        key = self._meta.field_map.get(name, name)
        if key in self._document:
            del self._document[key]

            # The deletion of the attribute is now complete, get out
            # before an AttributeError is raised by the super delete.
            return

        object.__delattr__(self, name)

    def __getattr__(self, name):
        """Return a value from the document."""

        # The first thing to look for is nested keys
        if '__' in name or '.' in name:
            if '__' in name:
                # map_fields() requires a dict, so make a simple one and
                # then capture the first (only) key in the resulting
                # dict.
                mapped_name = map_fields(self.__class__._meta.field_map,
                                         {name: 1}, flatten_keys=True)
                mapped_name = get_next(iterkeys(mapped_name))()
            else:
                mapped_name = name

            with ignored(AttributeError):
                # If not, give it a go the normal way.
                return get_nested_key(self._document, mapped_name)

        # If the attribute is a key in the document, use it.
        name = self._meta.field_map.get(name, name)
        if not name in self._document:
            message = "'{0}' object has no attribute '{1}'."
            raise AttributeError(message.format(self.__class__.__name__, name))
        return self._document[name]

    def __setattr__(self, name, value):
        """Set a document value."""

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
            mapped_name = map_fields(self.__class__._meta.field_map, {name: 1},
                                     flatten_keys=True)
            mapped_name = get_next(iterkeys(mapped_name))()

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
        """Check equality of two instances."""

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
        """Check inequality of two instances."""

        return not a.__eq__(b)

    # Container methods

    def __contains__(self, name):
        """Check for a key in a document."""

        key = self._meta.field_map.get(name, name)
        return key in self._document
