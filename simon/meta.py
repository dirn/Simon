"""Meta options"""

from collections import Mapping
from itertools import chain
import warnings

from bson import ObjectId

from ._compat import iterkeys, itervalues
from .connection import get_database, pymongo_supports_mongoclient
from .utils import map_fields

__all__ = ('Meta',)


class Meta(object):

    """Custom options for a :class:`~simon.Model`."""

    _db = None

    def __init__(self, meta):
        self.meta = meta

        # Set all the default option values.
        self.auto_timestamp = True
        self.database = 'default'
        self.field_map = {}
        self.map_id = True
        self.required_fields = None
        self.sort = None
        self.typed_fields = {}

        if pymongo_supports_mongoclient:
            self.write_concern = 1
        else:
            self.write_concern = True

        if pymongo_supports_mongoclient and hasattr(meta, 'safe'):
            message = 'safe has been deprecated. Please use w instead.'
            warnings.warn(message, DeprecationWarning)

    def add_to_original(self, cls, name):
        """Add the ``Meta`` object to another class."""

        setattr(cls, name, self)

        if self.meta:
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                # Remove any "private" attributes.
                if name.startswith('_'):
                    del meta_attrs[name]

            # Add the known attributes to the instance
            for name in ('auto_timestamp', 'collection', 'database',
                         'field_map', 'map_id', 'required_fields', 'sort',
                         'typed_fields'):
                if name in meta_attrs:
                    setattr(self, name, meta_attrs.pop(name))

            if pymongo_supports_mongoclient:
                if 'w' in meta_attrs:
                    self.write_concern = meta_attrs.pop('w')
                elif 'safe' in meta_attrs:
                    self.write_concern = int(meta_attrs.pop('safe'))
            else:
                if 'safe' in meta_attrs:
                    self.write_concern = meta_attrs.pop('safe')
                elif 'w' in meta_attrs:
                    self.write_concern = bool(meta_attrs.pop('w'))

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
        self.core_attributes = tuple(chain(iterkeys(cls.__dict__),
                                          ('_document',)))

        # field_map must be a valid mapping
        if not isinstance(self.field_map, Mapping):
            raise TypeError("'field_map' must be a dict.")

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

        # Make sure that only types (or None) as used with typed_fields.
        for field in itervalues(self.typed_fields):
            if field is None or isinstance(field, type):
                # The field is properly typed.
                continue

            if (isinstance(field, list) and len(field) == 1
                    and isinstance(field[0], type)):
                # The field is a properly typed list.
                continue

            message = 'Fields must be a type, a typed list, or None.'
            raise TypeError(message)
        # Apply field_map to typed_fields now rather than each time it's
        # needed.
        self.typed_fields = map_fields(self.field_map, self.typed_fields,
                                       flatten_keys=True)

        # If _id hasn't been specified, add it.
        if '_id' not in self.typed_fields:
            self.typed_fields['_id'] = ObjectId

    @property
    def db(self):
        """Return the :class:`~pymongo.collection.Collection`."""

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
