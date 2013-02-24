"""Meta options"""

from .connection import get_database, pymongo_supports_mongoclient

__all__ = ('Meta',)


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
        self.sort = None

        if pymongo_supports_mongoclient:
            self.write_concern = 1
        else:
            self.write_concern = True

    def add_to_original(self, cls, name):
        """Adds the ``Meta`` object to another class.

        .. versionadded:: 0.1.0
        """

        setattr(cls, name, self)

        if self.meta:
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                # Remove any "private" attributes.
                if name.startswith('_'):
                    del meta_attrs[name]

            # Add the known attributes to the instance
            for name in ('auto_timestamp', 'collection', 'database',
                         'field_map', 'map_id', 'required_fields', 'sort'):
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
