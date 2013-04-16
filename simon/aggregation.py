"""The aggregation framework"""

import collections

from .utils import get_nested_key, ignored, map_fields

__all__ = ('Pipeline',)


class Pipeline(object):
    """A wrapper around aggregation framework pipelines

    .. versionadded:: 0.7.0
    """

    def __init__(self, cls=None):
        """Creates a new pipeline.

        :param cls: Model class to map the results to.
        :type cls: :class:`~simon.Model`.
        """

        self._project = {}
        self._match = {}
        self._limit = None
        self._skip = None
        self._unwind = []
        self._group = {}
        self._geonear = {}

        # Store a reference to the class so that fields can be mapped
        self._cls = cls

    def group(self, id, **computed_fields):
        """Groups documents together for calculating aggregates.

        This method can be used to control the ``$group`` operator of
        the aggregation query. It's syntax is similar to that of
        :meth:`~simon.Model.find`.

        The current instance is returned so that calls to other methods
        can be chained together.

        :param id: Identifier for the group being created.
        :type id: str.
        :param \*\*computed_fields: Keyword arguments specifying
                                    aggregate keys.
        :type \*\*computed_fields: \*\*kwargs.
        :returns: :class:`~simon.aggregation.Pipeline` -- the current
                  instance.
        """

        def add_bling(d):
            """Adds the `$` to field names.

            Recruses through a ``dict`` looking for string values. When
            one is found, a ``$`` is prepended to the value if one isn't
            found.

            :param d: The ``$group`` document or a nested part of it.
            :type d: dict.
            """

            for k in d:
                value = d[k]

                if isinstance(value, collections.Mapping):
                    # Recurse through nested dictionaries.
                    add_bling(value)
                    continue

                if isinstance(value, basestring) and value[0] != '$':
                    d[k] = '${0}'.format(value)

        self._group['_id'] = id

        # Use map_fields() to flatten keys and handle the aggregation
        # operators like $sum and $first.
        fields = map_fields({}, computed_fields, with_operators=True,
                            flatten_keys=True)

        for k, v in fields.iteritems():
            self._group[k] = v

        add_bling(self._group)

        return self

    def limit(self, limit):
        """Applies a limit to the number of documents in the pipeline.

        This method can be used to control the ``$limit`` operator of
        the aggregation query.

        The current instance is returned so that calls to other methods
        can be chained together.

        :param limit: Number of documents to return.
        :type limit: int.
        :returns: :class:`~simon.aggregation.Pipeline` -- the current
                  instance.
        """
        self._limit = limit

        return self

    def match(self, **fields):
        """Adds conditions to the pipeline.

        This method can be used to control the ``$match`` operator of
        the aggregation query. It's syntax is similar to that of
        :meth:`~simon.Model.find`.

        The current instance is returned so that calls to other methods
        can be chained together.

        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.aggregation.Pipeline` -- the current
                  instance.
        """

        if self._cls:
            # If there is a Model class, map the fields.
            field_map = self._cls._meta.field_map
        else:
            # If there isn't, map_fields() still needs to be called,
            # so fake it.
            field_map = {}

        fields = map_fields(field_map, fields, with_operators=True,
                            flatten_keys=True)

        self._match.update(fields)

        return self

    def project(self, **fields):
        """Adds fields to reshape a document stream.

        This method can be used to control the ``$project`` operator of
        the aggregation query. It can be used to:

          - include fields (``field_name=True``)
          - exclude fields (``field_name=False``)

        The current instance is returned so that calls to other methods
        can be chained together.

        :param \*\*fields: Fields to include in the ``$project``
                           document.
        :type \*\*fields: \*\*kwargs.
        :returns: :class:`~simon.aggregation.Pipeline` -- the current
                  instance.
        """

        for k, v in fields.iteritems():
            # Field names will include '__' (Simon's convention with
            # kwargs), but get_nested_key() is looking for '.'.
            k = k.replace('__', '.')

            if self._cls:
                # Look for the kwarg in the field map. If it isn't
                # there, control will resume outside the with.
                with ignored(KeyError):
                    value = get_nested_key(self._cls._meta.field_map, k)

                    if v:
                        # If the field is being included, perform the
                        # map through the $project operator and then
                        # move on to the next field.
                        self._project[value] = '${0}'.format(k)
                        continue

                    # The mapped field is being excluded, so exclude it
                    # through its real name.
                    k = value

            # Add the field to the projection, using 0 or 1.
            self._project[k] = int(v)

        return self

    def skip(self, skip):
        """Defines the number of documents to skip over in the pipeline.

        This method can be used to control the ``$skip`` operator of
        the aggregation query.

        The current instance is returned so that calls to other methods
        can be chained together.

        :param skip: Number of documents to skip.
        :type skip: int.
        :returns: :class:`~simon.aggregation.Pipeline` -- the current
                  instance.
        """
        self._skip = skip

        return self

    def unwind(self, *fields):
        """Peels element off an array individually.

        This method can be used to control the ``$unwind`` operator of
        the aggregation query.

        The current instance is returned so that calls to other methods
        can be chained together.

        :param \*fields: Names of fields to unwind.
        :type \*fields: \*args.
        :returns: :class:`~simon.aggregation.Pipeline` -- the current
                  instance.
        """

        # map_fields() needs a dictionary.
        fields = dict((k, 1) for k in fields)

        if self._cls:
            field_map = self._cls._meta.field_map
        else:
            field_map = {}

        fields = map_fields(field_map, fields, flatten_keys=True)

        for field in fields.iterkeys():
            # $unwind requires the field to prefixed with a $.
            field = '${0}'.format(field)

            if field not in self._unwind:
                self._unwind.append(field)

        return self
