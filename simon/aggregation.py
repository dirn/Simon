"""The aggregation framework"""

from .utils import get_nested_key

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
        self._unwind = {}
        self._group = {}
        self._geonear = {}

        # Store a reference to the class so that fields can be mapped
        self._cls = cls

    def project(self, include=None, exclude=None):
        """Adds fields to reshape a document stream.

        This method can be used to control the ``$project`` operator of
        the aggregation query. It can be used to:

          - include fields
          - exclude fields

        The current instance is returned so that calls to other methods
        can be chained together.

        :param include: A list of fields to include.
        :type include: list or tuple.
        :param exclude: A list of fields to exclude.
        :type exclude: list or tuple.
        :returns: :class:`~simon.aggregation.Pipeline` -- the current
                  instance.
        """

        def add_or_remove(fields, include_value):
            if not fields:
                return

            if not isinstance(fields, (list, tuple)):
                fields = (fields,)

            if not (self._cls and self._cls._meta.field_map):
                # If the instance isn't associated with a model class,
                # a simple dictionary is sufficient. This can also be
                # done for empty field maps.
                new_fields = dict((field, include_value) for field in fields)
            else:
                new_fields = {}

                # The $project operator can handle field mapping all by
                # itself by using `{real_name: $mapped_name}`. A field
                # can be found in the field map using get_nested_key().
                # This function will raise KeyError for fields that
                # aren't mapped and will returned the mapped name for
                # ones that are.
                for field in fields:
                    try:
                        key = get_nested_key(self._cls._meta.field_map, field)
                    except KeyError:
                        new_fields[field] = include_value
                    else:
                        if include_value:
                            new_fields[key] = '${0}'.format(field)
                        else:
                            new_fields[key] = include_value

            self._project.update(new_fields)

        add_or_remove(include, include_value=1)
        add_or_remove(exclude, include_value=0)

        return self
