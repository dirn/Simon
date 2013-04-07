"""The aggregation framework"""

from .utils import get_nested_key, ignored

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
