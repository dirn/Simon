"""The aggregation framework"""

__all__ = ('Pipeline',)


class Pipeline(object):
    """A wrapper around aggregation framework pipelines

    .. versionadded:: 0.7.0
    """

    def __init__(self):
        self._project = {}
        self._match = {}
        self._limit = None
        self._skip = None
        self._unwind = {}
        self._group = {}
        self._geonear = {}

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

        .. versionadded:: 0.7.0
        """

        def add_or_remove(fields, include_value):
            if not fields:
                return

            if not isinstance(fields, (list, tuple)):
                fields = (fields,)

            new_fields = dict((field, include_value) for field in fields)

            self._project.update(new_fields)

        add_or_remove(include, include_value=1)
        add_or_remove(exclude, include_value=0)

        return self
