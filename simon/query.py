"""Query functionality"""

__all__ = ('Q', 'QuerySet')


class Q(object):
    """A wrapper around a query condition to allow for logical ANDs and
    ORs through ``&`` and ``|``, respectively.

    .. versionadded:: 0.1.0
    """

    AND = '$and'
    OR = '$or'

    def __init__(self, **fields):
        """Creates a new filter.

        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.

        .. versionadded:: 0.1.0
        """

        self._filter = fields

    def _add_filter(self, filter, condition):
        """Adds to filters together.

        :param filter: The filter to add.
        :type filter: :class:`Q`.
        :returns: :class:`Q` -- the new filter.
        :raises: :class:`TypeError`.

        .. versionadded:: 0.1.0
        """

        if not isinstance(filter, Q):
            raise TypeError('`{0}` objects can only be combined with other '
                            '`{0}` objects.'.format(self.__class__.__name__))

        # If the filters are identical, get out early.
        if self._filter == filter._filter:
            return self

        obj = type(self)()

        # If the controlling Q object already contains a filter using
        # condition, the new filter should be added to the existing
        # filter. If condition doesn't already exist, though, a new
        # filter should be created.
        if condition in self._filter:
            obj._filter = self._filter
        else:
            obj._filter = {condition: [self._filter, filter._filter]}

        # Only add the new filter if it isn't already set.
        if filter._filter not in obj._filter[condition]:
            obj._filter[condition].append(filter._filter)

        return obj

    def __and__(self, filter):
        """Adds a new filter using ``$and``.

        :param filter: The filter to add.
        :type filter: :class:`Q`.
        :returns: :class:`Q` -- the new filter.

        .. versionadded:: 0.1.0
        """

        return self._add_filter(filter, self.AND)

    def __or__(self, filter):
        """Adds a new filter using ``$or``.

        :param filter: The filter to add.
        :type filter: :class:`Q`.
        :returns: :class:`Q` -- the new filter.

        .. versionadded:: 0.1.0
        """
        return self._add_filter(filter, self.OR)


class QuerySet(object):
    """A query set that wraps around MongoDB cursors and returns
    :class:`~simon.Model` objects.

    .. versionadded:: 0.1.0
    """

    def __init__(self, cursor=None, cls=None):
        """Creates a new query set.

        :param cursor: The result set.
        :type cursor: :class:`~pymongo.cursor.Cursor`.
        :param cls: Model class to map the results to.
        :type cls: :class:`~simon.Model`.

        .. versionadded:: 0.1.0
        """

        self._cls = cls
        self._cursor = cursor
        self._count = None

        self._items = []

    def count(self):
        """Gets the number of documents in the :class:`QuerySet`.

        If no cursor has been associated with the query set,
        ``TypeError`` will be raised.

        :returns: int -- the number of documents.
        :raises: :class:`TypeError`.

        .. versionadded:: 0.1.0
        """

        if not self._cursor:
            raise TypeError(
                "The '{0}' has no cursor associated with it.".format(
                    self.__class__.__name__))

        # Store the count interally so that the call doesn't need to
        # be made over and over
        if self._count is None:
            # Without setting with_limit_and_skip to True, the count
            # would reflect all documents matching the query, not just
            # those available through the cursor
            self._count = self._cursor.count(with_limit_and_skip=True)
        return self._count

    def distinct(self, key):
        """Gets a list of distinct values for ``key`` across all
        documents in the :class:`QuerySet`.

        :param key: Name of the key.
        :type key: str.
        :returns: list -- distinct values for the key.

        .. versionadded:: 0.1.0
        """

        # If the QuerySet has a model class, check for key in the
        # class's field map
        if self._cls:
            key = self._cls._meta.field_map.get(key, key)

        return self._cursor.distinct(key)

    def limit(self, limit):
        """Applies a limit to the number of documents in the
        :class:`QuerySet`.

        :param limit: Number of documents to return.
        :type limit: int.
        :returns: :class:`QuerySet` -- the documents with the limit
                  applied.

        .. versionadded:: 0.1.0
        """

        # Make sure to clone the cursor so as not to alter the original
        return QuerySet(self._cursor.clone().limit(limit), self._cls)

    def skip(self, skip):
        """Skips a number of documents in the :class:`QuerySet`.

        :param skip: Number of documents to skip.
        :type skip: int.
        :returns: :class:`QuerySet` -- the documents remaining.

        .. versionadded:: 0.1.0
        """

        # Make sure to clone the cursor so as not to alter the original
        return QuerySet(self._cursor.clone().skip(skip), self._cls)

    def sort(self, *keys):
        """Sorts the documents in the :class:`QuerySet`.

        By default all sorting is done in ascending order. To switch
        any key to sort in descending order, place a ``-`` before the
        name of the key.

        ..

            >>> qs.sort('id')
            >>> qs.sort('grade', '-score')

        :param \*keys: Names of the fields to sort by.
        :type \*keys: \*args.
        :returns: :class:`QuerySet` -- the sorted documents.

        .. versionadded:: 0.1.0
        """

        # Build the list of sorting (key, direction) pairs. If the
        # QuerySet has a model class, check for the key in the class's
        # field map
        sorting = []
        for key in keys:
            if key[0] == '-':
                key = key[1:]
                direction = -1
            else:
                direction = 1

            if self._cls:
                key = self._cls._meta.field_map.get(key, key)

            sorting.append((key, direction))

        # Make sure to clone the cursor so as not to alter the original
        return QuerySet(self._cursor.clone().sort(sorting), self._cls)

    def _fill_to(self, index):
        """Builds the cache of documents retrieved from the cursor.

        :class:`QuerySet` objects keep documents loaded from the
        cursor in an internal cache to avoid repetitive trips to the
        database. In order to properly load a cache that can be
        iterated over, any documents before the requested one must be
        loaded.

        :param index: Index to fill the cache to.
        :type index: int.

        .. versionadded:: 0.1.0
        """

        # If the specified index is beyond the total number of
        # documents, load until the last document
        if index >= self.count():
            # Because count() has been called, _count can be used to
            # avoid the overhead of calling a function
            index = self._count - 1

        # Iterate over all documents between the last one loaded and
        # the one specified by index. If the :class:`QuerySet` has a
        # model class, store an instance of the class in the cache,
        # otherwise store the raw document
        try:
            for x in range(len(self._items), index + 1):
                item = next(self._cursor)
                if self._cls:
                    item = self._cls(**item)
                self._items.append(item)
        except StopIteration:
            # This should never happen because of the check at the top
            # of this method, but it's here just in case something
            # crazy happens, like a document is added to the cursor
            # (since new documents can suddenly appear in a cursor)
            # during iteration
            pass

    def __getitem__(self, k):
        """Gets an item or slice from the :class:`QuerySet`.

        .. versionadded:: 0.1.0
        """

        # If k is a slice, set the bound to either the slice's stop
        # attribute. If there is no value for stop, use the total
        # number of items in the QuerySet
        if isinstance(k, slice):
            if k.stop is not None:
                bound = int(k.stop)
            else:
                bound = self.count()
        # If k isn't a slice, make sure it's a valid value and use
        # it as the bound
        else:
            if k < 0:
                raise TypeError('Negative indexing is not supported.')
            elif k >= self.count():
                if self._cls:
                    message = "No such item in '{0}' for '{1}' object".format(
                        self.__class__.__name__,
                        self._cls.__name__ if self._cls else
                        self.__class__.__name__)
                else:
                    message = "No such item in '{0}'".format(
                        self.__class__.__name__)
                raise IndexError(message)

            bound = k + 1

        # If the requested index or slice hasn't been loaded into the
        # cache yet, do it now
        if len(self._items) < bound:
            self._fill_to(bound)

        return self._items[k]

    def __iter__(self):
        """Iterates through the documents in the cursor.

        .. versionadded:: 0.1.0
        """

        for x in range(0, self.count()):
            # Fetch the document from the cursor if it hasn't already
            # been loaded
            if len(self._items) <= x:
                self._fill_to(x)

            yield self._items[x]

    def __len__(self):
        """Gets the length of the :class:`QuerySet`.

        .. versionadded:: 0.1.0
        """

        try:
            return self.count()
        except TypeError:
            return 0
