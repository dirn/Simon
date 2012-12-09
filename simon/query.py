"""A custom query set that wraps around MongoDB cursors"""

__all__ = ('box', 'circle', 'near', 'polygon', 'Q', 'QuerySet', 'within')

import collections


def _validate_point(point, name=None, alternate_type=None):
    """Validates the type and length of a point.

    This method defines a point as either a ``list`` of exactly two
    elements. If will also accept a ``tuple``.

    :param point: The point to validate.
    :type point: list.
    :param name: (optional) A descriptive name to use when an
                 exception is raised.
    :type name: str.
    :param alternate_type: (optional) Alternate type(s) to check for.
    :type alternate_type: type or tuple of types.
    :raises: :class:`TypeError`, :class:`ValueError`.

    .. versionadded:: 0.1.0
    """

    exception = None

    type_to_check = alternate_type or (list, tuple)

    if not isinstance(point, type_to_check):
        exception = TypeError
    elif len(point) != 2:
        exception = ValueError

    if exception is not None:
        message = '{0} must be a list containing exactly 2 elements'
        raise exception(message.format(name or '`point`'))


def box(lower_left_point, upper_right_point):
    """Builds a ``$box`` query.

    This is a convenience method for ``$within`` queries that use
    ``$box`` as their shape.

    ``lower_left_point`` and ``upper_right_point`` are a pair of
    coordinates, each as a ``list``, that combine to define the bounds
    of the box in which to search.

    :param lower_left_point: The lower-left bound of the box.
    :type lower_left_point: list.
    :param upper_right_point: The upper-right bound of the box.
    :type upper_right_point: list.
    :returns: dict -- the ``$box`` query.
    :raises: :class:`TypeError`, :class:`ValueError`.

    .. versionadded:: 0.1.0
    """

    _validate_point(lower_left_point, '`lower_left_point`')
    _validate_point(upper_right_point, '`upper_right_point`')

    return within('box', lower_left_point, upper_right_point)


def circle(point, radius):
    """Builds a ``$circle`` query.

    This is a convenience method for ``$within`` queries that use
    ``$circle`` as their shape.

    :param point: The center of the circle.
    :type point: list.
    :param radius: The distance from the center of the circle.
    :type radius: int.
    :returns: dict -- the ``$circle`` query.
    :raises: :class:`TypeError`, :class:`ValueError`.

    .. versionadded:: 0.1.0
    """

    _validate_point(point)

    return within('circle', point, radius)


def near(point, max_distance=None, unique_docs=False):
    """Builds a ``$near`` query.

    This is a convenience method for more complex ``$near`` queries. For
    simple queries that simply use the point, the regular query syntax
    of ``field__near=[x, y]`` will suffice. This method provides a way
    to include ``$maxDistance`` and (if support is added)
    ``$uniqueDocs`` without needing to structure the query as
    ``field={'$near': [x, y], '$maxDistance': z}``.

    **Note** 2012-11-29 As of the current release of MongoDB (2.2),
    ``$near`` queries do not support the ``$uniqueDocs`` parameter. It
    is included here so that when support is added to MongoDB, no
    changes to the library will be needed.

    :param point: The point to use for the geospatial lookup.
    :type point: list, containing exactly two elements.
    :param max_distance: (optional) The maximum distance a point can be
                         from ``point``.
    :type max_distance: int.
    :param unique_docs: (optional) If ``True`` will only return unique
                        documents.
    :returns: dict -- the ``$near`` query.
    :raises: :class:`TypeError`, :class:`ValueError`.

    .. versionadded:: 0.1.0
    """

    _validate_point(point)

    # All queries containing the $near point
    query = {'$near': point}

    # Check for and add any of the optional operators as necessary
    if max_distance is not None:
        query['$maxDistance'] = max_distance
    if unique_docs:
        query['$uniqueDocs'] = unique_docs

    return query


def polygon(*points):
    """Builds a ``$polygon`` query.

    This is a convenience method for ``$within`` queries that use
    ``$polygon`` as their shape.

    ``points`` should either be expressed as a series of ``list``'s or a
    single ``dict`` containing ``dict``'s providing pairs of coordinates
    that behind the polygon.

    :param points: The bounds of the polygon.
    :type points: args.
    :returns: dict -- the ``$polygon`` query.
    :raises: :class:`TypeError`, :class:`ValueError`.

    .. versionadded:: 0.1.0
    """

    if len(points) > 1:
        # If there are more than one point, they should be a series of
        # lists defining the coordinates. They should be passed into
        # within() as *args.
        for p in points:
            _validate_point(p, 'Each point')

        return within('polygon', *points)

    elif points:
        # If there is one point, it should be a dictionary of nested
        # dictionaries providing the coordinates. They should be passed
        # into within() as **kwargs.
        points = points[0]

        if not isinstance(points, collections.Mapping):
            raise TypeError('`points` must either be a list of points or a '
                            'dict mapping of points.')
        if len(points) < 2:
            raise ValueError('`points` must either be a list of points or a '
                             'dict mapping of points.')

        for k, p in points.iteritems():
            _validate_point(p, name='Each point', alternate_type=collections.Mapping)

        return within('polygon', **points)

    # At this point, there are no points
    raise TypeError('`points` must either be a list of points or a '
                    'dict mapping of points.')


class Q(object):
    """A wrapper around a query condition to allow for logical ANDs and
    ORs through ``&`` and ``|``, respectively.

    .. versionadded:: 0.1.0
    """

    AND = '$and'
    OR = '$or'

    def __init__(self, **fields):
        """Creates a new filter.

        :param fields: Keyword arguments specifying the query.
        :type fields: kwargs.

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
    :class:`~simon.Model` objects

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

        :returns: int -- the number of documents.

        .. versionadded:: 0.1.0
        """

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

        :param keys: Names of the fields to sort by.
        :type keys: args.
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
                raise IndexError(
                    "No such item in QuerySet for '{0}' object".format(
                        self._cls.__name__ if self._cls else
                        self.__class__.__name__))

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

        return self.count()


def within(shape, *bounds, **bounds_map):
    """Builds a ``$within`` query.

    This is a convenience method for ``$within`` queries.

    :param shape: The shape of the bounding area.
    :type shape: str.
    :param bounds: Coordinate pairs defining the bounding area.
    :type bounds: args.
    :param bounds_map: Named coordinate pairs defining the bounding area.
    :type bounds_map: kwargs.
    :returns: dict -- the ``$within`` query.
    :raises: :class:`RuntimeError`.

    .. versionadded:: 0.1.0
    """

    if bounds and bounds_map:
        raise RuntimeError('Only one of `bounds` and `bounds_map` can be '
                           'provided.')

    # **kwargs trumps *args here. This decision was made so the *args
    # can be forced into a list with ease.
    bounds = bounds_map or list(bounds)

    query = {'$within': {'${0}'.format(shape): bounds}}

    return query
