"""Helper functions to ease geospatial queries"""

import collections

__all__ = ('box', 'circle', 'near', 'polygon', 'within')


def _validate_point(point, name=None, alternate_type=None):
    """Validate the type and length of a point.

    This function defines a point as either a ``list`` of exactly two
    elements. If will also accept a ``tuple``.

    :param point: The point to validate.
    :type point: list.
    :param name: (optional) A descriptive name to use when an
                 exception is raised.
    :type name: str.
    :param alternate_type: (optional) Alternate type(s) to check for.
    :type alternate_type: type or tuple of types.
    :raises: :class:`TypeError`, :class:`ValueError`.

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
    """Build a ``$box`` query.

    This is a convenience function for ``$within`` queries that use
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

    """

    _validate_point(lower_left_point, '`lower_left_point`')
    _validate_point(upper_right_point, '`upper_right_point`')

    return within('box', lower_left_point, upper_right_point)


def circle(point, radius):
    """Build a ``$circle`` query.

    This is a convenience function for ``$within`` queries that use
    ``$circle`` as their shape.

    :param point: The center of the circle.
    :type point: list.
    :param radius: The distance from the center of the circle.
    :type radius: int.
    :returns: dict -- the ``$circle`` query.
    :raises: :class:`TypeError`, :class:`ValueError`.

    """

    _validate_point(point)

    return within('circle', point, radius)


def near(point, max_distance=None, unique_docs=False):
    """Build a ``$near`` query.

    This is a convenience function for more complex ``$near`` queries.
    For simple queries that simply use the point, the regular query
    syntax of ``field__near=[x, y]`` will suffice. This function
    provides a way to include ``$maxDistance`` and (if support is added)
    ``$uniqueDocs`` without needing to structure the query as
    ``field={'$near': [x, y], '$maxDistance': z}``.

    .. note::
       As of the current release of MongoDB (2.2), ``$near`` queries do
       not support the ``$uniqueDocs`` parameter. It is included here so
       that when support is added to MongoDB, no changes to the library
       will be needed. - 11 November 2012

    :param point: The point to use for the geospatial lookup.
    :type point: list, containing exactly two elements.
    :param max_distance: (optional) The maximum distance a point can be
                         from ``point``.
    :type max_distance: int.
    :param unique_docs: (optional) If ``True`` will only return unique
                        documents.
    :returns: dict -- the ``$near`` query.
    :raises: :class:`TypeError`, :class:`ValueError`.

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
    """Build a ``$polygon`` query.

    This is a convenience function for ``$within`` queries that use
    ``$polygon`` as their shape.

    ``points`` should either be expressed as a series of ``list``'s or a
    single ``dict`` containing ``dict``'s providing pairs of coordinates
    that behind the polygon.

    :param \*points: The bounds of the polygon.
    :type \*points: \*args.
    :returns: dict -- the ``$polygon`` query.
    :raises: :class:`TypeError`, :class:`ValueError`.

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

        for k, p in points.items():
            _validate_point(p, name='Each point',
                            alternate_type=collections.Mapping)

        return within('polygon', **points)

    # At this point, there are no points
    raise TypeError('`points` must either be a list of points or a '
                    'dict mapping of points.')


def within(shape, *bounds, **bounds_map):
    """Build a ``$within`` query.

    This is a convenience function for ``$within`` queries.

    :param shape: The shape of the bounding area.
    :type shape: str.
    :param \*bounds: Coordinate pairs defining the bounding area.
    :type \*bounds: \*args.
    :param \*\*bounds_map: Named coordinate pairs defining the bounding area.
    :type \*\*bounds_map: \*\*kwargs.
    :returns: dict -- the ``$within`` query.
    :raises: :class:`RuntimeError`.

    """

    if bounds and bounds_map:
        raise RuntimeError("Only one of 'bounds' and 'bounds_map' can be "
                           "provided.")

    # **kwargs trumps *args here. This decision was made so the *args
    # can be forced into a list with ease.
    bounds = bounds_map or list(bounds)

    query = {'$within': {'${0}'.format(shape): bounds}}

    return query
