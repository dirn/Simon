"""Query functionality"""

import pymongo

from ._compat import get_next, iterkeys, range
from .utils import ignored, map_fields

__all__ = ('Q', 'QuerySet')


class Q(object):

    """Wrapper around logical ANDs and ORs.

    The :class:`~simon.query.Q` class serves as a wrapper around query
    conditions to allow for logical ANDs and ORs through ``&`` and
    ``|``, respectively.

    """

    AND = '$and'
    OR = '$or'

    def __init__(self, **fields):
        """Create a new filter.

        :param \*\*fields: Keyword arguments specifying the query.
        :type \*\*fields: \*\*kwargs.

        """

        self._filter = fields

    def _add_filter(self, filter, condition):
        """Add to filters together.

        :param filter: The filter to add.
        :type filter: :class:`Q`.
        :returns: :class:`Q` -- the new filter.
        :raises: :class:`TypeError`.

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
        """Add a new filter using ``$and``.

        :param filter: The filter to add.
        :type filter: :class:`Q`.
        :returns: :class:`Q` -- the new filter.

        """

        return self._add_filter(filter, self.AND)

    def __or__(self, filter):
        """Add a new filter using ``$or``.

        :param filter: The filter to add.
        :type filter: :class:`Q`.
        :returns: :class:`Q` -- the new filter.

        """

        return self._add_filter(filter, self.OR)


class QuerySet(object):

    """Wrapper around MongoDB cursors.

    The :class:`~simon.QuerySet` class wraps around
    :class:`~pymongo.cursor.Cursor`. When given a :class:`~simon.Model`
    class, it will return documents wrapped in an instances of the
    class. Otherwise documents will be returned as ``dict``.

    """

    def __init__(self, cursor=None, cls=None):
        """Create a new query set.

        :param cursor: The result set.
        :type cursor: :class:`~pymongo.cursor.Cursor`.
        :param cls: Model class to map the results to.
        :type cls: :class:`~simon.Model`.

        """

        self._cls = cls
        self._cursor = cursor
        self._count = None

        self._items = []

        self._sorting = None

    def count(self):
        """Return the number of documents in the :class:`QuerySet`.

        If no cursor has been associated with the query set,
        ``TypeError`` will be raised.

        :returns: int -- the number of documents.
        :raises: :class:`TypeError`.

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
        """Return distinct values for ``key`` in the :class:`QuerySet`.

        :param key: Name of the key.
        :type key: str.
        :returns: list -- distinct values for the key.

        """

        # If the QuerySet has a model class, check for key in the
        # class's field map
        if self._cls:
            query = map_fields(self._cls._meta.field_map, {key: 1},
                               flatten_keys=True, with_operators=True)
            key = get_next(iterkeys(query))()

        return self._cursor.distinct(key)

    def limit(self, limit):
        """Apply a limit to the documents in the :class:`QuerySet`.

        :param limit: Number of documents to return.
        :type limit: int.
        :returns: :class:`QuerySet` -- the documents with the limit
                  applied.

        """

        # Make sure to clone the cursor so as not to alter the original
        return QuerySet(self._cursor.clone().limit(limit), self._cls)

    def skip(self, skip):
        """Skip a number of documents in the :class:`QuerySet`.

        :param skip: Number of documents to skip.
        :type skip: int.
        :returns: :class:`QuerySet` -- the documents remaining.

        """

        # Make sure to clone the cursor so as not to alter the original
        return QuerySet(self._cursor.clone().skip(skip), self._cls)

    def sort(self, *fields):
        """Sort the documents in the :class:`QuerySet`.

        By default all sorting is done in ascending order. To switch
        any key to sort in descending order, place a ``-`` before the
        name of the key.

        ..

            >>> qs.sort('id')
            >>> qs.sort('grade', '-score')

        :param \*fields: Names of the fields to sort by.
        :type \*fields: \*args.
        :returns: :class:`QuerySet` -- the sorted documents.

        .. versionchanged:: 0.3.0
           Sorting doesn't occur until documents are loaded

        """

        # Build the list of sorting (field, direction) pairs. If the
        # QuerySet has a model class, check for the field in the class's
        # field map.
        sorting = []
        for field in fields:
            if field[0] == '-':
                field = field[1:]
                direction = pymongo.DESCENDING
            else:
                direction = pymongo.ASCENDING

            if self._cls:
                query = map_fields(self._cls._meta.field_map, {field: 1},
                                   flatten_keys=True, with_operators=True)
                field = get_next(iterkeys(query))()

            sorting.append((field, direction))

        # Make sure to clone the cursor so as not to alter the original
        qs = QuerySet(self._cursor.clone(), self._cls)

        # Add the sorting so that _fill_to() can apply it later.
        qs._sorting = sorting

        return qs

    def _fill_to(self, index):
        """Build the cache of documents retrieved from the cursor.

        :class:`QuerySet` objects keep documents loaded from the
        cursor in an internal cache to avoid repetitive trips to the
        database. In order to properly load a cache that can be
        iterated over, any documents before the requested one must be
        loaded.

        :param index: Index to fill the cache to.
        :type index: int.

        .. versionchanged:: 0.3.0
           Processes the sorting when documents are first fetched

        """

        # If all of the the requested documents have been loaded, get
        # out early.
        if index < len(self._items):
            return

        if self._sorting:
            # If there's a sort, apply it...
            self._cursor.sort(self._sorting)
            # and remove it so it won't happen again.
            self._sorting = None

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
        with ignored(StopIteration):
            # StopIteration should never happen because of the check at
            # the top of this method, but it's here just in case
            # something crazy happens, like a document is added to the
            # cursor (since new documents can suddenly appear in a
            # cursor) during iteration.
            for x in range(len(self._items), index + 1):
                item = get_next(self._cursor)()
                if self._cls:
                    item = self._cls(**item)
                self._items.append(item)

    def __getitem__(self, k):
        """Return an item or slice from the :class:`QuerySet`."""

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

        # bound will contain the number, not the index, of the last
        # requested document. Load all documents up to and including
        # the last bound now.
        self._fill_to(bound - 1)

        return self._items[k]

    def __iter__(self):
        """Iterate through the documents in the cursor."""

        for x in range(0, self.count()):
            # Fetch the document from the cursor if it hasn't already
            # been loaded
            if len(self._items) <= x:
                self._fill_to(x)

            yield self._items[x]

    def __len__(self):
        """Return the length of the :class:`QuerySet`."""

        try:
            return self.count()
        except TypeError:
            return 0
