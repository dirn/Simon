try:
    import unittest2 as unittest
except ImportError:
    import unittest

import collections

from bson import ObjectId
import mock
from pymongo.cursor import Cursor

from simon import Model, connection, query

AN_OBJECT_ID_STR = '50d4dce70ea5fae6fb84e44b'
AN_OBJECT_ID = ObjectId(AN_OBJECT_ID_STR)


class TestModel1(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        field_map = {'id': '_id', 'fake': 'real'}


class TestQ(unittest.TestCase):
    """Test the `Q` class."""

    def test___init__(self):
        """Test the `__init__()` method."""

        q = query.Q(a=1)
        self.assertEqual(q._filter, {'a': 1})

        q = query.Q(a=1, b=2)
        self.assertEqual(q._filter, {'a': 1, 'b': 2})

    def test___and__(self):
        """Test the `__and__()` method."""

        q1 = query.Q(a=1)
        q1._add_filter = mock.Mock()

        q2 = query.Q(b=2)

        q1.__and__(q2)

        q1._add_filter.assert_called_with(q2, '$and')

    def test___or__(self):
        """Test the `__or__()` method."""

        q1 = query.Q(a=1)
        q1._add_filter = mock.Mock()

        q2 = query.Q(b=2)

        q1.__or__(q2)

        q1._add_filter.assert_called_with(q2, '$or')

    def test__add_filter(self):
        """Test the `_add_filter()` method."""

        q1 = query.Q(a=1)
        q2 = query.Q(a=1)

        expected = {'a': 1}
        actual = q1._add_filter(q2, query.Q.AND)._filter
        self.assertEqual(actual, expected)

        q1 = query.Q(a=1)
        q2 = query.Q(b=2)

        expected = {'$and': [{'a': 1}, {'b': 2}]}
        actual = q1._add_filter(q2, query.Q.AND)._filter
        self.assertEqual(actual, expected)

        expected = {'$or': [{'a': 1}, {'b': 2}]}
        actual = q1._add_filter(q2, query.Q.OR)._filter
        self.assertEqual(actual, expected)

    def test__add_filter_combine_conditions(self):
        """Test the `_add_filter()` method with different conditions."""

        q1 = query.Q(a=1)
        q2 = query.Q(b=2)
        q3 = query.Q(c=3)

        expected = {'$or': [{'$and': [{'a': 1}, {'b': 2}]}, {'c': 3}]}
        tmp = q1._add_filter(q2, query.Q.AND)
        actual = tmp._add_filter(q3, query.Q.OR)._filter
        self.assertEqual(actual, expected)

        expected = {'$and': [{'$or': [{'a': 1}, {'b': 2}]}, {'c': 3}]}
        tmp = q1._add_filter(q2, query.Q.OR)
        actual = tmp._add_filter(q3, query.Q.AND)._filter
        self.assertEqual(actual, expected)

    def test__add_filter_filter_doesnt_exist(self):
        """Test the `_add_filter()` method with a new filter."""

        q1 = query.Q(a=1)
        q2 = query.Q(b=2)
        q3 = query.Q(c=3)

        expected = {'$and': [{'a': 1}, {'b': 2}, {'c': 3}]}
        tmp = q1._add_filter(q2, query.Q.AND)
        actual = tmp._add_filter(q3, query.Q.AND)._filter
        self.assertEqual(actual, expected)

        expected = {'$or': [{'a': 1}, {'b': 2}, {'c': 3}]}
        tmp = q1._add_filter(q2, query.Q.OR)
        actual = tmp._add_filter(q3, query.Q.OR)._filter
        self.assertEqual(actual, expected)

    def test__add_filter_filter_exists(self):
        """Test the `_add_filter()` method with a filter that exists."""

        q1 = query.Q(a=1)
        q2 = query.Q(b=2)

        expected = {'$and': [{'a': 1}, {'b': 2}]}
        tmp = q1._add_filter(q2, query.Q.AND)
        actual = tmp._add_filter(q2, query.Q.AND)._filter
        self.assertEqual(actual, expected)

        expected = {'$or': [{'a': 1}, {'b': 2}]}
        tmp = q1._add_filter(q2, query.Q.OR)
        actual = tmp._add_filter(q2, query.Q.OR)._filter
        self.assertEqual(actual, expected)

    def test__add_filter_typeerror(self):
        """Test that `_add_filter()` raises `TypeError`."""

        q = query.Q(a=1)
        with self.assertRaises(TypeError):
            q._add_filter(1, query.Q.AND)


class TestQuerySet(unittest.TestCase):
    """Test :class:`~simon.query.QuerySet` functionality"""

    @classmethod
    def setUpClass(cls):
        cls.connection = connection.connect('localhost', name='test-simon')

    def test_count(self):
        """Test the `count()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)
        qs.count()

        cursor.count.assert_called_with(with_limit_and_skip=True)

        # cursor.count() should get cached as qs._count, so it should
        # only be called once by qs.count()
        qs.count()
        cursor.count.assert_not_called()

    def test_count_typeerror(self):
        """Test that `count()` raises `TypeError`."""

        qs = query.QuerySet()
        with self.assertRaises(TypeError):
            qs.count()

    def test_distinct(self):
        """Test the `distinct()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)
        qs.distinct('a')

        cursor.distinct.assert_called_with('a')

    def test_distinct_field_map(self):
        """Test the `distinct()` method with a name in `field_map`."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)
        qs.distinct('fake')

        cursor.distinct.assert_called_with('real')

    def test_distinct_nested_field(self):
        """Test the `distinct()` method with a nested field."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)
        qs.distinct('a__b')

        cursor.distinct.assert_called_with('a.b')

    def test_limit(self):
        """Test the `limit()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.limit(1)
        cursor.clone.assert_called_with()
        cursor.clone().limit.assert_called_with(1)

        qs.limit(2)
        cursor.clone.assert_called_with()
        cursor.clone().limit.assert_called_with(2)

    def test_skip(self):
        """Test the `skip()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.skip(1)
        cursor.clone.assert_called_with()
        cursor.clone().skip.assert_called_with(1)

        qs.skip(2)
        cursor.clone.assert_called_with()
        cursor.clone().skip.assert_called_with(2)

    def test_sort(self):
        """Test the `sort()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('_id')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('_id', 1)])

        qs.sort('-_id')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('_id', -1)])

    def test_sort_field_map(self):
        """Test the `sort()` method with a name in `field_map`."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)

        qs.sort('fake')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('real', 1)])

    def test_sort_multiple_ascending(self):
        """Test the `sort()` method for multiple ascending keys."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('a', 'b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', 1), ('b', 1)])

    def test_sort_multiple_descending(self):
        """Test the `sort()` method for multiple descending keys."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('-a', '-b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', -1), ('b', -1)])

    def test_sort_multiple_ascending_then_descending(self):
        """Test the `sort()` method for multiple keys ascending first."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('a', '-b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', 1), ('b', -1)])

    def test_sort_multiple_descending_then_ascending(self):
        """Test the `sort()` method for multiple keys descending first."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('-a', 'b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', -1), ('b', 1)])

    def test_sort_nested_field(self):
        """Test the `sort()` method with a nested field."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)

        qs.sort('a__b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a.b', 1)])

    def test__fill_to(self):
        """Test the `_fill_to()` method."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(2)

        self.assertEqual(len(qs._items), 3)

    def test__fill_to_as_documents(self):
        """Test that `_fill_to()` stores documents."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.next.return_value = {'_id': AN_OBJECT_ID}

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(0)

        self.assertTrue(isinstance(qs._items[0], dict))

    def test__fill_to_as_model(self):
        """Test that `_fill_to()` stores model instances."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.next.return_value = {'_id': AN_OBJECT_ID}

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)

        qs._fill_to(0)

        self.assertTrue(isinstance(qs._items[0], TestModel1))

    def test__fill_to_indexes(self):
        ("Test that `_fill_to()` property fills to the specified "
         "index.")

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        for x in range(3):
            qs._fill_to(x)
            self.assertEqual(len(qs._items), x + 1)

    def test__fill_to_overfill(self):
        ("Test that `_fill_to()` correctly handles indexes greater than"
         " the maximum index of the result cache.")

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(3)

        self.assertEqual(len(qs._items), 3)

    def test__fill_to_twice(self):
        """Test that `_fill_to()` can be called multiple times."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(0)
        self.assertEqual(len(qs._items), 1)

        qs._fill_to(0)
        self.assertEqual(len(qs._items), 1)

        qs._fill_to(3)
        self.assertEqual(len(qs._items), 3)

        qs._fill_to(3)
        self.assertEqual(len(qs._items), 3)

    def test___getitem__(self):
        """Test the `__getitem__()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        # qs._fill_to() would normally populate qs._items
        qs._items = range(3)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            for x in range(3):
                self.assertEqual(qs[x], qs._items[x])
                _fill_to.assert_called_with(x)

    def test___getitem___slice(self):
        """Test the `__getitem__()` method with slices."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        # qs._fill_to() would normally populate qs._items
        qs._items = range(3)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            self.assertEqual(qs[1:], qs._items[1:])
            _fill_to.assert_called_with(2)

            self.assertEqual(qs[:1], qs._items[:1])
            _fill_to.assert_called_with(0)

            self.assertEqual(qs[1:2], qs._items[1:2])
            _fill_to.assert_called_with(1)

            self.assertEqual(qs[::2], qs._items[::2])
            _fill_to.assert_called_with(2)

            self.assertEqual(qs[1::2], qs._items[1::2])
            _fill_to.assert_called_with(2)

            self.assertEqual(qs[::], qs._items[::])
            _fill_to.assert_called_with(2)

    def test___getitem___indexerror(self):
        """Test that `__getitem__()` raises `IndexError`."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)

        with self.assertRaises(IndexError) as e:
            qs[3]

        expected = "No such item in 'QuerySet' for 'TestModel1' object"
        self.assertEqual(e.exception.message, expected)

        qs = query.QuerySet(cursor=cursor)

        with self.assertRaises(IndexError) as e:
            qs[3]

        expected = "No such item in 'QuerySet'"
        self.assertEqual(e.exception.message, expected)

    def test___getitem___typeerror(self):
        """Test that `__getitem__()` raises `TypeError`."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        with self.assertRaises(TypeError):
            qs[-1]

    def test___iter__(self):
        """Test the `__iter__()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        self.assertTrue(isinstance(qs.__iter__(), collections.Iterable))

    def test___iter___fills_cache(self):
        """Test that `__iter__()` fills the result cache."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        def append_to_cache(v):
            qs._items.append(v)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            _fill_to.side_effect = append_to_cache

            i = 0
            for x in qs:
                _fill_to.assert_called_with(i)
                i += 1

        self.assertEqual(len(qs._items), 3)

    def test__iter___fills_cache_partial(self):
        """Test that `__iter__()` fills the rest of the result cache."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)
        qs._items = [0]

        def append_to_cache(v):
            qs._items.append(v)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            _fill_to.side_effect = append_to_cache

            i = 0
            for x in qs:
                if i == 0:
                    # qs._fill_to(0) will already have been called
                    _fill_to.assert_not_called()
                else:
                    _fill_to.assert_called_with(i)
                i += 1

        self.assertEqual(len(qs._items), 3)

    def test___len__(self):
        """Test the `__len__()` method."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        self.assertEqual(len(qs), cursor.count())
