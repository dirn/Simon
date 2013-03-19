try:
    import unittest2 as unittest
except ImportError:
    import unittest

import collections

import mock
from pymongo.cursor import Cursor

from simon import connection, query

from .utils import AN_OBJECT_ID, ModelFactory

DefaultModel = ModelFactory('DefaultModel')
MappedModel = ModelFactory('MappedModel', field_map={'fake': 'real'})


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
        with mock.patch('simon.connection.MongoClient'):
            cls.connection = connection.connect('localhost', name='test-simon')

    def setUp(cls):
        cls.cursor = mock.MagicMock(spec=Cursor)
        cls.qs = query.QuerySet(cursor=cls.cursor)
        cls.model_qs = query.QuerySet(cursor=cls.cursor, cls=DefaultModel)

    def test_count(self):
        """Test the `count()` method."""

        self.qs.count()

        self.cursor.count.assert_called_with(with_limit_and_skip=True)

        # cursor.count() should get cached as qs._count, so it should
        # only be called once by qs.count()
        self.qs.count()
        self.cursor.count.assert_not_called()

    def test_count_typeerror(self):
        """Test that `count()` raises `TypeError`."""

        qs = query.QuerySet()
        with self.assertRaises(TypeError):
            qs.count()

    def test_distinct(self):
        """Test the `distinct()` method."""

        self.qs.distinct('a')

        self.cursor.distinct.assert_called_with('a')

    def test_distinct_field_map(self):
        """Test the `distinct()` method with a name in `field_map`."""

        self.model_qs._cls = MappedModel

        self.model_qs.distinct('fake')

        self.cursor.distinct.assert_called_with('real')

    def test_distinct_nested_field(self):
        """Test the `distinct()` method with a nested field."""

        self.model_qs.distinct('a__b')

        self.cursor.distinct.assert_called_with('a.b')

    def test_limit(self):
        """Test the `limit()` method."""

        self.qs.limit(1)
        self.cursor.clone.assert_called_with()
        self.cursor.clone().limit.assert_called_with(1)

        self.qs.limit(2)
        self.cursor.clone.assert_called_with()
        self.cursor.clone().limit.assert_called_with(2)

    def test_skip(self):
        """Test the `skip()` method."""

        self.qs.skip(1)
        self.cursor.clone.assert_called_with()
        self.cursor.clone().skip.assert_called_with(1)

        self.qs.skip(2)
        self.cursor.clone.assert_called_with()
        self.cursor.clone().skip.assert_called_with(2)

    def test_sort(self):
        """Test the `sort()` method."""

        qs = self.qs.sort('_id')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('_id', 1)])
        qs._cursor.sort.assert_not_called()

        qs = self.qs.sort('-_id')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('_id', -1)])
        qs._cursor.sort.assert_not_called()

    def test_sort_field_map(self):
        """Test the `sort()` method with a name in `field_map`."""

        self.model_qs._cls = MappedModel

        qs = self.model_qs.sort('fake')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('real', 1)])
        qs._cursor.sort.assert_not_called()

    def test_sort_multiple_ascending(self):
        """Test the `sort()` method for multiple ascending keys."""

        qs = self.qs.sort('a', 'b')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('a', 1), ('b', 1)])
        qs._cursor.sort.assert_not_called()

    def test_sort_multiple_descending(self):
        """Test the `sort()` method for multiple descending keys."""

        qs = self.qs.sort('-a', '-b')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('a', -1), ('b', -1)])
        qs._cursor.sort.assert_not_called()

    def test_sort_multiple_ascending_then_descending(self):
        """Test the `sort()` method for multiple keys ascending first."""

        qs = self.qs.sort('a', '-b')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('a', 1), ('b', -1)])
        qs._cursor.sort.assert_not_called()

    def test_sort_multiple_descending_then_ascending(self):
        """Test the `sort()` method for multiple keys descending first."""

        qs = self.qs.sort('-a', 'b')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('a', -1), ('b', 1)])
        qs._cursor.sort.assert_not_called()

    def test_sort_nested_field(self):
        """Test the `sort()` method with a nested field."""

        qs = self.model_qs.sort('a__b')
        self.cursor.clone.assert_called_with()
        self.assertEqual(qs._sorting, [('a.b', 1)])
        qs._cursor.sort.assert_not_called()

    def test__fill_to(self):
        """Test the `_fill_to()` method."""

        self.cursor.count.return_value = 3

        self.qs._fill_to(2)

        self.assertEqual(len(self.qs._items), 3)

    def test__fill_to_as_documents(self):
        """Test that `_fill_to()` stores documents."""

        self.cursor.next.return_value = {'_id': AN_OBJECT_ID}

        self.qs._fill_to(0)

        self.assertIsInstance(self.qs._items[0], dict)

    def test__fill_to_as_model(self):
        """Test that `_fill_to()` stores model instances."""

        self.cursor.next.return_value = {'_id': AN_OBJECT_ID}

        self.model_qs._fill_to(0)

        self.assertIsInstance(self.model_qs._items[0], self.model_qs._cls)

    def test__fill_to_indexes(self):
        ("Test that `_fill_to()` property fills to the specified "
         "index.")

        self.cursor.count.return_value = 3

        for x in range(3):
            self.qs._fill_to(x)
            self.assertEqual(len(self.qs._items), x + 1)

    def test__fill_to_overfill(self):
        ("Test that `_fill_to()` correctly handles indexes greater than"
         " the maximum index of the result cache.")

        self.cursor.count.return_value = 3

        self.qs._fill_to(3)

        self.assertEqual(len(self.qs._items), 3)

    def test__fill_to_sort(self):
        """Test that `_fill_to()` correctly handles sorting."""

        self.cursor.count.return_value = 3

        self.qs._sorting = [('a', 1)]

        self.qs._fill_to(0)

        self.cursor.sort.assert_called_with([('a', 1)])
        self.assertIsNone(self.qs._sorting)

    def test__fill_to_twice(self):
        """Test that `_fill_to()` can be called multiple times."""

        self.cursor.count.return_value = 3

        self.qs._fill_to(0)
        self.assertEqual(len(self.qs._items), 1)

        self.qs._fill_to(0)
        self.assertEqual(len(self.qs._items), 1)

        self.qs._fill_to(3)
        self.assertEqual(len(self.qs._items), 3)

        self.qs._fill_to(3)
        self.assertEqual(len(self.qs._items), 3)

    def test___getitem__(self):
        """Test the `__getitem__()` method."""

        self.cursor.count.return_value = 3

        # qs._fill_to() would normally populate qs._items
        self.qs._items = range(3)

        with mock.patch.object(self.qs, '_fill_to') as _fill_to:
            for x in range(3):
                self.assertEqual(self.qs[x], self.qs._items[x])
                _fill_to.assert_called_with(x)

    def test___getitem___slice(self):
        """Test the `__getitem__()` method with slices."""

        self.cursor.count.return_value = 3

        # qs._fill_to() would normally populate qs._items
        self.qs._items = range(3)

        with mock.patch.object(self.qs, '_fill_to') as _fill_to:
            self.assertEqual(self.qs[1:], self.qs._items[1:])
            _fill_to.assert_called_with(2)

            self.assertEqual(self.qs[:1], self.qs._items[:1])
            _fill_to.assert_called_with(0)

            self.assertEqual(self.qs[1:2], self.qs._items[1:2])
            _fill_to.assert_called_with(1)

            self.assertEqual(self.qs[::2], self.qs._items[::2])
            _fill_to.assert_called_with(2)

            self.assertEqual(self.qs[1::2], self.qs._items[1::2])
            _fill_to.assert_called_with(2)

            self.assertEqual(self.qs[::], self.qs._items[::])
            _fill_to.assert_called_with(2)

    def test___getitem___indexerror(self):
        """Test that `__getitem__()` raises `IndexError`."""

        self.cursor.count.return_value = 3

        with self.assertRaises(IndexError) as e:
            self.model_qs[3]

        expected = "No such item in 'QuerySet' for 'DefaultModel' object"
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        with self.assertRaises(IndexError) as e:
            self.qs[3]

        expected = "No such item in 'QuerySet'"
        actual = str(e.exception)
        self.assertEqual(actual, expected)

    def test___getitem___typeerror(self):
        """Test that `__getitem__()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            self.qs[-1]

    def test___iter__(self):
        """Test the `__iter__()` method."""

        self.assertIsInstance(self.qs.__iter__(), collections.Iterable)

    def test___iter___fills_cache(self):
        """Test that `__iter__()` fills the result cache."""

        self.cursor.count.return_value = 3

        def append_to_cache(v):
            self.qs._items.append(v)

        with mock.patch.object(self.qs, '_fill_to') as _fill_to:
            _fill_to.side_effect = append_to_cache

            i = 0
            for x in self.qs:
                _fill_to.assert_called_with(i)
                i += 1

        self.assertEqual(len(self.qs._items), 3)

    def test__iter___fills_cache_partial(self):
        """Test that `__iter__()` fills the rest of the result cache."""

        self.cursor.count.return_value = 3

        self.qs._items = [0]

        def append_to_cache(v):
            self.qs._items.append(v)

        with mock.patch.object(self.qs, '_fill_to') as _fill_to:
            _fill_to.side_effect = append_to_cache

            i = 0
            for x in self.qs:
                if i == 0:
                    # qs._fill_to(0) will already have been called
                    _fill_to.assert_not_called()
                else:
                    _fill_to.assert_called_with(i)
                i += 1

        self.assertEqual(len(self.qs._items), 3)

    def test___len__(self):
        """Test the `__len__()` method."""

        self.cursor.count.return_value = 3

        self.assertEqual(len(self.qs), self.cursor.count())
