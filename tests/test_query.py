try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from simon import query


class TestHelpers(unittest.TestCase):
    """Test the helper methods in the `query` module."""

    def test_box(self):
        """Test the `box()` method."""

        with mock.patch('simon.query.within') as within:
            query.box([1, 2], [3, 4])
            within.assert_called_with('box', [1, 2], [3, 4])

        expected = {'$within': {'$box': [[1, 2], [3, 4]]}}
        actual = query.box([1, 2], [3, 4])
        self.assertEqual(actual, expected)

    def test_box_typeerror(self):
        """Test that `box()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            query.box(1, [2, 3])

        with self.assertRaises(TypeError):
            query.box([1, 2], 3)

    def test_box_valueerror(self):
        """Test that `box()` raises `ValueError`."""

        with self.assertRaises(ValueError):
            query.box([1, 2, 3], [4, 5])

        with self.assertRaises(ValueError):
            query.box([1, 2], [3, 4, 5])

    def test_circle(self):
        """Test the `circle()` method."""

        with mock.patch('simon.query.within') as within:
            query.circle([1, 2], 3)
            within.assert_called_with('circle', [1, 2], 3)

        expected = {'$within': {'$circle': [[1, 2], 3]}}
        actual = query.circle([1, 2], 3)
        self.assertEqual(actual, expected)

    def test_circle_typeerror(self):
        """Test that `circle()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            query.circle(1, 2)

    def test_circle_valueerror(self):
        """Test that `circle()` raises `ValueError`."""

        with self.assertRaises(ValueError):
            query.circle([1, 2, 3], 4)

    def test_near(self):
        """Test the `near()` method."""

        expected = {'$near': [1, 2]}
        actual = query.near([1, 2])
        self.assertEqual(actual, expected)

        expected = {'$near': [1, 2], '$maxDistance': 3}
        actual = query.near([1, 2], max_distance=3)
        self.assertEqual(actual, expected)

        expected = {'$near': [1, 2], '$uniqueDocs': True}
        actual = query.near([1, 2], unique_docs=True)
        self.assertEqual(actual, expected)

        expected = {'$near': [1, 2], '$maxDistance': 3, '$uniqueDocs': True}
        actual = query.near([1, 2], max_distance=3, unique_docs=True)
        self.assertEqual(actual, expected)

        # near() supports point as a tuple
        expected = {'$near': (1, 2)}
        actual = query.near((1, 2))
        self.assertEqual(actual, expected)

    def test_near_typeerror(self):
        """Test that `near()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            query.near(1)

        with self.assertRaises(TypeError):
            query.near([1])

        with self.assertRaises(TypeError):
            query.near([1, 2, 3])

    def test_polygon(self):
        """Test the `polygon()` method."""

        with mock.patch('simon.query.within') as within:
            query.polygon([1, 2], [3, 4], [5, 6])
            within.assert_called_with('polygon', [1, 2], [3, 4], [5, 6])

            query.polygon({'a': {'x': 1, 'y': 2}, 'b': {'x': 3, 'y': 4}})
            within.assert_called_with('polygon', a={'x': 1, 'y': 2},
                                      b={'x': 3, 'y': 4})

        expected = {'$within': {'$polygon': [[1, 2], [3, 4], [5, 6]]}}
        actual = query.polygon([1, 2], [3, 4], [5, 6])
        self.assertEqual(actual, expected)

        expected = {'$within': {'$polygon': {'a': {'x': 1, 'y': 2},
                                             'b': {'x': 3, 'y': 4}}}}
        actual = query.polygon({'a': {'x': 1, 'y': 2}, 'b': {'x': 3, 'y': 4}})
        self.assertEqual(actual, expected)

    def test_polygon_mappings_typeerror(self):
        """Test that `polygon()` raises `TypeError` with mappings."""

        with self.assertRaises(TypeError):
            query.polygon({'a': {'b': 1, 'c': 2}, 'd': 3})

    def test_polygon_mappings_valueerror(self):
        """Test that `polygon()` raises `ValueError` with mappings."""

        with self.assertRaises(ValueError):
            query.polygon({'a': 1})

        with self.assertRaises(ValueError):
            query.polygon({'a': {'b': 1}})

        with self.assertRaises(ValueError):
            query.polygon({'a': {'b': 1, 'c': 2}})

        with self.assertRaises(ValueError):
            query.polygon({'a': {'b': 1, 'c': 2}, 'd': {'e': 3}})

    def test_polygon_typeerror(self):
        """Test that `polygon()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            query.polygon()

        with self.assertRaises(TypeError):
            query.polygon(1)

        with self.assertRaises(TypeError):
            query.polygon([1])

        with self.assertRaises(TypeError):
            query.polygon([1, 2, 3])

        with self.assertRaises(TypeError):
            query.polygon([1, 2], 3)

    def test_polygon_valueerror(self):
        """Test that `polygon()` raises `ValueError`."""

        with self.assertRaises(ValueError):
            query.polygon([1, 2], [3])

        with self.assertRaises(ValueError):
            query.polygon([1, 2], [3, 4, 5])

    def test_within(self):
        """Test the `within()` method."""

        expected = {'$within': {'$box': [[1, 2], [3, 4]]}}
        actual = query.within('box', [1, 2], [3, 4])
        self.assertEqual(actual, expected)

        expected = {'$within': {'$polygon': {'a': {'x': 1, 'y': 2},
                                             'b': {'x': 3, 'y': 4}}}}
        actual = query.within('polygon', a={'x': 1, 'y': 2},
                              b={'x': 3, 'y': 4})
        self.assertEqual(actual, expected)

    def test_within_runtimeerror(self):
        """Test that `within()` raises `RuntimeError`."""

        with self.assertRaises(RuntimeError):
            query.within('shape', *[1, 2], **{'a': {'x': 3, 'y': 4}})


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
