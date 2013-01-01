try:
    import unittest2 as unittest
except ImportError:
    import unittest

import collections
import mock

from simon import query


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
