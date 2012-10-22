try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import MongoModel
from simon.utils import map_fields, parse_kwargs


class TestModel(MongoModel):
    class Meta:
        collection = 'test'
        field_map = {'b': 'c', 'd.e': 'f.e', 'g.h': 'i.j', 'x': 'z.x', 'y': 'z.y'}


class TestUtils(unittest.TestCase):
    def test_map_fields(self):
        """Test the `map_fields()` method."""

        expected = {'a': 1}
        actual = map_fields(TestModel, {'a': 1})
        self.assertEqual(actual, expected)

        expected = {'c': 1}
        actual = map_fields(TestModel, {'b': 1})
        self.assertEqual(actual, expected)

        expected = {'a': 1, 'c': 2}
        actual = map_fields(TestModel, {'a': 1, 'b': 2})
        self.assertEqual(actual, expected)

        expected = {'f': {'e': 1}}
        actual = map_fields(TestModel, {'d.e': 1})
        self.assertEqual(actual, expected)

        expected = {'i': {'j': 1}}
        actual = map_fields(TestModel, {'g.h': 1})
        self.assertEqual(actual, expected)

        expected = {'z': {'x': 1, 'y': 2}}
        actual = map_fields(TestModel, {'x': 1, 'y': 2})
        self.assertEqual(actual, expected)

    def test_map_fields_second_pass(self):
        """Test the `map_fields()` method with a second pass."""

        expected = {'a': {'b': 1}}
        actual = map_fields(TestModel, {'a__b': 1})
        self.assertEqual(actual, expected)

        expected = {'c': {'d': 1}}
        actual = map_fields(TestModel, {'b__d': 1})
        self.assertEqual(actual, expected)

        expected = {'f': {'e': 1}}
        actual = map_fields(TestModel, {'d__e': 1})
        self.assertEqual(actual, expected)

        expected = {'i': {'j': 1}}
        actual = map_fields(TestModel, {'g__h': 1})
        self.assertEqual(actual, expected)

        expected = {'z': {'x': 1, 'y': 2}}
        actual = map_fields(TestModel, {'z__x': 1, 'z__y': 2})
        self.assertEqual(actual, expected)

    def test_parse_kwargs(self):
        """Test the `parse_kwargs()` method."""

        expected = {'a': 1}
        actual = parse_kwargs(a=1)
        self.assertEqual(actual, expected)

        expected = {'a': 1, 'b': 2}
        actual = parse_kwargs(a=1, b=2)
        self.assertEqual(actual, expected)

        expected = {'a': {'b': 1, 'c': 2}}
        actual = parse_kwargs(a__b=1, a__c=2)
        self.assertEqual(actual, expected)

        expected = {'a': {'b': {'c': 1, 'd': 2}}}
        actual = parse_kwargs(a__b__c=1, a__b__d=2)
        self.assertEqual(actual, expected)

        expected = {'a': {'b': 1, 'c': {'d': 2}}}
        actual = parse_kwargs(a__b=1, a__c__d=2)
        self.assertEqual(actual, expected)

        expected = {'__a': 1}
        actual = parse_kwargs(__a=1)
        self.assertEqual(actual, expected)

        expected = {'a__': 1}
        actual = parse_kwargs(a__=1)
        self.assertEqual(actual, expected)

        expected = {'__a__': 1}
        actual = parse_kwargs(__a__=1)
        self.assertEqual(actual, expected)

        expected = {'__a': 1, 'a__': 2, '__a__': 3}
        actual = parse_kwargs(__a=1, a__=2, __a__=3)
        self.assertEqual(actual, expected)
