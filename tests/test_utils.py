try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import MongoModel
from simon.utils import (get_nested_key, map_fields, parse_kwargs,
                         remove_nested_key, update_nested_keys)


class TestModel(MongoModel):
    class Meta:
        collection = 'test'
        field_map = {'b': 'c', 'd.e': 'f.e', 'g.h': 'i.j', 'x': 'z.x',
                     'y': 'z.y'}


class TestUtils(unittest.TestCase):
    def test_get_nested_key(self):
        """Test the `get_nested_key()` method."""

        expected = 1
        actual = get_nested_key({'a': 1}, 'a')
        self.assertEqual(actual, expected)

        expected = 1
        actual = get_nested_key({'a': 1, 'b': 2}, 'a')
        self.assertEqual(actual, expected)

        expected = 1
        actual = get_nested_key({'a': {'b': 1}}, 'a.b')
        self.assertEqual(actual, expected)

        expected = 1
        actual = get_nested_key({'a': {'b': 1, 'c': 2}, 'd': 3}, 'a.b')
        self.assertEqual(actual, expected)

        expected = 1
        actual = get_nested_key({'a': {'b': {'c': {'d': 1}}}}, 'a.b.c.d')
        self.assertEqual(actual, expected)

    def test_get_nested_key_keyerror(self):
        """Test that `get_nested_key()` raises `KeyError`."""

        with self.assertRaises(KeyError):
            get_nested_key({}, 'a')

        with self.assertRaises(KeyError):
            get_nested_key({'a': 1}, 'b')

        with self.assertRaises(KeyError):
            get_nested_key({'a': {'b': 1}}, 'c')

        with self.assertRaises(KeyError):
            get_nested_key({'a': {'b': 1}}, 'a.c')

        # And once more with a key that's more nested than the dict
        with self.assertRaises(KeyError):
            get_nested_key({'a': {'b': 1}}, 'a.b.c')

    def test_map_fields(self):
        """Test the `map_fields()` method."""

        expected = {'a': 1}
        actual = map_fields(TestModel, {'a': 1})
        self.assertEqual(actual, expected)

        expected = {'__a': 1}
        actual = map_fields(TestModel, {'__a': 1})
        self.assertEqual(actual, expected)

        expected = {'a__': 1}
        actual = map_fields(TestModel, {'a__': 1})
        self.assertEqual(actual, expected)

        expected = {'__a__': 1}
        actual = map_fields(TestModel, {'__a__': 1})
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

    def test_map_fields_comparison_operators(self):
        """Test the `map_fields()` method with `with_comparisons` set."""

        expected = {'a': 1}
        actual = map_fields(TestModel, {'a': 1}, with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$gt': 1}}
        actual = map_fields(TestModel, {'a__gt': 1}, with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$gte': 1}}
        actual = map_fields(TestModel, {'a__gte': 1}, with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$lt': 1}}
        actual = map_fields(TestModel, {'a__lt': 1}, with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$lte': 1}}
        actual = map_fields(TestModel, {'a__lte': 1}, with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$ne': 1}}
        actual = map_fields(TestModel, {'a__ne': 1}, with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$in': [1, 2]}}
        actual = map_fields(TestModel, {'a__in': [1, 2]},
                            with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$nin': [1, 2]}}
        actual = map_fields(TestModel, {'a__nin': [1, 2]},
                            with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$exists': True}}
        actual = map_fields(TestModel, {'a__exists': True},
                            with_comparisons=True)
        self.assertEqual(actual, expected)

    def test_map_fields_flattened_keys(self):
        """Test the `map_fields()` method with `flatten_keys` set."""

        expected = {'a': 1}
        actual = map_fields(TestModel, {'a': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'__a': 1}
        actual = map_fields(TestModel, {'__a': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a__': 1}
        actual = map_fields(TestModel, {'a__': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'__a__': 1}
        actual = map_fields(TestModel, {'__a__': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a': 1, 'c': 2}
        actual = map_fields(TestModel, {'a': 1, 'b': 2}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a.b': 1}
        actual = map_fields(TestModel, {'a__b': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a.b.c': 1}
        actual = map_fields(TestModel, {'a__b__c': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a.b': 1, 'a.c': 2}
        actual = map_fields(TestModel, {'a__b': 1, 'a__c': 2},
                            flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'f.e': 1}
        actual = map_fields(TestModel, {'d__e': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        # Test some with with_comparisons set, too
        expected = {'a.b.c': {'$gt': 1}}
        actual = map_fields(TestModel, {'a__b__c__gt': 1}, flatten_keys=True,
                            with_comparisons=True)
        self.assertEqual(actual, expected)

        expected = {'f.e': {'$lt': 1}}
        actual = map_fields(TestModel, {'d__e__lt': 1}, flatten_keys=True,
                            with_comparisons=True)
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

    def test_remove_nested_key(self):
        """Test the `remove_nested_key()` method."""

        original = {'a': 1}
        expected = {}
        actual = remove_nested_key(original, 'a')
        self.assertEqual(actual, expected)

        original = {'a': 1, 'b': 2}
        expected = {'b': 2}
        actual = remove_nested_key(original, 'a')
        self.assertEqual(actual, expected)

        original = {'a': 1, 'b': {'c': 2, 'd': 3}}
        expected = {'a': 1}
        actual = remove_nested_key(original, 'b')
        self.assertEqual(actual, expected)

        original = {'a': 1, 'b': {'c': 2, 'd': 3}}
        expected = {'a': 1, 'b': {'c': 2}}
        actual = remove_nested_key(original, 'b.d')
        self.assertEqual(actual, expected)

        original = {'a': 1, 'b': {'c': 2, 'd': 3}}
        expected = {'a': 1, 'b': {}}
        actual = remove_nested_key(original, 'b.c')
        actual = remove_nested_key(original, 'b.d')
        self.assertEqual(actual, expected)

    def test_remove_nested_key_keyerror(self):
        """Test that `remove_nested_key()` raises `KeyError`."""

        original = {'a': 1}
        with self.assertRaises(KeyError):
            remove_nested_key(original, 'b')

        original = {'a': 1, 'b': {'c': 2}}
        with self.assertRaises(KeyError):
            remove_nested_key(original, 'b.d')

    def test_remove_nested_key_typeerror(self):
        """Test that `remove_nested_key()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            remove_nested_key(1, 'a')

    def test_update_nested_keys(self):
        """Test the `update_nested_values()` method."""

        original = {'a': 1}
        changes = {'a': 2}
        expected = {'a': 2}
        actual = update_nested_keys(original, changes)
        self.assertEqual(actual, expected)

        original = {'a': 1}
        changes = {'b': 2}
        expected = {'a': 1, 'b': 2}
        actual = update_nested_keys(original, changes)
        self.assertEqual(actual, expected)

        original = {'a': {'b': 1}}
        changes = {'a': {'b': 2}}
        expected = {'a': {'b': 2}}
        actual = update_nested_keys(original, changes)
        self.assertEqual(actual, expected)

        original = {'a': 1, 'b': 2}
        changes = {'a': 3}
        expected = {'a': 3, 'b': 2}
        actual = update_nested_keys(original, changes)
        self.assertEqual(actual, expected)

        original = {'a': {'b': 1, 'c': 2}}
        changes = {'a': {'c': 3}}
        expected = {'a': {'b': 1, 'c': 3}}
        actual = update_nested_keys(original, changes)
        self.assertEqual(actual, expected)

        original = {'a': {'b': 1, 'c': {'d': {'e': {'f': 2}}}}}
        changes = {'a': {'c': {'d': {'e': 3, 'g': 4}}, 'h': 5}, 'i': 6}
        expected = {'a': {'b': 1, 'c': {'d': {'e': 3, 'g': 4}}, 'h': 5},
                    'i': 6}
        actual = update_nested_keys(original, changes)
        self.assertEqual(actual, expected)

    def test_update_nested_keys_typeerror(self):
        """Test that `update_nested_keys()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            update_nested_keys(1, {'a': 1})

        with self.assertRaises(TypeError):
            update_nested_keys({'a': 1}, 1)

        with self.assertRaises(TypeError):
            update_nested_keys(1, 1)
