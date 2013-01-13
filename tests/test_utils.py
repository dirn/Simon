try:
    import unittest2 as unittest
except ImportError:
    import unittest

from bson import ObjectId
from bson.errors import InvalidId

from simon import Model
from simon.utils import (get_nested_key, guarantee_object_id, map_fields,
                         parse_kwargs, remove_nested_key, update_nested_keys)


AN_OBJECT_ID_STR = '50d4dce70ea5fae6fb84e44b'
AN_OBJECT_ID = ObjectId(AN_OBJECT_ID_STR)


class TestModel(Model):
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

    def test_guarantee_object_id(self):
        """Test the `guarantee_object_id()` method."""

        expected = AN_OBJECT_ID
        actual = guarantee_object_id(AN_OBJECT_ID_STR)

        self.assertEqual(actual, expected)

    def test_guarantee_object_id_dict(self):
        """Test the `guarantee_object_id()` method with `dict`."""

        expected = {
            '$gt': AN_OBJECT_ID,
            '$lt': AN_OBJECT_ID,
        }
        actual = guarantee_object_id({
            '$gt': AN_OBJECT_ID_STR,
            '$lt': AN_OBJECT_ID,
        })
        self.assertEqual(actual, expected)

    def test_guarantee_object_id_invalidid(self):
        """Test that `guarantee_object_id()` raises `InvalidId`."""

        with self.assertRaises(InvalidId):
            guarantee_object_id('abc')

    def test_guarantee_object_id_list(self):
        """Test the `guarantee_object_id()` method with a `list`."""

        expected = {
            '$in': [AN_OBJECT_ID, AN_OBJECT_ID],
        }
        actual = guarantee_object_id({
            '$in': [AN_OBJECT_ID_STR, AN_OBJECT_ID],
        })
        self.assertEqual(actual, expected)

    def test_guarantee_object_id_object_id(self):
        """Test the `guarantee_object_id()` method with `ObjectId`."""

        expected = AN_OBJECT_ID
        actual = guarantee_object_id(AN_OBJECT_ID)
        self.assertEqual(actual, expected)

    def test_guarantee_object_id_tuple(self):
        """Test the `guarantee_object_id()` method with a `tuple`."""

        expected = {
            '$in': [AN_OBJECT_ID, AN_OBJECT_ID],
        }
        actual = guarantee_object_id({
            '$in': (AN_OBJECT_ID_STR, AN_OBJECT_ID),
        })
        self.assertEqual(actual, expected)

    def test_guarantee_object_id_typeerror(self):
        """Test that `guarantee_object_id()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            guarantee_object_id(1)

        with self.assertRaises(TypeError):
            guarantee_object_id([AN_OBJECT_ID_STR])

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

        # Test some with with_operators set, too
        expected = {'a.b.c': {'$gt': 1}}
        actual = map_fields(TestModel, {'a__b__c__gt': 1}, flatten_keys=True,
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'f.e': {'$lt': 1}}
        actual = map_fields(TestModel, {'d__e__lt': 1}, flatten_keys=True,
                            with_operators=True)
        self.assertEqual(actual, expected)

    def test_map_fields_logical_operators(self):
        """Test the `map_fields()` method with logical operators."""

        expected = {'$and': [{'a': {'b': 1}}, {'c': 2}]}
        actual = map_fields(TestModel, {'$and': [{'a__b': 1}, {'c': 2}]})
        self.assertEqual(actual, expected)

        expected = {'$or': [{'a': {'b': 1}}, {'c': 2}]}
        actual = map_fields(TestModel, {'$or': [{'a__b': 1}, {'c': 2}]})
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

    def test_map_fields_with_and_and_or(self):
        """Test the `map_fields()` method with `$and` and `$or`."""

        # Each of the following tests should be tested for both $and
        # and $or.

        condition = {'$and': [{'a': 1}, {'b': 2}]}
        expected = {'$and': [{'a': 1}, {'c': 2}]}
        actual = map_fields(TestModel, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$or': [{'a': 1}, {'b': 2}]}
        expected = {'$or': [{'a': 1}, {'c': 2}]}
        actual = map_fields(TestModel, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$and': [{'a': 1, 'c': 2}, {'d.e': 3}]}
        expected = {'$and': [{'a': 1, 'c': 2}, {'f.e': 3}]}
        actual = map_fields(TestModel, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$or': [{'a': 1, 'c': 2}, {'d.e': 3}]}
        expected = {'$or': [{'a': 1, 'c': 2}, {'f.e': 3}]}
        actual = map_fields(TestModel, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$and': [{'a': 1}, {'g.h': 2},
                              {'$or': [{'x': 3}, {'y': 4}]}]}
        expected = {'$and': [{'a': 1}, {'i.j': 2},
                             {'$or': [{'z.x': 3}, {'z.y': 4}]}]}
        actual = map_fields(TestModel, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$or': [{'a': 1}, {'g.h': 2},
                             {'$and': [{'x': 3}, {'y': 4}]}]}
        expected = {'$or': [{'a': 1}, {'i.j': 2},
                            {'$and': [{'z.x': 3}, {'z.y': 4}]}]}
        actual = map_fields(TestModel, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

    def test_map_fields_with_operators(self):
        """Test the `map_fields()` method with `with_operators` set."""

        expected = {'a': 1}
        actual = map_fields(TestModel, {'a': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$all': [1, 2]}}
        actual = map_fields(TestModel, {'a__all': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$gt': 1}}
        actual = map_fields(TestModel, {'a__gt': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$gte': 1}}
        actual = map_fields(TestModel, {'a__gte': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$lt': 1}}
        actual = map_fields(TestModel, {'a__lt': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$lte': 1}}
        actual = map_fields(TestModel, {'a__lte': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$ne': 1}}
        actual = map_fields(TestModel, {'a__ne': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$in': [1, 2]}}
        actual = map_fields(TestModel, {'a__in': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$nin': [1, 2]}}
        actual = map_fields(TestModel, {'a__nin': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$exists': True}}
        actual = map_fields(TestModel, {'a__exists': True},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'loc': {'$near': [1, 2]}}
        actual = map_fields(TestModel, {'loc__near': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$size': 2}}
        actual = map_fields(TestModel, {'a__size': 2}, with_operators=True)
        self.assertEqual(actual, expected)

    def test_map_fields_with_operators_not(self):
        ("Test the `map_fields()` method with `not` and "
         "`with_operators` set.")

        expected = {'a': {'$not': {'$gt': 1}}}
        actual = map_fields(TestModel, {'a__not__gt': 1},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'c': {'$not': {'$lt': 1}}}
        actual = map_fields(TestModel, {'b__not__lt': 1},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'b': {'$not': {'$ne': 1}}}}
        actual = map_fields(TestModel, {'a__b__not__ne': 1},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$not': {'$gte': 1}}, 'c': 2}
        actual = map_fields(TestModel, {'a__not__gte': 1, 'b': 2},
                            with_operators=True)
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
