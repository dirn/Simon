try:
    import unittest2 as unittest
except ImportError:
    import unittest

from datetime import datetime
import warnings

from bson.errors import InvalidId
import pymongo

from simon.utils import (current_datetime, get_nested_key, guarantee_object_id,
                         is_atomic, map_fields, parse_kwargs,
                         remove_nested_key, set_write_concern,
                         set_write_concern_as_safe, set_write_concern_as_w,
                         update_nested_keys)

from .utils import AN_OBJECT_ID, AN_OBJECT_ID_STR


field_map = {'b': 'c', 'd.e': 'f.e', 'g.h': 'i.j', 'x': 'z.x', 'y': 'z.y'}


class TestUtils(unittest.TestCase):
    def test_current_datetime(self):
        """Test teh `current_datetime()` method."""

        now = current_datetime()

        self.assertIsInstance(now, datetime)

        # Because now.microsecond should have any value smaller than a
        # millisecond record, its value should be evenly divisible by 0
        self.assertEqual(now.microsecond % 1000, 0)

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

    def test_is_atomic(self):
        """Test the `is_atomic()` method."""

        self.assertTrue(is_atomic({'$set': {'a': 1}}))

        self.assertFalse(is_atomic({'a': 1}))

    def test_map_fields(self):
        """Test the `map_fields()` method."""

        expected = {'a': 1}
        actual = map_fields(field_map, {'a': 1})
        self.assertEqual(actual, expected)

        expected = {'__a': 1}
        actual = map_fields(field_map, {'__a': 1})
        self.assertEqual(actual, expected)

        expected = {'a__': 1}
        actual = map_fields(field_map, {'a__': 1})
        self.assertEqual(actual, expected)

        expected = {'__a__': 1}
        actual = map_fields(field_map, {'__a__': 1})
        self.assertEqual(actual, expected)

        expected = {'c': 1}
        actual = map_fields(field_map, {'b': 1})
        self.assertEqual(actual, expected)

        expected = {'a': 1, 'c': 2}
        actual = map_fields(field_map, {'a': 1, 'b': 2})
        self.assertEqual(actual, expected)

        expected = {'f': {'e': 1}}
        actual = map_fields(field_map, {'d.e': 1})
        self.assertEqual(actual, expected)

        expected = {'i': {'j': 1}}
        actual = map_fields(field_map, {'g.h': 1})
        self.assertEqual(actual, expected)

        expected = {'z': {'x': 1, 'y': 2}}
        actual = map_fields(field_map, {'x': 1, 'y': 2})
        self.assertEqual(actual, expected)

    def test_map_fields_flattened_keys(self):
        """Test the `map_fields()` method with `flatten_keys` set."""

        expected = {'a': 1}
        actual = map_fields(field_map, {'a': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'__a': 1}
        actual = map_fields(field_map, {'__a': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a__': 1}
        actual = map_fields(field_map, {'a__': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'__a__': 1}
        actual = map_fields(field_map, {'__a__': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a': 1, 'c': 2}
        actual = map_fields(field_map, {'a': 1, 'b': 2}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a.b': 1}
        actual = map_fields(field_map, {'a__b': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a.b.c': 1}
        actual = map_fields(field_map, {'a__b__c': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'a.b': 1, 'a.c': 2}
        actual = map_fields(field_map, {'a__b': 1, 'a__c': 2},
                            flatten_keys=True)
        self.assertEqual(actual, expected)

        expected = {'f.e': 1}
        actual = map_fields(field_map, {'d__e': 1}, flatten_keys=True)
        self.assertEqual(actual, expected)

        # Test some with with_operators set, too
        expected = {'a.b.c': {'$gt': 1}}
        actual = map_fields(field_map, {'a__b__c__gt': 1}, flatten_keys=True,
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'f.e': {'$lt': 1}}
        actual = map_fields(field_map, {'d__e__lt': 1}, flatten_keys=True,
                            with_operators=True)
        self.assertEqual(actual, expected)

    def test_map_fields_logical_operators(self):
        """Test the `map_fields()` method with logical operators."""

        expected = {'$and': [{'a': {'b': 1}}, {'c': 2}]}
        actual = map_fields(field_map, {'$and': [{'a__b': 1}, {'c': 2}]})
        self.assertEqual(actual, expected)

        expected = {'$or': [{'a': {'b': 1}}, {'c': 2}]}
        actual = map_fields(field_map, {'$or': [{'a__b': 1}, {'c': 2}]})
        self.assertEqual(actual, expected)

    def test_map_fields_second_pass(self):
        """Test the `map_fields()` method with a second pass."""

        expected = {'a': {'b': 1}}
        actual = map_fields(field_map, {'a__b': 1})
        self.assertEqual(actual, expected)

        expected = {'c': {'d': 1}}
        actual = map_fields(field_map, {'b__d': 1})
        self.assertEqual(actual, expected)

        expected = {'f': {'e': 1}}
        actual = map_fields(field_map, {'d__e': 1})
        self.assertEqual(actual, expected)

        expected = {'i': {'j': 1}}
        actual = map_fields(field_map, {'g__h': 1})
        self.assertEqual(actual, expected)

        expected = {'z': {'x': 1, 'y': 2}}
        actual = map_fields(field_map, {'z__x': 1, 'z__y': 2})
        self.assertEqual(actual, expected)

    def test_map_fields_with_and_and_or(self):
        """Test the `map_fields()` method with `$and` and `$or`."""

        # Each of the following tests should be tested for both $and
        # and $or.

        condition = {'$and': [{'a': 1}, {'b': 2}]}
        expected = {'$and': [{'a': 1}, {'c': 2}]}
        actual = map_fields(field_map, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$or': [{'a': 1}, {'b': 2}]}
        expected = {'$or': [{'a': 1}, {'c': 2}]}
        actual = map_fields(field_map, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$and': [{'a': 1, 'c': 2}, {'d.e': 3}]}
        expected = {'$and': [{'a': 1, 'c': 2}, {'f.e': 3}]}
        actual = map_fields(field_map, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$or': [{'a': 1, 'c': 2}, {'d.e': 3}]}
        expected = {'$or': [{'a': 1, 'c': 2}, {'f.e': 3}]}
        actual = map_fields(field_map, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$and': [{'a': 1}, {'g.h': 2},
                              {'$or': [{'x': 3}, {'y': 4}]}]}
        expected = {'$and': [{'a': 1}, {'i.j': 2},
                             {'$or': [{'z.x': 3}, {'z.y': 4}]}]}
        actual = map_fields(field_map, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

        condition = {'$or': [{'a': 1}, {'g.h': 2},
                             {'$and': [{'x': 3}, {'y': 4}]}]}
        expected = {'$or': [{'a': 1}, {'i.j': 2},
                            {'$and': [{'z.x': 3}, {'z.y': 4}]}]}
        actual = map_fields(field_map, condition, flatten_keys=True)
        self.assertEqual(actual, expected)

    def test_map_fields_with_operators(self):
        """Test the `map_fields()` method with `with_operators` set."""

        expected = {'a': 1}
        actual = map_fields(field_map, {'a': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$all': [1, 2]}}
        actual = map_fields(field_map, {'a__all': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$gt': 1}}
        actual = map_fields(field_map, {'a__gt': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$gte': 1}}
        actual = map_fields(field_map, {'a__gte': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$lt': 1}}
        actual = map_fields(field_map, {'a__lt': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$lte': 1}}
        actual = map_fields(field_map, {'a__lte': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$ne': 1}}
        actual = map_fields(field_map, {'a__ne': 1}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$in': [1, 2]}}
        actual = map_fields(field_map, {'a__in': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$nin': [1, 2]}}
        actual = map_fields(field_map, {'a__nin': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$exists': True}}
        actual = map_fields(field_map, {'a__exists': True},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'loc': {'$near': [1, 2]}}
        actual = map_fields(field_map, {'loc__near': [1, 2]},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$size': 2}}
        actual = map_fields(field_map, {'a__size': 2}, with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$elemMatch': {'b': 2}}}
        actual = map_fields(field_map, {'a__elemMatch': {'b': 2}},
                            with_operators=True)
        self.assertEqual(actual, expected)

    def test_map_fields_with_operators_not(self):
        ("Test the `map_fields()` method with `not` and "
         "`with_operators` set.")

        expected = {'a': {'$not': {'$gt': 1}}}
        actual = map_fields(field_map, {'a__not__gt': 1},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'c': {'$not': {'$lt': 1}}}
        actual = map_fields(field_map, {'b__not__lt': 1},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'b': {'$not': {'$ne': 1}}}}
        actual = map_fields(field_map, {'a__b__not__ne': 1},
                            with_operators=True)
        self.assertEqual(actual, expected)

        expected = {'a': {'$not': {'$gte': 1}}, 'c': 2}
        actual = map_fields(field_map, {'a__not__gte': 1, 'b': 2},
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

    def test_set_write_concern(self):
        """Test the `set_write_concern()` method."""

        if pymongo.version_tuple[:2] < (2, 4):
            self.assertEqual(set_write_concern, set_write_concern_as_safe)
        else:
            self.assertEqual(set_write_concern, set_write_concern_as_w)

    def test_set_write_concern_as_safe(self):
        """Test the `set_write_concern_as_safe()` method."""

        options = {'safe': True}
        set_write_concern_as_safe(options, False)
        self.assertEqual(options, {'safe': True})

        options = {'safe': False}
        set_write_concern_as_safe(options, False)
        self.assertEqual(options, {'safe': False})

        options = {'safe': False}
        set_write_concern_as_safe(options, True)
        self.assertEqual(options, {'safe': True})

    def test_set_write_concern_as_safe_with_w(self):
        """Test the `set_write_concern_as_safe()` method with `w`."""

        options = {'w': 1}
        set_write_concern_as_safe(options, False)
        self.assertEqual(options, {'safe': True})

        options = {'w': 0}
        set_write_concern_as_safe(options, False)
        self.assertEqual(options, {'safe': False})

        options = {'w': 0}
        set_write_concern_as_safe(options, True)
        self.assertEqual(options, {'safe': True})

    def test_set_write_concern_as_w(self):
        """Test the `set_write_concern_as_w()` method."""

        options = {'w': 2}
        set_write_concern_as_w(options, 0)
        self.assertEqual(options, {'w': 2})

        options = {'w': 0}
        set_write_concern_as_w(options, 0)
        self.assertEqual(options, {'w': 0})

        options = {'w': 0}
        set_write_concern_as_w(options, 2)
        self.assertEqual(options, {'w': 2})

    def test_set_write_concern_as_w_deprecationwarning(self):
        ("Test that `set_write_concern_as_w()` triggers "
         "`DeprecationWarning`.")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')

            set_write_concern_as_w({'safe': True}, 0)

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))

            expected = 'safe has been deprecated. Please use w instead.'
            actual = str(w[-1].message)
            self.assertEqual(actual, expected)

    def test_set_write_concern_as_w_with_safe(self):
        """Test the `set_write_concern_as_w()` method with `safe`."""

        options = {'safe': True}
        set_write_concern_as_w(options, 0)
        self.assertEqual(options, {'w': 1})

        options = {'safe': 0}
        set_write_concern_as_w(options, 0)
        self.assertEqual(options, {'w': 0})

        options = {'safe': 0}
        set_write_concern_as_w(options, 2)
        self.assertEqual(options, {'w': 2})

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
