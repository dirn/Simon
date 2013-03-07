"""Tests of the Meta class"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from bson import ObjectId
import mock
import pymongo

from simon.meta import Meta


def skip_with_mongoclient(f):
    if pymongo.version_tuple[:2] >= (2, 4):
        return unittest.skip('`MongoClient` is supported.')
    else:
        return f


def skip_without_mongoclient(f):
    if pymongo.version_tuple[:2] >= (2, 4):
        return f
    else:
        return unittest.skip('`MongoClient is not supported.')


class TestClass(object):
    """This class can be used with `TestMeta` tests."""


class TestMeta(unittest.TestCase):
    def tearDown(self):
        if hasattr(TestClass, '_meta'):
            delattr(TestClass, '_meta')

    def test_add_to_original(self):
        """Test the `add_to_original()` method."""

        meta = Meta(None)

        meta.add_to_original(TestClass, '_meta')

        # Test the default
        # Use assertEqual for all of these tests to make them easier to
        # read and maintain.
        self.assertEqual(meta.auto_timestamp, True)
        self.assertEqual(meta.class_name, 'TestClass')
        self.assertEqual(meta.collection, 'testclasss')
        self.assertEqual(meta.database, 'default')
        self.assertEqual(meta.field_map, {'id': '_id'})
        self.assertEqual(meta.map_id, True)
        self.assertEqual(meta.required_fields, None)
        self.assertEqual(meta.sort, None)
        self.assertEqual(meta.typed_fields, {'_id': ObjectId})

        if pymongo.version_tuple[:2] >= (2, 4):
            self.assertEqual(meta.write_concern, 1)
        else:
            self.assertEqual(meta.write_concern, True)
        self.assertFalse(hasattr(meta, 'safe'))
        self.assertFalse(hasattr(meta, 'w'))

        # core_attributes is a bit tougher to test
        self.assertTrue(all(k.startswith('_') for k in meta.core_attributes))
        self.assertIn('_document', meta.core_attributes)
        self.assertIn('_meta', meta.core_attributes)

        # Make sure the meta attribute is removed
        self.assertFalse(hasattr(meta, 'meta'))

        # And most importantly of all...
        self.assertTrue(hasattr(TestClass, '_meta'))
        self.assertEqual(TestClass._meta, meta)

    def test_auto_timestamp(self):
        """Test the `auto_timestamp` attribute."""

        meta = Meta(mock.Mock(auto_timestamp=False))

        meta.add_to_original(TestClass, '_meta')

        self.assertFalse(TestClass._meta.auto_timestamp)

    def test_collection(self):
        """Test the `collection` attribute."""

        meta = Meta(mock.Mock(collection='collection'))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.collection, 'collection')

    def test_database(self):
        """Test the `database` attribute."""

        meta = Meta(mock.Mock(database='database'))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.database, 'database')

    def test_extra_attributes(self):
        """Test that extra attributes are not added."""

        meta = Meta(mock.Mock(bad_attribute=1))

        self.assertFalse(hasattr(meta, 'bad_attribute'))

    def test_field_map(self):
        """Test the `field_map` attribute."""

        meta = Meta(mock.Mock(field_map={'fake': 'real'}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.field_map,
                         {'fake': 'real', 'id': '_id'})

        meta = Meta(mock.Mock(field_map={'fake': 'real'}, map_id=False))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.field_map, {'fake': 'real'})

    def test_init(self):
        """Test the `__init__()` method."""

        mock_meta = mock.Mock()

        meta = Meta(mock_meta)

        # test that what you give for the meta class is used as meta
        self.assertEqual(meta.meta, mock_meta)

        # Use assertEqual for all of these tests to make them easier to
        # read and maintain.
        self.assertEqual(meta.auto_timestamp, True)
        self.assertEqual(meta.database, 'default')
        self.assertEqual(meta.field_map, {})
        self.assertEqual(meta.map_id, True)
        self.assertEqual(meta.required_fields, None)
        self.assertEqual(meta.sort, None)
        self.assertEqual(meta.typed_fields, {})

        if pymongo.version_tuple[:2] >= (2, 4):
            self.assertEqual(meta.write_concern, 1)
        else:
            self.assertEqual(meta.write_concern, True)
        self.assertFalse(hasattr(meta, 'safe'))
        self.assertFalse(hasattr(meta, 'w'))

        # make sure attributes added later haven't been added
        self.assertFalse(hasattr(meta, 'class_name'))
        self.assertFalse(hasattr(meta, 'collection'))

    def test_map_id(self):
        """Test the `map_id` attribute."""

        meta = Meta(mock.Mock(map_id=False))

        meta.add_to_original(TestClass, '_meta')

        self.assertFalse(TestClass._meta.map_id)

    def test_repr(self):
        """Test the `__repr__()` method."""

        meta = Meta(None)

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual('{0!r}'.format(meta),
                         '<Meta options for TestClass>')

    def test_required_fields(self):
        """Test the `required_fields` attribute."""

        # single value
        meta = Meta(mock.Mock(required_fields='a'))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.required_fields, ('a',))

        # multiple values
        meta = Meta(mock.Mock(required_fields=['a', 'b']))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.required_fields, ['a', 'b'])

    @skip_with_mongoclient
    def test_safe(self):
        """Test the `safe` attribute."""

        meta = Meta(mock.Mock(safe=False))

        meta.add_to_original(TestClass, '_meta')

        self.assertFalse(TestClass._meta.write_concern)

    @skip_without_mongoclient
    def test_safe_deprecationwarning(self):
        ("Test that `safe` triggers `DeprecationWarning` for PyMongo "
         "with MongoClient.")

        with mock.patch('simon.meta.warnings') as warnings:
            meta = Meta(mock.Mock(safe=True))

            meta.add_to_original(TestClass, '_meta')

            message = 'safe has been deprecated. Please use w instead.'
            warnings.warn.assert_called_with(message, DeprecationWarning)

    def test_sort(self):
        """Test the `sort` attribute."""

        # single value
        meta = Meta(mock.Mock(sort='a'))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.sort, ('a',))

        # multiple values
        meta = Meta(mock.Mock(sort=['a', '-b']))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.sort, ['a', '-b'])

    def test_str(self):
        """Test the `__str__()` method."""

        meta = Meta(None)

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual('{0!s}'.format(meta),
                         'TestClass.Meta')

    def test_typed_fields(self):
        """Test the `typed_fields` attribute."""

        # default
        meta = Meta(None)

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.typed_fields, {'_id': ObjectId})

        # custom
        meta = Meta(mock.Mock(typed_fields={'a': int}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.typed_fields,
                         {'_id': ObjectId, 'a': int})

        # list
        meta = Meta(mock.Mock(typed_fields={'a': [int]}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.typed_fields,
                         {'_id': ObjectId, 'a': [int]})

        # nested
        meta = Meta(mock.Mock(typed_fields={'a.b': int}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.typed_fields,
                         {'_id': ObjectId, 'a.b': int})

        # with _id
        meta = Meta(mock.Mock(typed_fields={'a': int, 'id': None}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.typed_fields, {'a': int, '_id': None})

    def test_typed_fields_typeerror(self):
        """Test the `typed_fields` attribute for `TypeError`."""

        meta = Meta(mock.Mock(typed_fields={'a': 1}))

        with self.assertRaises(TypeError) as e:
            meta.add_to_original(TestClass, '_meta')

        actual = str(e.exception)
        expected = 'Fields must be a type, a typed list, or None.'
        self.assertEqual(actual, expected)

        meta = Meta(mock.Mock(typed_fields={'a': 'b'}))

        with self.assertRaises(TypeError) as e:
            meta.add_to_original(TestClass, '_meta')

        actual = str(e.exception)
        expected = 'Fields must be a type, a typed list, or None.'
        self.assertEqual(actual, expected)

        meta = Meta(mock.Mock(typed_fields={'a': ['b']}))

        with self.assertRaises(TypeError) as e:
            meta.add_to_original(TestClass, '_meta')

        actual = str(e.exception)
        expected = 'Fields must be a type, a typed list, or None.'
        self.assertEqual(actual, expected)

    def test_unicode(self):
        """Test the `__unicode__()` method."""

        meta = Meta(None)

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(u'{0}'.format(meta),
                         u'TestClass.Meta')

    @skip_without_mongoclient
    def test_w(self):
        """Test the `w` attribute."""

        meta = Meta(mock.Mock(w=0))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.write_concern, 0)

    def test_write_conern(self):
        """Test the write concern attributes."""

        if pymongo.version_tuple[:2] >= (2, 4):
            have_attribute = 'w'
            have_not_attribute = 'safe'
            have_on = 1
            have_not_on = True
            have_off = 0
            have_not_off = False
        else:
            have_attribute = 'w'
            have_not_attribute = 'safe'
            have_on = 1
            have_not_on = True
            have_off = 0
            have_not_off = False

        # The correct attribute on
        meta = Meta(mock.Mock(**{have_attribute: have_on}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.write_concern, have_on)

        # The correct attribute off
        meta = Meta(mock.Mock(**{have_attribute: have_off}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.write_concern, have_off)

        # The wrong attribute on
        meta = Meta(mock.Mock(**{have_not_attribute: have_not_on}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.write_concern, have_on)

        # The wrong attribute off
        meta = Meta(mock.Mock(**{have_not_attribute: have_not_off}))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.write_concern, have_off)
