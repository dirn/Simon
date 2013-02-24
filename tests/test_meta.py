"""Tests of the Meta class"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from simon.meta import Meta


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
        self.assertEqual(meta.safe, True)
        self.assertEqual(meta.sort, None)

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
        self.assertEqual(meta.safe, True)
        self.assertEqual(meta.sort, None)

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

        meta = Meta(mock.Mock(required_fields='a'))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.required_fields, ('a',))

        meta = Meta(mock.Mock(required_fields=['a', 'b']))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.required_fields, ['a', 'b'])

    def test_safe(self):
        """Test the `safe` attribute."""

        meta = Meta(mock.Mock(safe=False))

        meta.add_to_original(TestClass, '_meta')

        self.assertFalse(TestClass._meta.safe)

    def test_sort(self):
        """Test the `sort` attribute."""

        meta = Meta(mock.Mock(sort='a'))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.sort, ('a',))

        meta = Meta(mock.Mock(sort=['a', '-b']))

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(TestClass._meta.sort, ['a', '-b'])

    def test_str(self):
        """Test the `__str__()` method."""

        meta = Meta(None)

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual('{0!s}'.format(meta),
                         'TestClass.Meta')

    def test_unicode(self):
        """Test the `__unicode__()` method."""

        meta = Meta(None)

        meta.add_to_original(TestClass, '_meta')

        self.assertEqual(u'{0}'.format(meta),
                         u'TestClass.Meta')
