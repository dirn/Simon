try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import Model
from simon.base import ModelMetaClass


class TestModel1(Model):
    class Meta:
        collection = 'test1'
        some_junk_value = 1


class TestModel2(Model):
    class Meta:
        auto_timestamp = False
        collection = 'test2'
        field_map = {'python_key': 'mongo_key'}
        safe = True


class TestModel3(object):
    __metaclass__ = ModelMetaClass

    class Meta:
        auto_timestamp = True
        collection = 'test3'
        field_map = {'python_key': 'mongo_key'}
        map_id = False
        safe = False


class TestMetaClass(unittest.TestCase):
    def test_auto_timestamp(self):
        """Test the `_meta.auto_timestamp` attribute."""

        # Test the default behavior
        self.assertTrue(hasattr(TestModel1._meta, 'auto_timestamp'))
        self.assertTrue(TestModel1._meta.auto_timestamp)

        # Test the explicit behavior, just as a sanity check
        self.assertFalse(TestModel2._meta.auto_timestamp)
        self.assertTrue(TestModel3._meta.auto_timestamp)

    def test_collection(self):
        """Test the `_meta.collection` attribute."""

        self.assertEqual(TestModel1._meta.collection, 'test1')

    def test_collection_none(self):
        """Test that `_meta.collection` is a required attribute."""

        with self.assertRaises(AttributeError):
            class TestModel(Model):
                pass

    def test_default_attributes(self):
        """Test that the default attributes are added."""

        self.assertTrue(hasattr(TestModel1, 'MultipleDocumentsFound'))
        self.assertTrue(hasattr(TestModel1, 'NoDocumentFound'))

        self.assertTrue(hasattr(TestModel1._meta, 'core_attributes'))

    def test_extra_attributes(self):
        """Test that extra attributes are added to `_meta`."""

        self.assertTrue(hasattr(TestModel1._meta, 'some_junk_value'))
        self.assertEqual(TestModel1._meta.some_junk_value, 1)

    def test_field_map(self):
        """Test the `_meta.field_map` attribute."""

        default = {'id': '_id'}
        self.assertEqual(default, TestModel1._meta.field_map)

        # This test checks for the default being added by the meta class
        custom = {'id': '_id', 'python_key': 'mongo_key'}
        self.assertEqual(custom, TestModel2._meta.field_map)

        # This test checks the behavior when map_id is False
        custom = {'python_key': 'mongo_key'}
        self.assertEqual(custom, TestModel3._meta.field_map)

    def test_safe(self):
        """Test the `_meta.safe` attribute."""

        self.assertFalse(TestModel1._meta.safe)
        self.assertTrue(TestModel2._meta.safe)
        self.assertFalse(TestModel3._meta.safe)
