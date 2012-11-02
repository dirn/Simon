try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import MongoModel
from simon.base import MongoModelMetaClass


class TestModel1(MongoModel):
    class Meta:
        collection = 'test1'
        some_junk_value = 1


class TestModel2(MongoModel):
    class Meta:
        collection = 'test2'
        field_map = {'python_key': 'mongo_key'}


class TestModel3(object):
    __metaclass__ = MongoModelMetaClass

    class Meta:
        collection = 'test3'
        field_map = {'python_key': 'mongo_key'}
        map_id = False


class TestMetaClass(unittest.TestCase):
    def test_collection(self):
        """Test the `_meta.collection` attribute."""

        self.assertEqual(TestModel1._meta.collection, 'test1')

    def test_collection_none(self):
        """Test that `_meta.collection` is a required attribute."""

        with self.assertRaises(AttributeError):
            class TestModel(MongoModel):
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
