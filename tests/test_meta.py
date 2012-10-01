import unittest

from simon import MongoModel


class TestModel1(MongoModel):
    class Meta:
        collection = 'test1'
        some_junk_value = 1


class TestModel2(MongoModel):
    class Meta:
        collection = 'test2'
        field_map = {'python_key': 'mongo_key'}


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

        self.assertTrue(hasattr(TestModel1._meta, 'core_attributes'))
        self.assertTrue(hasattr(TestModel1._meta, 'document'))

    def test_extra_attributes(self):
        """Test that extra attributes are added to `_meta`."""

        self.assertTrue(hasattr(TestModel1._meta, 'some_junk_value'))
        self.assertEqual(TestModel1._meta.some_junk_value, 1)

    def test_field_map(self):
        """Test the `_meta.field_map` attribute."""

        default = {'id': '_id'}
        self.assertTrue(
            all(default[k] == v for k, v in TestModel1._meta.field_map.items()))
        self.assertTrue(
            all(k in default for k in TestModel1._meta.field_map.keys()))
        self.assertTrue(
            all(k in TestModel1._meta.field_map for k in default.keys()))

        custom = {'python_key': 'mongo_key'}
        self.assertTrue(
            all(custom[k] == v for k, v in TestModel2._meta.field_map.items()))
        self.assertTrue(
            all(k in custom for k in TestModel2._meta.field_map.keys()))
        self.assertTrue(
            all(k in TestModel2._meta.field_map for k in custom.keys()))
