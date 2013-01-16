try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from pymongo.collection import Collection

from simon import Model


class DefaultModel(Model):
    """This is used to test the default values."""


class TestModel1(Model):
    class Meta:
        collection = 'test1'
        some_junk_value = 1


class TestModel2(Model):
    some_junk_value = 2

    class Meta:
        auto_timestamp = False
        collection = 'test2'
        field_map = {'python_key': 'mongo_key'}
        safe = True


class TestModel3(Model):
    class Meta:
        auto_timestamp = True
        collection = 'test3'
        database = 'simon'
        field_map = {'python_key': 'mongo_key'}
        map_id = False
        safe = False
        sort = '-id'


class TestModel4(TestModel3):
    """This is used to test a subclassed model."""


class TestModel5(Model):
    class Meta:
        sort = ['id', '-name']


class TestMetaClass(unittest.TestCase):
    def test_auto_timestamp(self):
        """Test the `_meta.auto_timestamp` attribute."""

        # Test the default behavior
        self.assertTrue(hasattr(DefaultModel._meta, 'auto_timestamp'))
        self.assertTrue(DefaultModel._meta.auto_timestamp)

        self.assertTrue(hasattr(TestModel1._meta, 'auto_timestamp'))
        self.assertTrue(TestModel1._meta.auto_timestamp)

        # Test the explicit behavior, just as a sanity check
        self.assertFalse(TestModel2._meta.auto_timestamp)
        self.assertTrue(TestModel3._meta.auto_timestamp)
        self.assertTrue(TestModel5._meta.auto_timestamp)

    def test_class_name(self):
        """Test the `_meta.class_name` attribute."""

        self.assertEqual(DefaultModel.__name__, DefaultModel._meta.class_name)
        self.assertEqual(TestModel1.__name__, TestModel1._meta.class_name)
        self.assertEqual(TestModel2.__name__, TestModel2._meta.class_name)
        self.assertEqual(TestModel3.__name__, TestModel3._meta.class_name)
        self.assertEqual(TestModel5.__name__, TestModel5._meta.class_name)

    def test_collection(self):
        """Test the `_meta.collection` attribute."""

        self.assertEqual(TestModel1._meta.collection, 'test1')

    def test_collection_none(self):
        """Test that `_meta.collection` is a required attribute."""

        class TestModel(Model):
            pass

        class SomeOtherModel(Model):
            pass

        class ModelWithMeta(Model):
            class Meta:
                database = 'default'

        self.assertEqual(TestModel._meta.collection, 'testmodels')
        self.assertEqual(SomeOtherModel._meta.collection, 'someothermodels')
        self.assertEqual(ModelWithMeta._meta.collection, 'modelwithmetas')

    def test_core_attributes(self):
        """Test the `_meta.core_attributes` attribute."""

        self.assertTrue('_document' in DefaultModel._meta.core_attributes)
        self.assertTrue('_meta' in DefaultModel._meta.core_attributes)
        self.assertFalse('Meta' in DefaultModel._meta.core_attributes)
        self.assertTrue(
            'MultipleDocumentsFound' in DefaultModel._meta.core_attributes)
        self.assertTrue(
            'NoDocumentFound' in DefaultModel._meta.core_attributes)

        self.assertTrue('_document' in TestModel1._meta.core_attributes)
        self.assertTrue('_meta' in TestModel1._meta.core_attributes)
        self.assertTrue('Meta' in TestModel1._meta.core_attributes)
        self.assertTrue(
            'MultipleDocumentsFound' in TestModel1._meta.core_attributes)
        self.assertTrue('NoDocumentFound' in TestModel1._meta.core_attributes)

        self.assertTrue('some_junk_value' in TestModel2._meta.core_attributes)

        self.assertTrue('Meta' in TestModel3._meta.core_attributes)
        self.assertFalse('Meta' in TestModel4._meta.core_attributes)

    def test_database(self):
        """Test the `_meta.database` attribute."""

        self.assertEqual(DefaultModel._meta.database, 'default')
        self.assertEqual(TestModel1._meta.database, 'default')
        self.assertEqual(TestModel3._meta.database, 'simon')

    def test_db(self):
        """Test the `_meta.db` attribute."""

        self.assertTrue(isinstance(DefaultModel._meta.db, Collection))

        with mock.patch('simon.base.get_database') as get_database:
            # Make a new class here to ensure that the database hasn't
            # yet been set.
            class MockModel(Model):
                pass

            # At this point the internal _db should be None.
            self.assertTrue(TestModel1._meta._db is None)

            # Once the mock is called, _db should always be set to this
            # value.
            get_database.return_value = {MockModel._meta.collection: 1}

            # Accessing the property triggers the call.
            MockModel._meta.db

            self.assertEqual(MockModel._meta._db, 1)
            get_database.assert_called_with(MockModel._meta.database)

            # As per above, _db should not get set to this value.
            get_database.return_value = {MockModel._meta.collection: 2}

            MockModel._meta.db

            self.assertEqual(MockModel._meta._db, 1)

    def test_default_attributes(self):
        """Test that the default attributes are added."""

        self.assertTrue(hasattr(TestModel1, 'MultipleDocumentsFound'))
        self.assertTrue(hasattr(TestModel1, 'NoDocumentFound'))

        self.assertTrue(hasattr(TestModel1._meta, 'core_attributes'))

    def test_extra_attributes(self):
        """Test that extra attributes are not added to `_meta`."""

        self.assertFalse(hasattr(TestModel1._meta, 'some_junk_value'))

    def test_field_map(self):
        """Test the `_meta.field_map` attribute."""

        default = {'id': '_id'}
        self.assertEqual(default, DefaultModel._meta.field_map)
        self.assertEqual(default, TestModel1._meta.field_map)

        # This test checks for the default being added by the meta class
        custom = {'id': '_id', 'python_key': 'mongo_key'}
        self.assertEqual(custom, TestModel2._meta.field_map)

        # This test checks the behavior when map_id is False
        custom = {'python_key': 'mongo_key'}
        self.assertEqual(custom, TestModel3._meta.field_map)

    def test_map_id(self):
        """Test the `_meta.map_id` attribute."""

        self.assertTrue(DefaultModel._meta.map_id)
        self.assertTrue(TestModel1._meta.map_id)
        self.assertFalse(TestModel3._meta.map_id)

    def test_repr(self):
        """Test the `__repr__()` method."""

        self.assertEqual('{0!r}'.format(DefaultModel._meta),
                         '<Meta options for DefaultModel>')

    def test_safe(self):
        """Test the `_meta.safe` attribute."""

        self.assertFalse(DefaultModel._meta.safe)
        self.assertFalse(TestModel1._meta.safe)
        self.assertTrue(TestModel2._meta.safe)
        self.assertFalse(TestModel3._meta.safe)

    def test_sort(self):
        """Test the `_meta.sort` attribute."""

        self.assertTrue(DefaultModel._meta.sort is None)
        self.assertTrue(TestModel1._meta.sort is None)
        self.assertTrue(TestModel2._meta.sort is None)
        self.assertEqual(TestModel3._meta.sort, ('-id',))
        self.assertEqual(TestModel5._meta.sort, ['id', '-name'])

    def test_str(self):
        """Test the `__str__()` method."""

        self.assertEqual('{0!s}'.format(DefaultModel._meta),
                         'DefaultModel.Meta')

    def test_subclassed(self):
        """Test a subclassed model."""

        meta3 = TestModel3._meta
        meta4 = TestModel4._meta

        # All of _meta's attributes should have the same value.
        self.assertEqual(meta3.auto_timestamp, meta4.auto_timestamp)
        self.assertEqual(meta3.collection, meta4.collection)
        self.assertEqual(meta3.database, meta4.database)
        self.assertEqual(meta3.field_map, meta4.field_map)
        self.assertEqual(meta3.map_id, meta4.map_id)
        self.assertEqual(meta3.safe, meta4.safe)
        self.assertEqual(meta3.sort, meta4.sort)

        # Each model should have the same list of core attributes
        # except that the subclassed one shouldn't have Meta.
        self.assertEqual(sorted(meta3.core_attributes),
                         sorted(meta4.core_attributes + ['Meta']))

    def test_unicode(self):
        """Test the `__unicode__()` method."""

        self.assertEqual(u'{0}'.format(DefaultModel._meta),
                         u'DefaultModel.Meta')
