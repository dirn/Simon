"""Tests of the base module that don't use the database"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from contextlib import nested
from datetime import datetime
import warnings

import mock

from simon import Model, connection
from simon.query import Q

from .utils import AN_OBJECT_ID, ModelFactory

DefaultModel = ModelFactory('DefaultModel')
MappedModel = ModelFactory('MappedModel', field_map={'fake': 'real'})
StringModel = ModelFactory('StringModel',
                           __str__=lambda x: 'this is the __str__',
                           __unicode__=lambda x: 'this is the __unicode__')


class BaseModel(Model):
    """Use for testing subclassing."""

    class Meta:
        auto_timestamp = False
        collection = 'collection'
        database = 'database'
        field_map = {'fake': 'real'}
        map_id = False
        required_fields = 'a'
        safe = False
        sort = 'a'


class SubclassModel(BaseModel):
    """Use for testing subclassing."""


class TestModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with mock.patch('simon.connection.MongoClient'):
            cls.connection = connection.connect('localhost', name='test-simon')

    @classmethod
    def tearDownClass(cls):
        # Reset the cached connections and databases so the ones added
        # during one test don't affect another
        connection._connections = None
        connection._databases = None

    def test_all(self):
        """Test the `all()` method."""

        with mock.patch('simon.Model.find') as mock_find:
            DefaultModel.all()

            mock_find.assert_called_with()

    def test_contains(self):
        """Test the `__contains__()` method."""

        m = DefaultModel()
        m._document['a'] = 1
        m._document['b'] = 2

        self.assertIn('a', m)
        self.assertIn('b', m)
        self.assertNotIn('c', m)

    def test_contains_field_map(self):
        """Test the `__contains__()` method with a mapped field."""

        m = MappedModel()
        m._document['real'] = 1

        self.assertIn('fake', m)
        self.assertIn('real', m)
        self.assertNotIn('fake', m._document)

    def test_create(self):
        """Test the `create()` method."""

        TestModel = ModelFactory('TestModel', auto_timestamp=False)

        with mock.patch.object(TestModel, '_update') as _update:
            TestModel.create(a=1)

            _update.assert_called_with({'a': 1}, safe=None, w=None,
                                       upsert=True)

    def test_db(self):
        ("Test that the `db` attribute is associated with classes and "
         "instances.")

        self.assertTrue(hasattr(DefaultModel._meta, 'db'))

        m = DefaultModel()
        self.assertTrue(hasattr(m._meta, 'db'))

    def test_delattr(self):
        """Test the `__delattr__()` method."""

        m = DefaultModel()
        m._document['a'] = 1
        with self.assertRaises(AttributeError):
            del m.b

        del m.a
        self.assertFalse(hasattr(m, 'a'))
        self.assertNotIn('a', m._document)

        with self.assertRaises(AttributeError):
            del m.attribute

    def test_delattr_field_map(self):
        """Test the `__delattr__()` method with a mapped field."""

        m = MappedModel()
        m._document['real'] = 1

        del m.fake
        self.assertNotIn('real', m._document)

    def test_eq(self):
        """Test the `__eq__()` method."""

        # I'm using TestModel here because TestModel1 has crazy
        # settings.

        Subclass1 = ModelFactory('Subclass1', spec=DefaultModel,
                                 collection='some-other-collection')

        Subclass2 = ModelFactory('Subclass2', spec=DefaultModel,
                                 database='some-other-database')

        Subclass3 = ModelFactory('Subclass3', spec=DefaultModel,
                                 collection='defaultmodels')

        # Different classes shouldn't be equal
        m1 = DefaultModel(_id=1)
        m2 = MappedModel(_id=1)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Even a subclass shouldn't be equal ...
        # ... when the collection is different
        m1 = DefaultModel(_id=3)
        m2 = Subclass1(_id=3)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # ... when the database is different
        m1 = DefaultModel(_id=4)
        m2 = Subclass2(_id=4)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Unless they use the same database and collection
        m1 = DefaultModel(_id=1)
        m2 = Subclass3(_id=1)
        self.assertTrue(m1 == m2)
        self.assertTrue(m2 == m1)

        # No _id's shouldn't be equal
        m1 = DefaultModel()
        m2 = DefaultModel()
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Only one _id shouldn't be equal
        m1 = DefaultModel(_id=1)
        m2 = DefaultModel()
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Different _id's shouldn't be equal
        m1 = DefaultModel(_id=1)
        m2 = DefaultModel(_id=2)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # The same _id should be equal
        m1 = DefaultModel(_id=1)
        m2 = DefaultModel(_id=1)
        self.assertTrue(m1 == m2)
        self.assertTrue(m2 == m1)

        # And just for the heck of it...
        m1 = DefaultModel(_id=1)
        self.assertFalse(m1 == 'abc')
        self.assertFalse('abc' == m1)

    def test_find_deprecationwarning(self):
        """Test that `find()` triggers `DeprecationWarning`."""

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')

            with mock.patch.object(DefaultModel, '_find'):
                DefaultModel.find(Q(a=1), Q(b=2))

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))

            expected = 'qs has been deprecated. Please use q instead.'
            actual = str(w[-1].message)
            self.assertEqual(actual, expected)

    def test_get_deprecationwarning(self):
        """Test that `get()` triggers `DeprecationWarning`."""

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')

            with mock.patch.object(DefaultModel, '_find'):
                DefaultModel.get(Q(a=1), Q(b=2))

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))

            expected = 'qs has been deprecated. Please use q instead.'
            actual = str(w[-1].message)
            self.assertEqual(actual, expected)

    def test_get_or_create_create(self):
        """Test the `get_or_create()` method for creating documents."""

        with nested(mock.patch.object(DefaultModel, 'get'),
                    mock.patch.object(DefaultModel, 'create'),
                    ) as (get, create):
            get.side_effect = DefaultModel.NoDocumentFound

            create.return_value = mock.Mock()

            m, created = DefaultModel.get_or_create(_id=AN_OBJECT_ID)

            get.assert_called_with(_id=AN_OBJECT_ID)
            # Because get_or_create() is being called without explicity
            # setting a value for safe, safe's default value will be
            # passed along to creates()
            create.assert_called_with(_id=AN_OBJECT_ID, safe=None, w=None)

            self.assertTrue(created)

    def test_get_or_create_get(self):
        """Test the `get_or_create()` method for getting documents."""

        with mock.patch.object(DefaultModel, 'get') as get:
            get.return_value = mock.Mock()

            m, created = DefaultModel.get_or_create(_id=AN_OBJECT_ID)

            get.assert_called_with(_id=AN_OBJECT_ID)

            self.assertFalse(created)

    def test_getattr(self):
        """Test the `__getattr__()` method."""

        class TestModel(Model):
            attribute = 2

        m = TestModel()

        m._document['a'] = 1
        self.assertEqual(m.a, 1)

        with self.assertRaises(AttributeError):
            m.b

        m._document['c'] = {'d': 1}
        self.assertEqual(m.c, {'d': 1})
        self.assertEqual(m.c__d, 1)
        self.assertEqual(getattr(m, 'c.d'), 1)

        with self.assertRaises(KeyError):
            m.c__e

        self.assertEqual(m.attribute, 2)

    def test_getattr_field_map(self):
        """Test the `__getattr__()` method with a mapped field."""

        m = MappedModel()

        m._document['real'] = 1

        self.assertEqual(getattr(m, 'fake'), 1)

    def test_increment(self):
        """Test the `increment()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.increment('a')

            _update.assert_called_with({'$inc': {'a': 1}}, safe=None, w=None)

            m.increment('a', 2)

            _update.assert_called_with({'$inc': {'a': 2}}, safe=None, w=None)

    def test_increment_multiple(self):
        """Test the `increment()` method with multiple fields."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.increment(a=1, b=2)

            _update.assert_called_with({'$inc': {'a': 1, 'b': 2}}, safe=None,
                                       w=None)

    def test_increment_valueerror(self):
        """Test that `increment()` raises `ValueError`."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with self.assertRaises(ValueError):
            m.increment()

    def test_init(self):
        """Test the `__init__()` method."""

        fields = {'a': 1}
        m = DefaultModel(**fields)
        self.assertTrue(all(getattr(m, k) == v for k, v in fields.items()))

        self.assertIsInstance(m._document, dict)

    def test_ne(self):
        """Test the `__ne__()` method."""

        # __ne__() should call __eq__(), so check that first
        with mock.patch.object(DefaultModel, '__eq__') as __eq__:
            m1 = DefaultModel(_id=1)
            m2 = DefaultModel(_id=1)

            m1 != m2

            __eq__.assert_called_with(m2)

        # __ne__() should return the opposite of __eq__(), so perform
        # a couple of simple comparisons just to make sure
        m1 = DefaultModel(_id=1)
        m2 = DefaultModel(_id=1)
        self.assertNotEqual(m1 == m2, m1 != m2)
        self.assertNotEqual(m2 == m1, m2 != m1)
        self.assertEqual(m1 != m2, m2 != m1)

        m1 = DefaultModel(_id=1)
        m2 = DefaultModel(_id=2)
        self.assertNotEqual(m1 == m2, m1 != m2)
        self.assertNotEqual(m2 == m1, m2 != m1)
        self.assertEqual(m1 != m2, m2 != m1)

    def test_pop(self):
        """Test the `pop()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.pop('a')

            _update.assert_called_with({'$pop': {'a': 1}}, safe=None, w=None)

            m.pop(['a'])

            _update.assert_called_with({'$pop': {'a': 1}}, safe=None, w=None)

            m.pop('-a')

            _update.assert_called_with({'$pop': {'a': -1}}, safe=None, w=None)

    def test_pop_multiple(self):
        """Test the `pop()` method with multiple fields."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.pop(('a', 'b'))

            _update.assert_called_with({'$pop': {'a': 1, 'b': 1}}, safe=None,
                                       w=None)

            m.pop(('a', '-b'))

            _update.assert_called_with({'$pop': {'a': 1, 'b': -1}}, safe=None,
                                       w=None)

            m.pop(('-a', '-b'))

            _update.assert_called_with({'$pop': {'a': -1, 'b': -1}},
                                       safe=None, w=None)

    def test_pull(self):
        """Test the `pull()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.pull('a', 1)

            _update.assert_called_with({'$pull': {'a': 1}}, safe=None, w=None)

            m.pull('a', [1, 2])

            _update.assert_called_with({'$pullAll': {'a': [1, 2]}}, safe=None,
                                       w=None)

            m.pull(a=2)

            _update.assert_called_with({'$pull': {'a': 2}}, safe=None, w=None)

            m.pull(a=[2, 3])

            _update.assert_called_with({'$pullAll': {'a': [2, 3]}}, safe=None,
                                       w=None)

    def test_pull_multiple(self):
        """Test the `pull()` method with multiple fields."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.pull(a=1, b=2)

            _update.assert_called_with({'$pull': {'a': 1, 'b': 2}}, safe=None,
                                       w=None)

            m.pull(a=1, b=[2, 3])

            _update.assert_called_with({'$pull': {'a': 1},
                                        '$pullAll': {'b': [2, 3]}},
                                       safe=None, w=None)

            m.pull(a=[1, 2], b=[3, 4])

            _update.assert_called_with({'$pullAll': {'a': [1, 2],
                                                     'b': [3, 4]}},
                                       safe=None, w=None)

    def test_pull_valueerror(self):
        """Test that `pull()` raises `ValueError`."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with self.assertRaises(ValueError):
            m.pull()

        with self.assertRaises(ValueError):
            m.pull('a')

    def test_push(self):
        """Test the `push()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.push('a', 1)

            _update.assert_called_with({'$push': {'a': 1}}, safe=None, w=None)

            m.push('a', [1, 2])

            _update.assert_called_with({'$pushAll': {'a': [1, 2]}}, safe=None,
                                       w=None)

            m.push(a=2)

            _update.assert_called_with({'$push': {'a': 2}}, safe=None, w=None)

            m.push(a=[2, 3])

            _update.assert_called_with({'$pushAll': {'a': [2, 3]}}, safe=None,
                                       w=None)

    def tet_push_addtoset(self):
        """Test the `push()` method with `$addToSet`."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.push('a', 1, allow_duplicates=False)

            _update.assert_called_with({'$addToSet': {'a': 1}}, safe=None,
                                       w=None)

            m.push('a', [1, 2], allow_duplicates=False)

            _update.assert_called_with({'$addToSet': {'a': {'$each': [1, 2]}}},
                                       safe=False)

            m.push(a=2, allow_duplicates=False)

            _update.assert_called_with({'$addToSet': {'a': 2}}, safe=None,
                                       w=None)

            m.push(a=[2, 3], allow_duplicates=False)

            _update.assert_called_with({'$addToSet': {'a': {'$each': [2, 3]}}},
                                       safe=None, w=None)

    def test_push_multiple(self):
        """Test the `push()` method with multiple fields."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.push(a=1, b=2)

            _update.assert_called_with({'$push': {'a': 1, 'b': 2}}, safe=None,
                                       w=None)

            m.push(a=1, b=[2, 3])

            _update.assert_called_with({'$push': {'a': 1},
                                        '$pushAll': {'b': [2, 3]}},
                                       safe=None, w=None)

            m.push(a=[1, 2], b=[3, 4])

            _update.assert_called_with({'$pushAll': {'a': [1, 2],
                                                     'b': [3, 4]}},
                                       safe=None, w=None)

    def test_push_multiple_addto_set(self):
        ("Test the `push()` method with multiple fields with "
         "`$addToSet`.")

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.push(a=1, b=2, allow_duplicates=False)

            _update.assert_called_with({'$addToSet': {'a': 1, 'b': 2}},
                                       safe=None, w=None)

            m.push(a=1, b=[2, 3], allow_duplicates=False)

            _update.assert_called_with({'$addToSet': {'a': 1,
                                                      'b': {'$each': [2, 3]}}},
                                       safe=None, w=None)

            m.push(a=[1, 2], b=[3, 4], allow_duplicates=False)

            _update.assert_called_with({'$addToSet': {'a': {'$each': [1, 2]},
                                                      'b': {'$each': [3, 4]}}},
                                       safe=None, w=None)

    def test_push_valueerror(self):
        """Test that `push()` raises `ValueError`."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with self.assertRaises(ValueError):
            m.push()

        with self.assertRaises(ValueError):
            m.push('a')

    def test_raw_update(self):
        """Test the `raw_update()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.raw_update({'$set': {'a': 1}})

            _update.assert_called_with({'$set': {'a': 1}}, safe=None, w=None)

    def test_remove_fields(self):
        """Test the `remove_fields()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.remove_fields('a')

            _update.assert_called_with({'$unset': {'a': 1}}, safe=None, w=None)

    def test_remove_fields_multiple(self):
        """Test the `remove_fields()` method with multiple fields."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.remove_fields(('a', 'b'))

            _update.assert_called_with({'$unset': {'a': 1, 'b': 1}},
                                       safe=None, w=None)

    def test_rename(self):
        """Test the `rename()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.rename('a', 'b')

            _update.assert_called_with({'$rename': {'a': 'b'}}, safe=None,
                                       w=None)

    def test_rename_multiple(self):
        """Test the `rename()` method with multiple fields."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=1, b=2)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.rename(a='b', c='d')

            _update.assert_called_with({'$rename': {'a': 'b', 'c': 'd'}},
                                       safe=None, w=None)

    def test_rename_valueerror(self):
        """Test that `rename()` raises `ValueError`."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with self.assertRaises(ValueError):
            m.rename()

        with self.assertRaises(ValueError):
            m.rename('a')

        with self.assertRaises(ValueError):
            m.rename(field_to='a')

    def test_repr(self):
        """Test the `__repr__()` method."""

        m = DefaultModel()

        expected = '<DefaultModel: DefaultModel object>'
        actual = '{0!r}'.format(m)
        self.assertEqual(actual, expected)

        m2 = StringModel()

        expected = '<StringModel: this is the __str__>'
        actual = '{0!r}'.format(m2)
        self.assertEqual(actual, expected)

    def test_save(self):
        """Test the `save()` method."""

        TestModel = ModelFactory('TestModel', auto_timestamp=False)

        m = TestModel(a=1)

        with mock.patch.object(TestModel, '_update') as _update:
            m.save()

            _update.assert_called_with({'a': 1}, safe=None, w=None,
                                       upsert=True)

    def test_save_exception(self):
        """Test that `save()` raises the exception it catches."""

        m = DefaultModel(a=1)

        with mock.patch.object(DefaultModel, '_update') as _update:
            _update.side_effect = ValueError

            with self.assertRaises(ValueError):
                m.save()

            _update.side_effect = TypeError

            with self.assertRaises(TypeError):
                m.save()

    def test_save_fields(self):
        """Test the `save_fields()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=1)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.save_fields('a')

            _update.assert_called_with({'$set': {'a': 1}}, use_internal=True,
                                       safe=None, w=None)

    def test_save_fields_attributeerror(self):
        """Test that `save_fields()` raises `AttributeError`."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with self.assertRaises(AttributeError) as e:
            m.save_fields('a')

        expected = ("The 'DefaultModel' object does not have all of the "
                    "specified fields.")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

    def test_save_fields_multiple(self):
        """Test the `save_fields()` method with multiple fields."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=1, b=2)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.save_fields(('a', 'b'))

            _update.assert_called_with({'$set': {'a': 1, 'b': 1}},
                                       use_internal=True, safe=None, w=None)

    def test_save_timestamps(self):
        """Test that `save()` properly handles adding timestamps."""

        with mock.patch.object(DefaultModel, '_update'):
            # Insert
            m1 = DefaultModel()
            m1.save()

            self.assertIsInstance(m1._document['created'], datetime)
            self.assertIsInstance(m1._document['modified'], datetime)

            # Update
            m2 = DefaultModel(_id=AN_OBJECT_ID, modified=1)
            m2.save()

            self.assertNotIn('created', m2._document)
            self.assertIsInstance(m2._document['modified'], datetime)

    def test_save_timestamps_reset(self):
        ("Test that `save()` properly resets timestamps after an "
         "exception.")

        with mock.patch.object(DefaultModel, '_update') as _update:
            _update.side_effect = Exception

            m1 = DefaultModel()
            try:
                m1.save()
            except:
                pass  # Exception will be raised for this test

            self.assertNotIn('created', m1._document)
            self.assertNotIn('modified', m1._document)

            m2 = DefaultModel(_id=AN_OBJECT_ID, created=1, modified=2)
            try:
                m2.save()
            except:
                pass  # Exception will be raised for this test

            self.assertEqual(m2._document['created'], 1)
            self.assertEqual(m2._document['modified'], 2)

    def test_setattr(self):
        """Test the `__setattr__()` method."""

        class TestModel(Model):
            attribute = 2

        m = TestModel()
        m._document['a'] = 1
        self.assertNotIn('b', m._document)
        with self.assertRaises(AttributeError):
            m.b

        m.b = 2
        self.assertIn('b', m._document)
        self.assertEqual(m.b, 2)
        self.assertEqual(m.b, m._document['b'])

        m.c__d = 3
        self.assertIn('c', m._document)
        self.assertIn('d', m._document['c'])
        self.assertEqual(m.c['d'], 3)
        self.assertEqual(m.c['d'], m._document['c']['d'])

        setattr(m, 'd.e', 4)
        self.assertIn('d', m._document)
        self.assertIn('e', m._document['d'])
        self.assertEqual(m.d['e'], 4)
        self.assertEqual(m.d['e'], m._document['d']['e'])

        m.attribute = 5
        self.assertEqual(m.attribute, 5)
        self.assertNotIn('attribute', m._document)

        with self.assertRaises(AttributeError):
            m._meta = 'this better not work'

    def test_setattr_field_map(self):
        """Test the `__setattr__()` method with a mapped field."""

        m = MappedModel()
        setattr(m, 'fake', 1)

        self.assertIn('real', m._document)
        self.assertEqual(m._document['real'], 1)

    def test_str(self):
        """Test the `__str__()` method."""

        m = DefaultModel()

        expected = 'DefaultModel object'
        actual = '{0!s}'.format(m)
        self.assertEqual(actual, expected)

        m2 = StringModel()

        expected = 'this is the __str__'
        actual = '{0!s}'.format(m2)
        self.assertEqual(actual, expected)

    def test_unicode(self):
        """Test the `__unicode__()` method."""

        m = DefaultModel()

        expected = u'DefaultModel object'
        actual = u'{0}'.format(m)
        self.assertEqual(actual, expected)

        m2 = StringModel()

        expected = u'this is the __unicode__'
        actual = u'{0}'.format(m2)
        self.assertEqual(actual, expected)

    def test_update(self):
        """Test the `update()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel, '_update') as _update:
            m.update(a=1)

            _update.assert_called_with({'$set': {'a': 1}}, safe=None, w=None)


class TestModelMetaClass(unittest.TestCase):
    def test_core_attributes(self):
        """Test the `_meta.core_attributes` attribute."""

        core_attributes = ('_document', '_meta', 'MultipleDocumentsFound',
                           'NoDocumentFound')

        self.assertTrue(all(k in DefaultModel._meta.core_attributes
                            for k in core_attributes))
        self.assertNotIn('Meta', DefaultModel._meta.core_attributes)

        class AttributeModel(Model):
            an_attribute = 1

        self.assertIn('an_attribute', AttributeModel._meta.core_attributes)

    def test_db(self):
        """Test the `_meta.db` attribute."""

        with mock.patch('simon.meta.get_database') as get_database:
            # Make a new class here to ensure that the database hasn't
            # yet been set.
            class MockModel(Model):
                pass

            # At this point the internal _db should be None.
            self.assertIsNone(MockModel._meta._db)

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

    def test_exceptions(self):
        """Test that the exceptions are added."""

        self.assertTrue(hasattr(DefaultModel, 'MultipleDocumentsFound'))
        self.assertTrue(hasattr(DefaultModel, 'NoDocumentFound'))

    def test_subclassed(self):
        """Test a subclassed model."""

        BaseModel = ModelFactory('BaseModel', auto_timestamp=False,
                                 collection='collection', database='database',
                                 field_map={'fake': 'real'}, map_id=False,
                                 required_fields='a', safe=False, sort='a')

        SubclassModel = ModelFactory('SubclassModel', spec=BaseModel)

        base = BaseModel._meta
        subclass = SubclassModel._meta

        # All of _meta's attributes should have the same value.
        self.assertEqual(base.auto_timestamp, subclass.auto_timestamp)
        self.assertEqual(base.collection, subclass.collection)
        self.assertEqual(base.database, subclass.database)
        self.assertEqual(base.field_map, subclass.field_map)
        self.assertEqual(base.map_id, subclass.map_id)
        self.assertEqual(base.required_fields, subclass.required_fields)
        self.assertEqual(base.write_concern, subclass.write_concern)
        self.assertEqual(base.sort, subclass.sort)

        # Each model should have the same list of core attributes
        # except that the subclassed one shouldn't have Meta.
        self.assertEqual(sorted(base.core_attributes),
                         sorted(subclass.core_attributes))
