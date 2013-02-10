"""Tests of the base module that don't use the database"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from contextlib import nested
from datetime import datetime
import warnings

from bson import ObjectId
import mock

from simon import Model, connection
from simon.query import Q

AN_OBJECT_ID_STR = '50d4dce70ea5fae6fb84e44b'
AN_OBJECT_ID = ObjectId(AN_OBJECT_ID_STR)


class TestModel1(Model):
    attribute = 2

    class Meta:
        collection = 'test-simon'
        database = 'test-simon-mock'
        field_map = {'x': 'y'}


class TestModel2(Model):
    def __str__(self):
        return 'this is the __str__'

    def __unicode__(self):
        return u'this is the __unicode__'


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Use a dummy connection so that these tests can be run even
        # when there is no MongoDB server. In order to do that, the
        # connection module needs _databases to be a dictionary--
        # this would normally be done during the first call to
        # connect(), but there's no guarantee that will have happened--
        # and it needs the connection placed into it
        if not isinstance(connection._databases, dict):
            connection._databases = {}

        if 'test-simon-mock' not in connection._databases:
            connection._databases['test-simon-mock'] = {
                'test-simon': None,
            }

    @classmethod
    def tearDownClass(cls):
        # Reset the cached connections and databases so the ones added
        # during one test don't affect another
        connection._connections = None
        connection._databases = None

    def test_all(self):
        """Test the `all()` method."""

        with mock.patch('simon.Model.find') as mock_find:
            TestModel1.all()

            mock_find.assert_called_with()

    def test_contains(self):
        """Test the `__contains__()` method."""

        m = TestModel1()
        m._document['a'] = 1
        m._document['b'] = 2

        self.assertIn('a', m)
        self.assertIn('b', m)
        self.assertNotIn('c', m)

    def test_contains_field_map(self):
        """Test the `__contains__()` method with a mapped field."""

        m = TestModel1()
        m._document['y'] = 1

        self.assertIn('x', m)
        self.assertIn('y', m)
        self.assertNotIn('x', m._document)

    def test_create(self):
        """Test the `create()` method."""

        with nested(mock.patch.object(TestModel1, '_update'),
                    mock.patch('simon.base.current_datetime'),
                    ) as (_update, current_datetime):
            current_datetime.return_value = 1

            TestModel1.create(a=1)

            _update.assert_called_with({'a': 1, 'created': 1, 'modified': 1},
                                       safe=False, upsert=True)

    def test_db(self):
        ("Test that the `db` attribute is associated with classes and "
         "instances.")

        self.assertTrue(hasattr(TestModel1._meta, 'db'))

        m = TestModel1()
        self.assertTrue(hasattr(m._meta, 'db'))

    def test_delattr(self):
        """Test the `__delattr__()` method."""

        m = TestModel1()
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

        m = TestModel1()
        m._document['y'] = 1

        del m.x
        self.assertNotIn('y', m._document)

    def test_eq(self):
        """Test the `__eq__()` method."""

        # I'm using TestModel2 here because TestModel1 has crazy
        # settings.

        class TestModel2Subclass1(TestModel2):
            class Meta:
                collection = 'some-other-collection'

        class TestModel2Subclass2(TestModel2):
            class Meta:
                database = 'some-other-database'

        class TestModel2Subclass3(TestModel2):
            class Meta:
                collection = 'testmodel2s'

        # Different classes shouldn't be equal
        m1 = TestModel2(_id=1)
        m2 = TestModel1(_id=1)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Even a subclass shouldn't be equal ...
        # ... when the collection is different
        m1 = TestModel2(_id=3)
        m2 = TestModel2Subclass1(_id=3)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # ... when the database is different
        m1 = TestModel2(_id=4)
        m2 = TestModel2Subclass2(_id=4)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Unless they use the same database and collection
        m1 = TestModel2(_id=1)
        m2 = TestModel2Subclass3(_id=1)
        self.assertTrue(m1 == m2)
        self.assertTrue(m2 == m1)

        # No _id's shouldn't be equal
        m1 = TestModel2()
        m2 = TestModel2()
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Only one _id shouldn't be equal
        m1 = TestModel2(_id=1)
        m2 = TestModel2()
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # Different _id's shouldn't be equal
        m1 = TestModel2(_id=1)
        m2 = TestModel2(_id=2)
        self.assertFalse(m1 == m2)
        self.assertFalse(m2 == m1)

        # The same _id should be equal
        m1 = TestModel2(_id=1)
        m2 = TestModel2(_id=1)
        self.assertTrue(m1 == m2)
        self.assertTrue(m2 == m1)

        # And just for the heck of it...
        m1 = TestModel2(_id=1)
        self.assertFalse(m1 == 'abc')
        self.assertFalse('abc' == m1)

    def test_find_deprecationwarning(self):
        """Test that `find()` triggers `DeprecationWarning`."""

        warnings.simplefilter('error', DeprecationWarning)

        with mock.patch.object(TestModel1, '_find'):
            with self.assertRaises(DeprecationWarning) as e:
                TestModel1.find(Q(a=1), Q(b=2))

        warnings.resetwarnings()
        warnings.simplefilter('default')

        expected = 'qs has been deprecated. Please use q instead.'
        actual = e.exception.message
        self.assertEqual(actual, expected)

    def test_get_deprecationwarning(self):
        """Test that `get()` triggers `DeprecationWarning`."""

        warnings.simplefilter('error', DeprecationWarning)

        with mock.patch.object(TestModel1, '_find'):
            with self.assertRaises(DeprecationWarning) as e:
                TestModel1.get(Q(a=1), Q(b=2))

        warnings.resetwarnings()
        warnings.simplefilter('default')

        expected = 'qs has been deprecated. Please use q instead.'
        actual = e.exception.message
        self.assertEqual(actual, expected)

    def test_get_or_create_create(self):
        """Test the `get_or_create()` method for creating documents."""

        with nested(mock.patch.object(TestModel1, 'get'),
                    mock.patch.object(TestModel1, 'create'),
                    ) as (get, create):
            get.side_effect = TestModel1.NoDocumentFound

            create.return_value = mock.Mock()

            m, created = TestModel1.get_or_create(_id=AN_OBJECT_ID)

            get.assert_called_with(_id=AN_OBJECT_ID)
            # Because get_or_create() is being called without explicity
            # setting a value for safe, safe's default value will be
            # passed along to creates()
            create.assert_called_with(_id=AN_OBJECT_ID, safe=False)

            self.assertTrue(created)

    def test_get_or_create_get(self):
        """Test the `get_or_create()` method for getting documents."""

        with mock.patch.object(TestModel1, 'get') as get:
            get.return_value = mock.Mock()

            m, created = TestModel1.get_or_create(_id=AN_OBJECT_ID)

            get.assert_called_with(_id=AN_OBJECT_ID)

            self.assertFalse(created)

    def test_getattr(self):
        """Test the `__getattr__()` method."""

        m = TestModel1()

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

        m = TestModel1()

        m._document['y'] = 1

        self.assertEqual(getattr(m, 'x'), 1)

    def test_increment(self):
        """Test the `increment()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.increment('a')

            _update.assert_called_with({'$inc': {'a': 1}}, safe=False)

            m.increment('a', 2)

            _update.assert_called_with({'$inc': {'a': 2}}, safe=False)

    def test_increment_multiple(self):
        """Test the `increment()` method with multiple fields."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.increment(a=1, b=2)

            _update.assert_called_with({'$inc': {'a': 1, 'b': 2}}, safe=False)

    def test_increment_valueerror(self):
        """Test that `increment()` raises `ValueError`."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with self.assertRaises(ValueError):
            m.increment()

    def test_init(self):
        """Test the `__init__()` method."""

        fields = {'a': 1}
        m = TestModel1(**fields)
        self.assertTrue(all(getattr(m, k) == v for k, v in fields.items()))

        self.assertIsInstance(m._document, dict)

    def test_ne(self):
        """Test the `__ne__()` method."""

        # __ne__() should call __eq__(), so check that first
        with mock.patch.object(TestModel1, '__eq__') as __eq__:
            m1 = TestModel1(_id=1)
            m2 = TestModel1(_id=1)

            m1 != m2

            __eq__.assert_called_with(m2)

        # __ne__() should return the opposite of __eq__(), so perform
        # a couple of simple comparisons just to make sure
        m1 = TestModel1(_id=1)
        m2 = TestModel1(_id=1)
        self.assertNotEqual(m1 == m2, m1 != m2)
        self.assertNotEqual(m2 == m1, m2 != m1)
        self.assertEqual(m1 != m2, m2 != m1)

        m1 = TestModel1(_id=1)
        m2 = TestModel1(_id=2)
        self.assertNotEqual(m1 == m2, m1 != m2)
        self.assertNotEqual(m2 == m1, m2 != m1)
        self.assertEqual(m1 != m2, m2 != m1)

    def test_raw_update(self):
        """Test the `raw_update()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.raw_update({'$set': {'a': 1}})

            _update.assert_called_with({'$set': {'a': 1}}, safe=False)

    def test_remove_fields(self):
        """Test the `remove_fields()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.remove_fields('a')

            _update.assert_called_with({'$unset': {'a': 1}}, safe=False)

    def test_remove_fields_multiple(self):
        """Test the `remove_fields()` method with multiple fields."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.remove_fields(('a', 'b'))

            _update.assert_called_with({'$unset': {'a': 1, 'b': 1}},
                                       safe=False)

    def test_repr(self):
        """Test the `__repr__()` method."""

        m = TestModel1()

        expected = '<TestModel1: TestModel1 object>'
        actual = '{0!r}'.format(m)
        self.assertEqual(actual, expected)

        m2 = TestModel2()

        expected = '<TestModel2: this is the __str__>'
        actual = '{0!r}'.format(m2)
        self.assertEqual(actual, expected)

    def test_save(self):
        """Test the `save()` method."""

        m = TestModel1(a=1)

        with nested(mock.patch.object(TestModel1, '_update'),
                    mock.patch('simon.base.current_datetime'),
                    ) as (_update, current_datetime):
            current_datetime.return_value = 1

            m.save()

            _update.assert_called_with({'a': 1, 'created': 1, 'modified': 1},
                                       safe=False, upsert=True)

    def test_save_fields(self):
        """Test the `save_fields()` method."""

        m = TestModel1(_id=AN_OBJECT_ID, a=1)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.save_fields('a')

            _update.assert_called_with({'$set': {'a': 1}}, use_internal=True,
                                       safe=False)

    def test_save_fields_attributeerror(self):
        """Test that `save_fields()` raises `AttributeError`."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with self.assertRaises(AttributeError) as e:
            m.save_fields('a')

        expected = ("The 'TestModel1' object does not have all of the "
                    "specified fields.")
        actual = e.exception.message
        self.assertEqual(actual, expected)

    def test_save_fields_multiple(self):
        """Test the `save_fields()` method with multiple fields."""

        m = TestModel1(_id=AN_OBJECT_ID, a=1, b=2)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.save_fields(('a', 'b'))

            _update.assert_called_with({'$set': {'a': 1, 'b': 1}},
                                       use_internal=True, safe=False)

    def test_save_timestamps(self):
        """Test that `save()` properly handles adding timestamps."""

        with mock.patch.object(TestModel1, '_update'):
            # Insert
            m1 = TestModel1()
            m1.save()

            self.assertIsInstance(m1._document['created'], datetime)
            self.assertIsInstance(m1._document['modified'], datetime)

            # Update
            m2 = TestModel1(_id=AN_OBJECT_ID)
            m2.save()

            self.assertNotIn('created', m2._document)
            self.assertIsInstance(m2._document['modified'], datetime)

    def test_setattr(self):
        """Test the `__setattr__()` method."""

        m = TestModel1()
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

        m = TestModel1()
        setattr(m, 'x', 1)

        self.assertIn('y', m._document)
        self.assertEqual(m._document['y'], 1)

    def test_str(self):
        """Test the `__str__()` method."""

        m = TestModel1()

        expected = 'TestModel1 object'
        actual = '{0!s}'.format(m)
        self.assertEqual(actual, expected)

        m2 = TestModel2()

        expected = 'this is the __str__'
        actual = '{0!s}'.format(m2)
        self.assertEqual(actual, expected)

    def test_unicode(self):
        """Test the `__unicode__()` method."""

        m = TestModel1()

        expected = u'TestModel1 object'
        actual = u'{0}'.format(m)
        self.assertEqual(actual, expected)

        m2 = TestModel2()

        expected = u'this is the __unicode__'
        actual = u'{0}'.format(m2)
        self.assertEqual(actual, expected)

    def test_update(self):
        """Test the `update()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1, '_update') as _update:
            m.update(a=1)

            _update.assert_called_with({'$set': {'a': 1}}, safe=False)
