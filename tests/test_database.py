"""Tests of the database functionality"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from contextlib import nested
from datetime import datetime

from bson import ObjectId
import mock

from simon import Model, connection, query

AN_OBJECT_ID_STR = '50d4dce70ea5fae6fb84e44b'
AN_OBJECT_ID = ObjectId(AN_OBJECT_ID_STR)


class TestModel1(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        field_map = {'id': '_id', 'fake': 'real'}


class TestModel2(Model):
    class Meta:
        auto_timestamp = False
        collection = 'test-simon'
        database = 'test-simon'


class TestModel3(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        safe = False
        sort = 'a'


class TestModel4(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        sort = '-a'


class TestModel5(Model):
    class Meta:
        auto_timestamp = False
        required_fields = ('a', 'b')


class TestDatabase(unittest.TestCase):
    """Test database interaction"""

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

    def test___init__(self):
        """Test the `__init__()` method."""

        m = TestModel1(_id=AN_OBJECT_ID, a=1, b=2)

        self.assertEqual(m._id, AN_OBJECT_ID)
        self.assertEqual(m._document['_id'], AN_OBJECT_ID)

        self.assertEqual(m.a, 1)
        self.assertEqual(m._document['a'], 1)

        self.assertEqual(m.b, 2)
        self.assertEqual(m._document['b'], 2)

    def test_all(self):
        """Test the `all()` method."""

        with mock.patch('simon.Model.find') as mock_find:
            TestModel1.all()

            mock_find.assert_called_with()

    def test_create(self):
        """Test the `create()` method."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel1._meta.db, 'insert'),
                    ) as (current_datetime, insert):
            current_datetime.return_value = 1

            TestModel1.create(d=1, e=2, f=3)

            insert.assert_called_with({'d': 1, 'e': 2, 'f': 3, 'created': 1,
                                       'modified': 1},
                                      safe=True)

    def test_create_required_fields(self):
        """Test the `create()` method with `required_fields`."""

        with mock.patch.object(TestModel5._meta.db, 'insert') as insert:
            TestModel5.create(a=1, b=2)

            insert.assert_called_with({'a': 1, 'b': 2}, safe=True)

            TestModel5.create(a=1, b=2, c=3)

            insert.assert_called_with({'a': 1, 'b': 2, 'c': 3}, safe=True)

    def test_create_required_fields_typeerror(self):
        ("Test that `create()` raises `TypeError` with "
         "`required_fields`.")

        with self.assertRaises(TypeError) as e:
            TestModel5.create()

        expected = ("The 'TestModel5' object cannot be created because it must"
                    " contain all of the required fields: a, b")
        actual = e.exception.message
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError):
            TestModel5.create(a=1)

        with self.assertRaises(TypeError):
            TestModel5.create(b=2)

        with self.assertRaises(TypeError):
            TestModel5.create(a=1, c=3)

        with self.assertRaises(TypeError):
            TestModel5.create(c=3)

    def test_delete(self):
        """Test the `delete()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1._meta.db, 'remove') as remove:
            m.delete()

            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=True)

    def test_delete_typeerror(self):
        """Test that `delete()` raises `TypeError`."""

        m = TestModel1(a=1, b=2)

        with self.assertRaises(TypeError):
            m.delete()

    def test_delete_write_concern(self):
        """Test that `delete()` respects write concern."""

        with mock.patch.object(TestModel3._meta.db, 'remove') as remove:
            m = TestModel3(_id=AN_OBJECT_ID)
            m.delete()
            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=False)

            # m needs to be given its _id again because delete() strips
            # it to prevent attempts to resave a deleted document
            m = TestModel3(_id=AN_OBJECT_ID)
            m.delete(safe=False)
            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=False)

            m = TestModel3(_id=AN_OBJECT_ID)
            m.delete(safe=True)
            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=True)

    def test_find(self):
        """Test the `find()` method."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            qs = TestModel1.find(id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID})

            self.assertIsInstance(qs, query.QuerySet)

    def test_find_comparison(self):
        """Test the `find()` method with comparison operators."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(a__gt=1)
            find.assert_called_with({'a': {'$gt': 1}})

            TestModel1.find(a__lte=1, b=2)
            find.assert_called_with({'a': {'$lte': 1}, 'b': 2})

    def test_find_embedded_document(self):
        """Test the `find()` method with an embedded document."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(a__b=1)

            find.assert_called_with({'a.b': 1})

    def test_find_id_string(self):
        """Test the `find()` method with a string `_id`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(_id=AN_OBJECT_ID_STR)

            find.assert_called_with({'_id': AN_OBJECT_ID})

    def test_find_sorted(self):
        """Test the `find()` method with a sort on the class."""

        with mock.patch('simon.base.QuerySet') as QuerySet:
            with mock.patch.object(TestModel1._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                TestModel1.find()
                QuerySet.sort.assert_not_called()

            with mock.patch.object(TestModel3._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                TestModel3.find()
                QuerySet().sort.assert_called_with('a')

            with mock.patch.object(TestModel4._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                TestModel4.find()
                QuerySet().sort.assert_called_with('-a')

    def test_find_with_q(self):
        """Test the `find()` method with `Q` objects."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(query.Q(a=1))
            find.assert_called_with({'a': 1})

            TestModel1.find(query.Q(a=1) | query.Q(b=2))
            find.assert_called_with({'$or': [{'a': 1}, {'b': 2}]})

            TestModel1.find(query.Q(a=1) & query.Q(b=2))
            find.assert_called_with({'$and': [{'a': 1}, {'b': 2}]})

    def test_get(self):
        """Test the `get()` method."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__getitem__.return_value = {'_id': AN_OBJECT_ID}
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            m = TestModel1.get(_id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID})

            self.assertEqual(m._document['_id'], AN_OBJECT_ID)

    def test_get_comparison(self):
        """Test the `get()` method with comparison operators."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__getitem__.return_value = {'_id': AN_OBJECT_ID}
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(a__gt=1)

            find.assert_called_with({'a': {'$gt': 1}})

            TestModel1.get(a__lte=1, b=2)

            find.assert_called_with({'a': {'$lte': 1}, 'b': 2})

    def test_get_embedded_document(self):
        """Test the `get()` method with an embedded document."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__getitem__.return_value = {'_id': AN_OBJECT_ID}
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(a__b=1)

            find.assert_called_with({'a.b': 1})

    def test_get_id_string(self):
        """Test the `get()` method with a string `_id`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__iter__ = [{'_id': AN_OBJECT_ID}]
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(_id=AN_OBJECT_ID_STR)

            find.assert_called_with({'_id': AN_OBJECT_ID})

    def test_get_multipledocumentsfound(self):
        """Test that `get()` raises `MultipleDocumentsFound`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.Mock()
            mock_query_set.count.return_value = 2

            find.return_value = mock_query_set

            with self.assertRaises(TestModel1.MultipleDocumentsFound):
                TestModel1.get(_id=AN_OBJECT_ID)

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

    def test_get_nodocumentfound(self):
        """Test that `get()` raises `NoDocumentFound`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.Mock()
            mock_query_set.count.return_value = 0

            find.return_value = mock_query_set

            with self.assertRaises(TestModel1.NoDocumentFound):
                TestModel1.get(_id=AN_OBJECT_ID)

    def test_get_with_q(self):
        """Test the `get()` method with `Q` objects."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__iter__ = [{'_id': AN_OBJECT_ID}]
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(query.Q(a=1))
            find.assert_called_with({'a': 1})

            TestModel1.get(query.Q(a=1) | query.Q(b=2))
            find.assert_called_with({'$or': [{'a': 1}, {'b': 2}]})

            TestModel1.get(query.Q(a=1) & query.Q(b=2))
            find.assert_called_with({'$and': [{'a': 1}, {'b': 2}]})

    def test_increment(self):
        """Test the `increment()` method."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            m = TestModel1(_id=AN_OBJECT_ID)

            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}
            m.increment('a')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a': 1}}, safe=True)

            self.assertEqual(m._document['a'], 1)

            find_one.return_value = {'_id': AN_OBJECT_ID, 'b': 2}
            m.increment('b', 2)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'b': 2}}, safe=True)

            self.assertEqual(m._document['a'], 1)
            self.assertEqual(m._document['b'], 2)

    def test_increment_embedded_document(self):
        """Test the `increment()` method with an embedded document."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'c': 3}}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.increment(a__c=3)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a.c': 3}}, safe=True)

            self.assertEqual(m._document['a']['c'], 3)

    def test_increment_field_map(self):
        """Test the `increment()` method with a name in `field_map`."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'real': 2}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.increment(fake=2)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'real': 2}}, safe=True)

            self.assertEqual(m._document['real'], 2)

    def test_increment_kwargs(self):
        """Test the `increment()` method with **kwargs."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 5}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.increment(a=1, b=5)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a': 1, 'b': 5}}, safe=True)

            self.assertEqual(m._document['a'], 1)
            self.assertEqual(m._document['b'], 5)

    def test_increment_typeerror(self):
        """Test that `increment()` raises `TypeError`."""

        m = TestModel1(a=1)

        with self.assertRaises(TypeError):
            m.increment('a')

    def test_increment_valueerror(self):
        """Test that `increment()` raises `ValueError`."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with self.assertRaises(ValueError):
            m.increment()

    def test_increment_write_concern(self):
        """Test that `increment()` respects write concern."""

        with nested(mock.patch.object(TestModel3._meta.db, 'find_one'),
                    mock.patch.object(TestModel3._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            m = TestModel3(_id=AN_OBJECT_ID)

            m.increment('a')
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a': 1}}, safe=False)

            # Because the return value of find_one() doesn't matter
            # beyond its one-time use, _id gets popped off, so for the
            # purposes of testing it needs to be put back
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            m.increment('a', safe=False)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a': 1}}, safe=False)

            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            m.increment('a', safe=True)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a': 1}}, safe=True)

    def test_raw_update(self):
        """Test the `raw_update()` method."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 2}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.raw_update({'$set': {'a': 1, 'b': 2}})

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1, 'b': 2}}, safe=True)

            self.assertEqual(m._document['a'], 1)
            self.assertEqual(m._document['b'], 2)

    def test_raw_update_typeerror(self):
        """Test that `raw_update()` raises `TypeError`."""

        m = TestModel1()

        with self.assertRaises(TypeError):
            m.raw_update({'a': 1})

    def test_raw_update_write_concern(self):
        """Test that `raw_update()` respects write concern."""

        with nested(mock.patch.object(TestModel3._meta.db, 'find_one'),
                    mock.patch.object(TestModel3._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 2}

            m = TestModel3(_id=AN_OBJECT_ID)
            m.raw_update({'$set': {'a': 1}})

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=False)

            # Because the return value of find_one() doesn't matter
            # beyond its one-time use, _id gets popped off, so for the
            # purposes of testing it needs to be put back
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 2}

            m.raw_update({'$set': {'a': 1}}, safe=False)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=False)

            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 2}

            m.raw_update({'$set': {'a': 1}}, safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=True)

    def test_remove_fields_multiple(self):
        """Test the `remove_fields()` method for multiple fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID, a=1, b=2)
            m.remove_fields(('a', 'b'))

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'a': True, 'b': True}},
                                      safe=True)

            self.assertNotIn('a', m._document)
            self.assertNotIn('b', m._document)

    def test_remove_fields_nested_multiple(self):
        """Test the `remove_fields()` method with nested fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID, b=1)
            m.c = {'d': 2, 'e': 3}
            m.remove_fields(('b', 'c__e'))

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'b': True, 'c.e': True}},
                                      safe=True)

            self.assertNotIn('b', m._document)
            self.assertIn('d', m._document['c'])
            self.assertNotIn('e', m._document['c'])

    def test_remove_fields_nested_one(self):
        """Test the `remove_fields()` method with nested fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.f = {'g': 1, 'h': 2}
            m.remove_fields('f__h')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'f.h': True}}, safe=True)

            self.assertIn('g', m._document['f'])
            self.assertNotIn('h', m._document['f'])

    def test_remove_fields_one(self):
        """Test the `remove_fields()` method for one field."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID, a=1, b=2)
            m.remove_fields('b')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'b': True}}, safe=True)

            self.assertIn('a', m._document)
            self.assertNotIn('b', m._document)

    def test_remove_fields_required_fields(self):
        """Test the `remove_fields()` method with `required_fields`."""

        with mock.patch.object(TestModel5._meta.db, 'update') as update:
            m = TestModel5(_id=AN_OBJECT_ID, a=1, b=2, c=3)
            m.remove_fields('c')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'c': 1}}, safe=True)

    def test_remove_fields_required_fields_typeerror(self):
        ("Test that `remove_fields()` raises `TypeError` with "
         "`required_fields`.")

        m = TestModel5(_id=AN_OBJECT_ID, a=1, b=2)

        with self.assertRaises(TypeError) as e:
            m.remove_fields(('a', 'b'))

        expected = ("The 'TestModel5' object cannot be updated because it must"
                    " contain all of the required fields: a, b")
        actual = e.exception.message
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError):
            m.remove_fields('a')

        with self.assertRaises(TypeError):
            m.remove_fields('b')

    def test_remove_fields_typeerror(self):
        """Test that `remove_fields()` raises `TypeError`."""

        m = TestModel1(a=1, b=2)

        with self.assertRaises(TypeError):
            m.remove_fields('a')

    def test_remove_fields_write_concern(self):
        """Test that `remove_fields()` respects write concern."""

        with mock.patch.object(TestModel3._meta.db, 'update') as update:
            m = TestModel3(_id=AN_OBJECT_ID, a=1)
            m.remove_fields('a')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'a': 1}}, safe=False)

            m = TestModel3(_id=AN_OBJECT_ID, a=1)
            m.remove_fields('a', safe=False)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'a': 1}}, safe=False)

            m = TestModel3(_id=AN_OBJECT_ID, a=1)
            m.remove_fields('a', safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'a': 1}}, safe=True)

    def test_save_field_map(self):
        """Test the `save()` method with a name in `field_map`."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel1._meta.db, 'insert'),
                    ) as (current_datetime, insert):
            current_datetime.return_value = 1

            insert.return_value = 1

            m = TestModel1(fake=1)
            m.save()

            insert.assert_called_with({'real': 1, 'created': 1, 'modified': 1},
                                      safe=True)

            self.assertEqual(m._document['_id'], 1)

    def test_save_fields_field_map(self):
        """Test the `save_fields()` method with a name in `field_map`."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.fake = 1
            m.save_fields('fake')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'real': 1}}, safe=True)

    def test_save_fields_multiple(self):
        """Test the `save_fields()` method for multiple fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.a = 1
            m.b = 2
            m.save_fields(('a', 'b'))

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1, 'b': 2}}, safe=True)

    def test_save_fields_nested_field(self):
        """Test the `save_fields()` method with a nested field."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.f = {'g': 1}
            m.save_fields('f__g')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'f.g': 1}}, safe=True)

    def test_save_fields_one(self):
        """Test the `save_fields()` method for one field."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.a = 1
            m.b = 2
            m.save_fields('b')

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'b': 2}}, safe=True)

    def test_save_fields_attributeerror(self):
        """Test that `save_fields()` raises `AttributeError`."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with self.assertRaises(AttributeError):
            m.save_fields('field_that_doesnt_exist')

    def test_save_fields_typeerror(self):
        """Test that `save_fields()` raises `TypeError`."""

        m = TestModel1(a=1, b=2)

        with self.assertRaises(TypeError):
            m.save_fields('a')

    def test_save_fields_write_concern(self):
        """Test that `save_fields()` respects write concern."""

        with mock.patch.object(TestModel3._meta.db, 'update') as update:
            m = TestModel3(_id=AN_OBJECT_ID, a=1)

            m.save_fields('a')
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=False)

            m.save_fields('a', safe=False)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=False)

            m.save_fields('a', safe=True)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=True)

    def test_save_insert(self):
        """Test the `save()` method for new documents."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel1._meta.db, 'insert'),
                    ) as (current_datetime, insert):
            current_datetime.return_value = 1

            m = TestModel1(a=1, b=2)
            m.save()

            insert.assert_called_with({'a': 1, 'b': 2, 'created': 1,
                                       'modified': 1},
                                      safe=True)

    def test_save_insert_required_fields(self):
        ("Test the `save()` method with `required_fields` for new "
         "documents.")

        with mock.patch.object(TestModel5._meta.db, 'insert') as insert:
            m = TestModel5(a=1, b=2)
            m.save()

            insert.assert_called_with({'a': 1, 'b': 2}, safe=True)

            m = TestModel5(a=1, b=2, c=3)
            m.save()

            insert.assert_called_with({'a': 1, 'b': 2, 'c': 3}, safe=True)

    def test_save_insert_required_fields_typeerror(self):
        ("Test that `save()` raises `TypeError` with "
         "`required_fields` for new documents.")

        m = TestModel5(a=1)

        with self.assertRaises(TypeError) as e:
            m.save()

        expected = ("The 'TestModel5' object cannot be saved because it must"
                    " contain all of the required fields: a, b")
        actual = e.exception.message
        self.assertEqual(actual, expected)

        m = TestModel5(b=2)

        with self.assertRaises(TypeError):
            m.save()

        m = TestModel5(c=3)

        with self.assertRaises(TypeError):
            m.save()

    def test_save_required_fields(self):
        """Test the `save()` method with `required_fields`."""

        with mock.patch.object(TestModel5._meta.db, 'update') as update:
            m = TestModel5(_id=AN_OBJECT_ID, a=1, b=2)
            m.save()

            update.assert_called_with({'_id': AN_OBJECT_ID}, {'a': 1, 'b': 2},
                                      safe=True)

            m = TestModel5(_id=AN_OBJECT_ID, a=1, b=2, c=3)
            m.save()

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'a': 1, 'b': 2, 'c': 3}, safe=True)

    def test_save_required_fields_typeerror(self):
        ("Test that `save()` raises `TypeError` with "
         "`required_fields`.")

        m = TestModel5(_id=AN_OBJECT_ID, a=1)

        with self.assertRaises(TypeError) as e:
            m.save()

        expected = ("The 'TestModel5' object cannot be saved because it must"
                    " contain all of the required fields: a, b")
        actual = e.exception.message
        self.assertEqual(actual, expected)

        m = TestModel5(_id=AN_OBJECT_ID, b=2)

        with self.assertRaises(TypeError):
            m.save()

        m = TestModel5(_id=AN_OBJECT_ID, c=3)

        with self.assertRaises(TypeError):
            m.save()

    def test_save_timestamps(self):
        """Test that `save()` properly handles adding timestamps."""

        with mock.patch.object(TestModel1._meta.db, 'insert') as insert:
            m1 = TestModel1()
            m1.save()

            self.assertIn('created', m1._document)
            self.assertIn('modified', m1._document)

            self.assertIsInstance(m1._document['created'], datetime)
            self.assertIsInstance(m1._document['modified'], datetime)

        with mock.patch.object(TestModel2._meta.db, 'insert') as insert:
            m2 = TestModel2(a=2)
            m2.save()

            insert.assert_called_with({'a': 2}, safe=True)

            self.assertNotIn('created', m2._document)
            self.assertNotIn('modified', m2._document)

    def test_save_update(self):
        """Test the `save()` method for existing documents."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (current_datetime, update):
            current_datetime.return_value = 1

            m = TestModel1(_id=AN_OBJECT_ID, created=1, modified=0)
            m.c = 3
            m.save()

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'c': 3, 'created': 1, 'modified': 1},
                                      safe=True)

    def test_save_write_concern(self):
        """Test that `save()` respects write concern."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel3._meta.db, 'update'),
                    ) as (current_datetime, update):
            current_datetime.return_value = 1

            m = TestModel3(_id=AN_OBJECT_ID)

            m.save()
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'created': 1, 'modified': 1},
                                      safe=False)

            m.save(safe=False)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'created': 1, 'modified': 1},
                                      safe=False)

            m.save(safe=True)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'created': 1, 'modified': 1},
                                      safe=True)

    def test_update(self):
        """Test the `update()` method."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 2, 'b': 3}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.update(a=2, b=3)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 2, 'b': 3}}, safe=True)

            self.assertEqual(m._document['a'], 2)
            self.assertEqual(m._document['b'], 3)

    def test_update_field_map(self):
        """Test the `update()` method with a name in `field_map`."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'real': 1}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.update(fake=1)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'real': 1}}, safe=True)

            self.assertEqual(m._document['real'], 1)

    def test_update_nested_field(self):
        """Test the `update()` method with a nested field."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'c': {'d': 1}}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.update(c__d=1)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'c.d': 1}}, safe=True)

            self.assertEqual(m._document['c'], {'d': 1})

    def test_update_typeerror(self):
        """Test that `update()` raises `TypeError`."""

        m = TestModel1()

        with self.assertRaises(TypeError):
            m.update(a=1)

    def test_update_write_concern(self):
        """Test that `update()` respects write concern."""

        with nested(mock.patch.object(TestModel3._meta.db, 'find_one'),
                    mock.patch.object(TestModel3._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            m = TestModel3(_id=AN_OBJECT_ID)

            m.update(a=1)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=False)

            m.update(a=1, safe=False)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=False)

            m.update(a=1, safe=True)
            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1}}, safe=True)
