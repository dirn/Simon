"""Tests of the database functionality

These tests require a connection to a MongoDB instance.

Most of these tests will use safe mode when updating the database. This
will cause the tests to take longer, but will provide more reliable
tests. The only time safe mode will be turned off is when a method's
behavior is different for each mode. When that is the case, both modes
will be tested.
"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import collections
from contextlib import nested
from datetime import datetime

from bson import ObjectId
import mock
from pymongo.collection import Collection
from pymongo.cursor import Cursor

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
        sort = 'a'


class TestModel4(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        sort = '-a'


class TestDatabase(unittest.TestCase):
    """Test database interaction"""

    @classmethod
    def setUpClass(cls):
        cls.connection = connection.connect('localhost', name='test-simon')

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

            TestModel1.create(d=1, e=2, f=3, safe=True)

            insert.assert_called_with({'d': 1, 'e': 2, 'f': 3, 'created': 1,
                                       'modified': 1},
                                      safe=True)

    def test_db_attribute(self):
        ("Test that the `db` attribute of classes and instances is the "
         "right type.")

        self.assertTrue(isinstance(TestModel1._meta.db, Collection))

        m = TestModel1()
        self.assertTrue(isinstance(m._meta.db, Collection))

    def test_delete(self):
        """Test the `delete()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1._meta.db, 'remove') as remove:
            m.delete(safe=True)

            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=True)

    def test_delete_typeerror(self):
        """Test that `delete()` raises `TypeError`."""

        m = TestModel1(a=1, b=2)

        with self.assertRaises(TypeError):
            m.delete()

    def test_find(self):
        """Test the `find()` method."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            qs = TestModel1.find(id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID})

            self.assertTrue(isinstance(qs, query.QuerySet))

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

            m, created = TestModel1.get_or_create(_id=AN_OBJECT_ID,
                                                  safe=True)

            get.assert_called_with(_id=AN_OBJECT_ID)
            create.assert_called_with(_id=AN_OBJECT_ID, safe=True)

            self.assertTrue(created)

    def test_get_or_create_get(self):
        """Test the `get_or_create()` method for getting documents."""

        with mock.patch.object(TestModel1, 'get') as get:
            get.return_value = mock.Mock()

            m, created = TestModel1.get_or_create(_id=AN_OBJECT_ID, safe=True)

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
            m.increment('a', safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a': 1}}, safe=True)

            self.assertTrue(m._document['a'], 1)

            find_one.return_value = {'_id': AN_OBJECT_ID, 'b': 2}
            m.increment('b', 2, safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'b': 2}}, safe=True)

            self.assertTrue(m._document['a'], 1)
            self.assertTrue(m._document['b'], 2)

    def test_increment_embedded_document(self):
        """Test the `increment()` method with an embedded document."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'c': 3}}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.increment(a__c=3, safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a.c': 3}}, safe=True)

            self.assertTrue(m._document['a']['c'], 3)

    def test_increment_field_map(self):
        """Test the `increment()` method with a name in `field_map`."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'real': 2}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.increment(fake=2, safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'real': 2}}, safe=True)

            self.assertTrue(m._document['real'], 2)

    def test_increment_kwargs(self):
        """Test the `increment()` method with **kwargs."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 5}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.increment(a=1, b=5, safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$inc': {'a': 1, 'b': 5}}, safe=True)

            self.assertTrue(m._document['a'], 1)
            self.assertTrue(m._document['b'], 5)

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

    def test_raw_update(self):
        """Test the `raw_update()` method."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 2}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.raw_update({'$set': {'a': 1, 'b': 2}}, safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1, 'b': 2}}, safe=True)

            self.assertEqual(m._document['a'], 1)
            self.assertEqual(m._document['b'], 2)

    def test_raw_update_typeerror(self):
        """Test that `raw_update()` raises `TypeError`."""

        m = TestModel1()

        with self.assertRaises(TypeError):
            m.raw_update({'a': 1})

    def test_remove_fields_multiple(self):
        """Test the `remove_fields()` method for multiple fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID, a=1, b=2)
            m.remove_fields(('a', 'b'), safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'a': True, 'b': True}},
                                      safe=True)

            self.assertFalse('a' in m._document)
            self.assertFalse('b' in m._document)

    def test_remove_fields_nested_multiple(self):
        """Test the `remove_fields()` method with nested fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID, b=1)
            m.c = {'d': 2, 'e': 3}
            m.remove_fields(('b', 'c__e'), safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'b': True, 'c.e': True}},
                                      safe=True)

            self.assertFalse('b' in m._document)
            self.assertTrue('d' in m._document['c'])
            self.assertFalse('e' in m._document['c'])

    def test_remove_fields_nested_one(self):
        """Test the `remove_fields()` method with nested fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.f = {'g': 1, 'h': 2}
            m.remove_fields('f__h', safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'f.h': True}}, safe=True)

            self.assertTrue('g' in m._document['f'])
            self.assertFalse('h' in m._document['f'])

    def test_remove_fields_one(self):
        """Test the `remove_fields()` method for one field."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID, a=1, b=2)
            m.remove_fields('b', safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$unset': {'b': True}}, safe=True)

            self.assertTrue('a' in m._document)
            self.assertFalse('b' in m._document)

    def test_remove_fields_typeerror(self):
        """Test that `remove_fields()` raises `TypeError`."""

        m = TestModel1(a=1, b=2)

        with self.assertRaises(TypeError):
            m.remove_fields('a')

    def test_save_field_map(self):
        """Test the `save()` method with a name in `field_map`."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel1._meta.db, 'insert'),
                    ) as (current_datetime, insert):
            current_datetime.return_value = 1

            insert.return_value = 1

            m = TestModel1(fake=1)
            m.save(safe=True)

            insert.assert_called_with({'real': 1, 'created': 1, 'modified': 1},
                                      safe=True)

            self.assertEqual(m._document['_id'], 1)

    def test_save_fields_field_map(self):
        """Test the `save_fields()` method with a name in `field_map`."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.fake = 1
            m.save_fields('fake', safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'real': 1}}, safe=True)

    def test_save_fields_multiple(self):
        """Test the `save_fields()` method for multiple fields."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.a = 1
            m.b = 2
            m.save_fields(('a', 'b'), safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'a': 1, 'b': 2}}, safe=True)

    def test_save_fields_nested_field(self):
        """Test the `save_fields()` method with a nested field."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.f = {'g': 1}
            m.save_fields('f__g', safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'f.g': 1}}, safe=True)

    def test_save_fields_one(self):
        """Test the `save_fields()` method for one field."""

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m = TestModel1(_id=AN_OBJECT_ID)
            m.a = 1
            m.b = 2
            m.save_fields('b', safe=True)

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

    def test_save_insert(self):
        """Test the `save()` method for new documents."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel1._meta.db, 'insert'),
                    ) as (current_datetime, insert):
            current_datetime.return_value = 1

            m = TestModel1(a=1, b=2)
            m.save(safe=True)

            insert.assert_called_with({'a': 1, 'b': 2, 'created': 1,
                                       'modified': 1},
                                      safe=True)

    def test_save_timestamps(self):
        """Test that `save()` properly handles adding timestamps."""

        with mock.patch.object(TestModel1._meta.db, 'insert') as insert:
            m1 = TestModel1()
            m1.save()

            self.assertTrue('created' in m1._document)
            self.assertTrue('modified' in m1._document)

            self.assertTrue(isinstance(m1._document['created'], datetime))
            self.assertTrue(isinstance(m1._document['modified'], datetime))

        with mock.patch.object(TestModel2._meta.db, 'insert') as insert:
            m2 = TestModel2(a=2)
            m2.save(safe=True)

            insert.assert_called_with({'a': 2}, safe=True)

            self.assertFalse('created' in m2._document)
            self.assertFalse('modified' in m2._document)

    def test_save_update(self):
        """Test the `save()` method for existing documents."""

        with nested(mock.patch('simon.base.current_datetime'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (current_datetime, update):
            current_datetime.return_value = 1

            m = TestModel1(_id=AN_OBJECT_ID, created=1, modified=0)
            m.c = 3
            m.save(safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'c': 3, 'created': 1, 'modified': 1},
                                      safe=True)

    def test_update(self):
        """Test the `update()` method."""

        with nested(mock.patch.object(TestModel1._meta.db, 'find_one'),
                    mock.patch.object(TestModel1._meta.db, 'update'),
                    ) as (find_one, update):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 2, 'b': 3}

            m = TestModel1(_id=AN_OBJECT_ID)
            m.update(a=2, b=3, safe=True)

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
            m.update(fake=1, safe=True)

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
            m.update(c__d=1, safe=True)

            update.assert_called_with({'_id': AN_OBJECT_ID},
                                      {'$set': {'c.d': 1}}, safe=True)

            self.assertEqual(m._document['c'], {'d': 1})

    def test_update_typeerror(self):
        """Test that `update()` raises `TypeError`."""

        m = TestModel1()

        with self.assertRaises(TypeError):
            m.update(a=1)


class TestQuery(unittest.TestCase):
    """Test :class:`~simon.query.QuerySet` functionality"""

    @classmethod
    def setUpClass(cls):
        cls.connection = connection.connect('localhost', name='test-simon')

    def test_count(self):
        """Test the `count()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)
        qs.count()

        cursor.count.assert_called_with(with_limit_and_skip=True)

        # cursor.count() should get cached as qs._count, so it should
        # only be called once by qs.count()
        qs.count()
        cursor.count.assert_not_called()

    def test_count_typeerror(self):
        """Test that `count()` raises `TypeError`."""

        qs = query.QuerySet()
        with self.assertRaises(TypeError):
            qs.count()

    def test_distinct(self):
        """Test the `distinct()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)
        qs.distinct('a')

        cursor.distinct.assert_called_with('a')

    def test_limit(self):
        """Test the `limit()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.limit(1)
        cursor.clone.assert_called_with()
        cursor.clone().limit.assert_called_with(1)

        qs.limit(2)
        cursor.clone.assert_called_with()
        cursor.clone().limit.assert_called_with(2)

    def test_skip(self):
        """Test the `skip()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.skip(1)
        cursor.clone.assert_called_with()
        cursor.clone().skip.assert_called_with(1)

        qs.skip(2)
        cursor.clone.assert_called_with()
        cursor.clone().skip.assert_called_with(2)

    def test_sort(self):
        """Test the `sort()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('_id')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('_id', 1)])

        qs.sort('-_id')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('_id', -1)])

    def test_sort_multiple_ascending(self):
        """Test the `sort()` method for multiple ascending keys."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('a', 'b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', 1), ('b', 1)])

    def test_sort_multiple_descending(self):
        """Test the `sort()` method for multiple descending keys."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('-a', '-b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', -1), ('b', -1)])

    def test_sort_multiple_ascending_then_descending(self):
        """Test the `sort()` method for multiple keys ascending first."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('a', '-b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', 1), ('b', -1)])

    def test_sort_multiple_descending_then_ascending(self):
        """Test the `sort()` method for multiple keys descending first."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        qs.sort('-a', 'b')
        cursor.clone.assert_called_with()
        cursor.clone().sort.assert_called_with([('a', -1), ('b', 1)])

    def test__fill_to(self):
        """Test the `_fill_to()` method."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(2)

        self.assertEqual(len(qs._items), 3)

    def test__fill_to_as_documents(self):
        """Test that `_fill_to()` stores documents."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.next.return_value = {'_id': AN_OBJECT_ID}

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(0)

        self.assertTrue(isinstance(qs._items[0], dict))

    def test__fill_to_as_model(self):
        """Test that `_fill_to()` stores model instances."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.next.return_value = {'_id': AN_OBJECT_ID}

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)

        qs._fill_to(0)

        self.assertTrue(isinstance(qs._items[0], TestModel1))

    def test__fill_to_indexes(self):
        ("Test that `_fill_to()` property fills to the specified "
         "index.")

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        for x in range(3):
            qs._fill_to(x)
            self.assertEqual(len(qs._items), x + 1)

    def test__fill_to_overfill(self):
        ("Test that `_fill_to()` correctly handles indexes greater than"
         " the maximum index of the result cache.")

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(3)

        self.assertEqual(len(qs._items), 3)

    def test__fill_to_twice(self):
        """Test that `_fill_to()` can be called multiple times."""

        cursor = mock.MagicMock(spec=Cursor)
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        qs._fill_to(0)
        self.assertEqual(len(qs._items), 1)

        qs._fill_to(0)
        self.assertEqual(len(qs._items), 1)

        qs._fill_to(3)
        self.assertEqual(len(qs._items), 3)

        qs._fill_to(3)
        self.assertEqual(len(qs._items), 3)

    def test___getitem__(self):
        """Test the `__getitem__()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        # qs._fill_to() would normally populate qs._items
        qs._items = range(3)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            for x in range(3):
                self.assertEqual(qs[x], qs._items[x])
                _fill_to.assert_called_with(x)

    def test___getitem___slice(self):
        """Test the `__getitem__()` method with slices."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        # qs._fill_to() would normally populate qs._items
        qs._items = range(3)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            self.assertEqual(qs[1:], qs._items[1:])
            _fill_to.assert_called_with(2)

            self.assertEqual(qs[:1], qs._items[:1])
            _fill_to.assert_called_with(0)

            self.assertEqual(qs[1:2], qs._items[1:2])
            _fill_to.assert_called_with(1)

            self.assertEqual(qs[::2], qs._items[::2])
            _fill_to.assert_called_with(2)

            self.assertEqual(qs[1::2], qs._items[1::2])
            _fill_to.assert_called_with(2)

            self.assertEqual(qs[::], qs._items[::])
            _fill_to.assert_called_with(2)

    def test___getitem___indexerror(self):
        """Test that `__getitem__()` raises `IndexError`."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor, cls=TestModel1)

        with self.assertRaises(IndexError) as e:
            qs[3]

        expected = "No such item in 'QuerySet' for 'TestModel1' object"
        self.assertEqual(e.exception.message, expected)

        qs = query.QuerySet(cursor=cursor)

        with self.assertRaises(IndexError) as e:
            qs[3]

        expected = "No such item in 'QuerySet'"
        self.assertEqual(e.exception.message, expected)

    def test___getitem___typeerror(self):
        """Test that `__getitem__()` raises `TypeError`."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        with self.assertRaises(TypeError):
            qs[-1]

    def test___iter__(self):
        """Test the `__iter__()` method."""

        cursor = mock.Mock()

        qs = query.QuerySet(cursor=cursor)

        self.assertTrue(isinstance(qs.__iter__(), collections.Iterable))

    def test___iter___fills_cache(self):
        """Test that `__iter__()` fills the result cache."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        def append_to_cache(v):
            qs._items.append(v)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            _fill_to.side_effect = append_to_cache

            i = 0
            for x in qs:
                _fill_to.assert_called_with(i)
                i += 1

        self.assertEqual(len(qs._items), 3)

    def test__iter___fills_cache_partial(self):
        """Test that `__iter__()` fills the rest of the result cache."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)
        qs._items = [0]

        def append_to_cache(v):
            qs._items.append(v)

        with mock.patch.object(qs, '_fill_to') as _fill_to:
            _fill_to.side_effect = append_to_cache

            i = 0
            for x in qs:
                if i == 0:
                    # qs._fill_to(0) will already have been called
                    _fill_to.assert_not_called()
                else:
                    _fill_to.assert_called_with(i)
                i += 1

        self.assertEqual(len(qs._items), 3)

    def test___len__(self):
        """Test the `__len__()` method."""

        cursor = mock.Mock()
        cursor.count.return_value = 3

        qs = query.QuerySet(cursor=cursor)

        self.assertEqual(len(qs), cursor.count())
