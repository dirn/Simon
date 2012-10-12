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
import mock

from simon import MongoModel, connection, query


class TestModel(MongoModel):
    class Meta:
        collection = 'test-simon'


class TestDatabase(unittest.TestCase):
    """Test database interaction"""

    @classmethod
    def setUpClass(cls):
        cls.connection = connection.connect('localhost', name='test-simon')

    @classmethod
    def tearDownClass(cls):
        cls.connection.drop_database('test-simon')

    def setUp(self):
        self.database = self.__class__.connection['test-simon']
        self.collection = self.database['test-simon']

        self._id = self.collection.insert({'a': 1, 'b': 2}, safe=True)

    def tearDown(self):
        self.database.drop_collection('test-simon')

    def test___init__(self):
        """Test the `__init__()` method."""

        doc = self.collection.find_one({'_id': self._id})

        m = TestModel(**doc)

        for k, v in doc.items():
            self.assertTrue(hasattr(m, k))
            self.assertEqual(m._document[k], v)

    def test_get(self):
        """Test the `get()` method."""

        m = TestModel.get(id=self._id)

        self.assertEqual(m.id, self._id)

        self.assertEqual(m._document['_id'], self._id)
        self.assertEqual(m._document['a'], 1)
        self.assertEqual(m._document['b'], 2)

    def test_get_multipledocumentsfound(self):
        """Test that `get()` raises `MultipleDocumentsFound`."""

        self.collection.insert({'a': 1})

        with self.assertRaises(TestModel.MultipleDocumentsFound):
            TestModel.get(a=1)

    def test_get_nodocumentfound(self):
        """Test that `get()` raises `NoDocumentFound`."""

        with self.assertRaises(TestModel.NoDocumentFound):
            TestModel.get(a=2)

    def test_remove(self):
        """Test the `remove()` method."""

        doc = self.collection.find_one({'_id': self._id})

        m = TestModel(**doc)

        m.remove(safe=True)

        doc = self.collection.find_one({'_id': self._id})

        self.assertIsNone(doc)

    def test_remove_typeerror(self):
        """Test that `remove()` raises `TypeError`."""

        m = TestModel(a=1, b=2)

        with self.assertRaises(TypeError):
            m.remove()

    def test_remove_fields_typeerror(self):
        """Test that `remove_fields()` raises `TypeError`."""

        m = TestModel(a=1, b=2)

        with self.assertRaises(TypeError):
            m.remove_fields('a')

    def test_remove_fields_multiple(self):
        """Test the `remove_fields()` method for multiple fields."""

        doc = self.collection.find_one({'_id': self._id})

        m = TestModel(**doc)

        m.remove_fields(('a', 'b'), safe=True)

        self.assertFalse('a' in m._document)
        self.assertFalse('b' in m._document)

        doc = self.collection.find_one({'_id': self._id})

        self.assertFalse('a' in doc)
        self.assertFalse('b' in doc)

    def test_remove_fields_one(self):
        """Test the `remove_fields()` method for one field."""

        doc = self.collection.find_one({'_id': self._id})

        m = TestModel(**doc)

        m.remove_fields('b', safe=True)

        self.assertTrue('a' in m._document)
        self.assertFalse('b' in m._document)

        doc = self.collection.find_one({'_id': self._id})

        self.assertTrue('a' in doc)
        self.assertFalse('b' in doc)

    def test_save_fields_multiple(self):
        """Test the `save_fields()` method for multiple fields."""

        doc = self.collection.find_one({'_id': self._id})

        m = TestModel(**doc)

        m.a = 3
        m.b = 4
        m.save_fields(('a', 'b'), safe=True)

        doc = self.collection.find_one({'_id': self._id})

        self.assertEqual(m._document['a'], doc['a'])
        self.assertEqual(m._document['b'], doc['b'])

    def test_save_fields_one(self):
        """Test the `save_fields()` method for one field."""

        doc = self.collection.find_one({'_id': self._id})

        m = TestModel(**doc)

        m.a = 3
        m.b = 4
        m.save_fields('b', safe=True)

        doc = self.collection.find_one({'_id': self._id})

        self.assertNotEqual(m._document['a'], doc['a'])
        self.assertEqual(m._document['b'], doc['b'])

    def test_save_fields_typeerror(self):
        """Test that `save_fields()` raises `TypeError`."""

        m = TestModel(a=1, b=2)

        with self.assertRaises(TypeError):
            m.save_fields('a')

    def test_save_insert(self):
        """Test the `save()` method for new documents."""

        m = TestModel(a=1, b=2)
        m.save(safe=True)

        doc = self.collection.find_one({'_id': m.id})

        self.assertEqual(m.id, doc['_id'])
        self.assertEqual(sorted(m._document), sorted(doc))
        self.assertTrue(hasattr(m, 'created'))
        self.assertTrue(hasattr(m, 'modified'))

    def test_save_update(self):
        """Test the `save()` method for existing documents."""

        # save() should also update the modified date. While this test
        # could simply test that modified is added to a document that
        # lacks it, it's a more complete test if it actually checks
        # for a changed value, so add it to the document in the
        # database.
        self.collection.update({'_id': self._id}, {'$set': {'modified': 1}})

        doc = self.collection.find_one({'_id': self._id})

        m = TestModel()
        m._document = doc.copy()

        m.c = 3
        m.save(safe=True)

        doc['c'] = 3

        for k, v in doc.items():
            if k != 'modified':
                self.assertEqual(m._document[k], v)
            else:
                self.assertNotEqual(m._document[k], v)

    def test_save_upsert(self):
        """Test the `save()` method for upserting documents."""

        m = TestModel(a=1, b=2)
        m.save(safe=True, upsert=True)

        self.assertTrue(hasattr(m, 'id'))
        self.assertTrue(hasattr(m, 'created'))
        self.assertTrue(hasattr(m, 'modified'))
        self.assertEqual(m.a, 1)
        self.assertEqual(m.b, 2)

    def test_save_upsert_unsafe(self):
        ("Test the `save()` method for upserting documents not in safe "
         "mode.")

        m = TestModel(a=1, b=2)
        m.save(safe=False, upsert=True)

        self.assertTrue(hasattr(m, 'id'))
        self.assertTrue(hasattr(m, 'created'))
        self.assertTrue(hasattr(m, 'modified'))
        self.assertEqual(m.a, 1)
        self.assertEqual(m.b, 2)


class TestQuery(unittest.TestCase):
    """Test :class:`~simon.query.QuerySet` functionality"""

    @classmethod
    def setUpClass(cls):
        cls.connection = connection.connect('localhost', name='test-simon')

    @classmethod
    def tearDownClass(cls):
        cls.connection.drop_database('test-simon')

    def setUp(self):
        self.database = self.__class__.connection['test-simon']
        self.collection = self.database['test-simon']

        self._id1 = self.collection.insert({'a': 1, 'b': 2}, safe=True)
        self._id2 = self.collection.insert({'a': 2, 'c': 1}, safe=True)
        self._id3 = self.collection.insert({'b': 1, 'c': 2}, safe=True)

        self.cursor = self.collection.find()
        self.qs = query.QuerySet(cursor=self.cursor, cls=TestModel)

    def tearDown(self):
        self.database.drop_collection('test-simon')

    def test_count(self):
        """Test the `count()` method."""

        self.assertEqual(self.qs.count(), self.cursor.count())
        self.assertEqual(self.qs._count, self.cursor.count())

    def test_distinct(self):
        """Test the `distinct()` method."""

        self.assertEqual(set(self.qs.distinct('a')), set([1, 2]))
        self.assertEqual(set(self.qs.distinct('b')), set([1, 2]))
        self.assertEqual(set(self.qs.distinct('c')), set([1, 2]))

    def test_limit(self):
        """Test the `limit()` method."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        limit1 = self.qs.limit(1)
        limit2 = self.qs.limit(2)
        limit3 = self.qs.limit(3)

        # Fill the result caches, using a bigger number to ensure
        # everything gets loaded
        limit1._fill_to(3)
        limit2._fill_to(3)
        limit3._fill_to(3)

        doc1 = {'_id': self._id1, 'a': 1, 'b': 2}
        doc2 = {'_id': self._id2, 'a': 2, 'c': 1}
        doc3 = {'_id': self._id3, 'b': 1, 'c': 2}

        self.assertTrue(doc1 in limit1._items)
        self.assertFalse(doc2 in limit1._items)
        self.assertFalse(doc3 in limit1._items)

        self.assertTrue(doc1 in limit2._items)
        self.assertTrue(doc2 in limit2._items)
        self.assertFalse(doc3 in limit2._items)

        self.assertTrue(doc1 in limit3._items)
        self.assertTrue(doc2 in limit3._items)
        self.assertTrue(doc3 in limit3._items)

    def test_limit_count(self):
        """Test that `limit()` correctly handles counts."""

        limit1 = self.qs.limit(1)
        limit2 = self.qs.limit(2)
        limit3 = self.qs.limit(3)

        self.assertEqual(limit1.count(), 1)
        self.assertEqual(limit2.count(), 2)
        self.assertEqual(limit3.count(), 3)

    def test_skip(self):
        """Test the `skip()` method."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        skip1 = self.qs.skip(1)
        skip2 = self.qs.skip(2)
        skip3 = self.qs.skip(3)

        # Fill the result caches, using a bigger number to ensure
        # everything gets loaded
        skip1._fill_to(3)
        skip2._fill_to(3)
        skip3._fill_to(3)

        doc1 = {'_id': self._id1, 'a': 1, 'b': 2}
        doc2 = {'_id': self._id2, 'a': 2, 'c': 1}
        doc3 = {'_id': self._id3, 'b': 1, 'c': 2}

        self.assertFalse(doc1 in skip1._items)
        self.assertTrue(doc2 in skip1._items)
        self.assertTrue(doc3 in skip1._items)

        self.assertFalse(doc1 in skip2._items)
        self.assertFalse(doc2 in skip2._items)
        self.assertTrue(doc3 in skip2._items)

        self.assertFalse(doc1 in skip3._items)
        self.assertFalse(doc2 in skip3._items)
        self.assertFalse(doc3 in skip3._items)

    def test_skip_count(self):
        """Test that `skip()` correctly handles counts."""

        skip1 = self.qs.skip(1)
        skip2 = self.qs.skip(2)
        skip3 = self.qs.skip(3)

        self.assertEqual(skip1.count(), 2)
        self.assertEqual(skip2.count(), 1)
        self.assertEqual(skip3.count(), 0)

    def test_sort_single_ascending(self):
        """Test the `sort()` method for a single ascending key."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        qs = self.qs.sort('a')

        # Fill the result cache, using a bigger number to ensure
        # everything gets loaded
        qs._fill_to(3)

        # Documents without the key should appear first, followed by
        # documents with the key with its value in ascending order
        self.assertEqual(qs[0]['_id'], self._id3)
        self.assertEqual(qs[1]['_id'], self._id1)
        self.assertEqual(qs[2]['_id'], self._id2)

    def test_sort_single_descending(self):
        """Test the `sort()` method for a single descending key."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        qs = self.qs.sort('-a')

        # Fill the result cache, using a bigger number to ensure
        # everything gets loaded
        qs._fill_to(3)

        # Documents with the key should appear first, with the value
        # in descending order, following by documents without the key
        self.assertEqual(qs[2]['_id'], self._id3)
        self.assertEqual(qs[1]['_id'], self._id1)
        self.assertEqual(qs[0]['_id'], self._id2)

    def test_sort_multiple_ascending(self):
        """Test the `sort()` method for multiple ascending keys."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        # In order to really test this, some new documents are needed
        # because all current key/value combinations used are unique
        self._id4 = self.collection.insert({'a': 1, 'b': 1}, safe=True)
        self._id5 = self.collection.insert({'b': 2, 'c': 2}, safe=True)

        # The cursor is live, so it should pick up the new documents
        qs = self.qs.sort('a', 'b')

        # Fill the result cache, using a bigger number to ensure
        # everything gets loaded
        qs._fill_to(5)

        # Documents will be sorted with both keys in ascending order.
        # Any document without a key will appear before documents with
        # that key
        self.assertEqual(qs[0]['_id'], self._id3)
        self.assertEqual(qs[1]['_id'], self._id5)
        self.assertEqual(qs[2]['_id'], self._id4)
        self.assertEqual(qs[3]['_id'], self._id1)
        self.assertEqual(qs[4]['_id'], self._id2)

    def test_sort_multiple_descending(self):
        """Test the `sort()` method for multiple descending keys."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        # In order to really test this, some new documents are needed
        # because all current key/value combinations used are unique
        self._id4 = self.collection.insert({'a': 1, 'b': 1}, safe=True)
        self._id5 = self.collection.insert({'b': 2, 'c': 2}, safe=True)

        # The cursor is live, so it should pick up the new documents
        qs = self.qs.sort('-a', '-b')

        # Fill the result cache, using a bigger number to ensure
        # everything gets loaded
        qs._fill_to(5)

        # Documents will be sorted with both keys in descending order.
        # Any document without a key will appear after documents with
        # that key
        self.assertEqual(qs[4]['_id'], self._id3)
        self.assertEqual(qs[3]['_id'], self._id5)
        self.assertEqual(qs[2]['_id'], self._id4)
        self.assertEqual(qs[1]['_id'], self._id1)
        self.assertEqual(qs[0]['_id'], self._id2)

    def test_sort_multiple_ascending_then_descending(self):
        """Test the `sort()` method for multiple keys ascending first."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        # In order to really test this, some new documents are needed
        # because all current key/value combinations used are unique
        self._id4 = self.collection.insert({'a': 1, 'b': 1}, safe=True)
        self._id5 = self.collection.insert({'b': 2, 'c': 2}, safe=True)

        # The cursor is live, so it should pick up the new documents
        qs = self.qs.sort('a', '-b')

        # Fill the result cache, using a bigger number to ensure
        # everything gets loaded
        qs._fill_to(5)

        # Documents will be sorted with both keys in ascending order.
        # Any document without a key will appear before documents with
        # that key
        self.assertEqual(qs[0]['_id'], self._id5)
        self.assertEqual(qs[1]['_id'], self._id3)
        self.assertEqual(qs[2]['_id'], self._id1)
        self.assertEqual(qs[3]['_id'], self._id4)
        self.assertEqual(qs[4]['_id'], self._id2)

    def test_sort_multiple_descending_then_ascending(self):
        """Test the `sort()` method for multiple keys descending first."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        # In order to really test this, some new documents are needed
        # because all current key/value combinations used are unique
        self._id4 = self.collection.insert({'a': 1, 'b': 1}, safe=True)
        self._id5 = self.collection.insert({'b': 2, 'c': 2}, safe=True)

        # The cursor is live, so it should pick up the new documents
        qs = self.qs.sort('-a', 'b')

        # Fill the result cache, using a bigger number to ensure
        # everything gets loaded
        qs._fill_to(5)

        # Documents will be sorted with both keys in descending order.
        # Any document without a key will appear after documents with
        # that key
        self.assertEqual(qs[4]['_id'], self._id5)
        self.assertEqual(qs[3]['_id'], self._id3)
        self.assertEqual(qs[2]['_id'], self._id1)
        self.assertEqual(qs[1]['_id'], self._id4)
        self.assertEqual(qs[0]['_id'], self._id2)

    def test__fill_to(self):
        """Test the `_fill_to()` method."""

        self.qs._cls = None

        # Fill the whole result cache and make sure that all documents
        # are loaded and in the correct order
        self.qs._fill_to(2)

        doc1 = {'_id': self._id1, 'a': 1, 'b': 2}
        doc2 = {'_id': self._id2, 'a': 2, 'c': 1}
        doc3 = {'_id': self._id3, 'b': 1, 'c': 2}

        self.assertTrue(doc1 in self.qs._items)
        self.assertTrue(doc2 in self.qs._items)
        self.assertTrue(doc3 in self.qs._items)

    def test__fill_to_as_documents(self):
        """Test that `_fill_to()` stores documents."""

        # When no class is associated with the QuerySet, documents
        # should be used
        self.qs._cls = None

        self.qs._fill_to(3)

        for x in xrange(3):
            self.assertTrue(isinstance(self.qs._items[x], dict))

    def test__fill_to_as_model(self):
        """Test that `_fill_to()` stores model instances."""

        self.qs._fill_to(3)

        for x in xrange(3):
            self.assertTrue(isinstance(self.qs._items[x], TestModel))

    def test__fill_to_indexes(self):
        """Test that `_fill_to()` property fills to the specified index."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        docs = [{'_id': self._id1, 'a': 1, 'b': 2},
                {'_id': self._id2, 'a': 2, 'c': 1},
                {'_id': self._id3, 'b': 1, 'c': 2}]

        for x in xrange(3):
            self.qs._fill_to(x)
            self.assertEqual(self.qs._items[x], docs[x])
            self.assertEqual(len(self.qs._items), x + 1)

    def test__fill_to_overfill(self):
        ("Test that `_fill_to()` correctly handles indexes greater than"
         " the maximum index of the result cache.")

        # count() will always be 1 greater than the last index
        self.qs._fill_to(self.qs.count())

        # 3 is being used here because if will fail if the total number
        # of documents is changed, whereas using 3 above wouldn't
        # necessarily lead to problems
        self.assertEqual(len(self.qs._items), 3)

    def test__fill_to_twice(self):
        """Test that `_fill_to()` can be called multiple times."""

        self.qs._fill_to(0)
        self.assertEqual(len(self.qs._items), 1)

        self.qs._fill_to(0)
        self.assertEqual(len(self.qs._items), 1)

        self.qs._fill_to(self.qs.count())
        self.assertEqual(len(self.qs._items), self.qs._count)

        self.qs._fill_to(self.qs._count)
        self.assertEqual(len(self.qs._items), self.qs._count)

    def test___getitem__(self):
        """Test the `__getitem__()` method."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        docs = [{'_id': self._id1, 'a': 1, 'b': 2},
                {'_id': self._id2, 'a': 2, 'c': 1},
                {'_id': self._id3, 'b': 1, 'c': 2}]

        for x in xrange(3):
            self.assertEqual(self.qs[x], docs[x])
            self.assertEqual(self.qs[x], self.qs._items[x])

    def test___getitem___slice(self):
        """Test the `__getitem__()` method with slices."""

        # Disable the model class associated with the model so that
        # the result cache can be compared directly to dictionaries
        self.qs._cls = None

        doc1 = {'_id': self._id1, 'a': 1, 'b': 2}
        doc2 = {'_id': self._id2, 'a': 2, 'c': 1}
        doc3 = {'_id': self._id3, 'b': 1, 'c': 2}

        slice1 = self.qs[1:]
        slice2 = self.qs[:1]
        slice3 = self.qs[1:2]
        slice4 = self.qs[::2]
        slice5 = self.qs[1::2]
        slice6 = self.qs[::]

        self.assertEqual(len(slice1), 2)
        self.assertFalse(doc1 in slice1)
        self.assertTrue(doc2 in slice1)
        self.assertTrue(doc3 in slice1)

        self.assertEqual(len(slice2), 1)
        self.assertTrue(doc1 in slice2)
        self.assertFalse(doc2 in slice2)
        self.assertFalse(doc2 in slice2)

        self.assertEqual(len(slice3), 1)
        self.assertFalse(doc1 in slice3)
        self.assertTrue(doc2 in slice3)
        self.assertFalse(doc3 in slice3)

        self.assertEqual(len(slice4), 2)
        self.assertTrue(doc1 in slice4)
        self.assertFalse(doc2 in slice4)
        self.assertTrue(doc3 in slice4)

        self.assertEqual(len(slice5), 1)
        self.assertFalse(doc1 in slice5)
        self.assertTrue(doc2 in slice5)
        self.assertFalse(doc3 in slice5)

        self.assertEqual(len(slice6), 3)
        self.assertTrue(doc1 in slice6)
        self.assertTrue(doc2 in slice6)
        self.assertTrue(doc3 in slice6)

    def test___getitem___indexerror(self):
        """Test that `__getitem__()` raises `IndexError`."""

        with self.assertRaises(IndexError):
            self.qs[3]

    def test___getitem___typeerror(self):
        """Test that `__getitem__()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            self.qs[-1]

    def test___iter__(self):
        """Test the `__iter__()` method."""

        self.assertTrue(isinstance(self.qs.__iter__(), collections.Iterable))

    def test___iter___fills_cache(self):
        """Test that `__iter__()` fills the result cache."""

        # Sanity check
        self.assertEqual(len(self.qs._items), 0)

        for x in self.qs:
            pass

        self.assertEqual(len(self.qs._items), 3)

    def test__iter___fills_cache_partial(self):
        """Test that `__iter__()` fills the rest of the result cache."""

        # Put a record into the result cache
        self.qs[0]

        for x in self.qs:
            pass

        self.assertEqual(len(self.qs._items), 3)

    def test___len__(self):
        """Test the `__len__()` method."""

        # Mock __len__() to make sure it's called for len()
        with mock.patch('simon.query.QuerySet.__len__') as mock_len:
            mock_len.return_value = 1

            self.assertEqual(len(self.qs), 1)

        # Also test the real value
        self.assertEqual(len(self.qs), 3)
