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
