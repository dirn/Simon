"""Integration tests"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import Model, connection


def skip_without_setting(setting):
    try:
        import integration_settings
    except ImportError:
        pass
    else:
        if hasattr(integration_settings, setting):
            return lambda f: f

    message = 'integration_settings.{0} was not found'.format(setting)
    return unittest.skip(message)


class TestModel(Model):
    class Meta:
        collection = 'simon-integration'
        database = 'simon-integration'


class TestUntypedModel(Model):
    class Meta:
        collection = 'simon-integration'
        database = 'simon-integration'
        typed_fields = {'_id': None}


class TestConnectionIntegrations(unittest.TestCase):
    """Connection integration tests"""

    def test__get_connection_with_localhost(self):
        """Test the `_get_connection()` method with localhost."""

        connection._get_connection(host='localhost', port=None)
        self.assertIsInstance(connection._connections['localhost:27017'],
                              connection.MongoClient)

    @skip_without_setting('REMOTE_HOST')
    def test__get_connection_with_remote_host(self):
        """Test the `_get_connection()` method with a remote host."""

        # The import is safe if the method passed the skip check
        from integration_settings import REMOTE_HOST

        username = REMOTE_HOST.get('MONGODB_USERNAME')
        password = REMOTE_HOST.get('MONGODB_PASSWORD')
        host = REMOTE_HOST.get('MONGODB_HOST')
        port = REMOTE_HOST.get('MONGODB_PORT', 27017)
        dbname = REMOTE_HOST.get('MONGODB_DBNAME')

        url = 'mongodb://{0}:{1}@{2}:{3}/{4}'.format(username, password,
                                                     host, port, dbname)
        connection._get_connection(host=url, port=None)
        self.assertIsInstance(
            connection._connections['{0}:{1}'.format(host, port)],
            connection.MongoClient)

    @skip_without_setting('REMOTE_REPLICA_SET')
    def test__get_connection_with_remote_host_replica_set(self):
        ("Test the `_get_connection()` method with a remote replica "
         "set.")

        # The import is safe if the method passed the skip check
        from integration_settings import REMOTE_REPLICA_SET

        username = REMOTE_REPLICA_SET.get('MONGODB_USERNAME')
        password = REMOTE_REPLICA_SET.get('MONGODB_PASSWORD')
        host = REMOTE_REPLICA_SET.get('MONGODB_HOST')
        dbname = REMOTE_REPLICA_SET.get('MONGODB_DBNAME')
        replica_set = REMOTE_REPLICA_SET.get('MONGODB_REPLICA_SET')

        url = 'mongodb://{0}:{1}@{2}/{3}'.format(username, password,
                                                 host, dbname)
        connection._get_connection(host=url, port=None,
                                   replica_set=replica_set)
        self.assertIsInstance(
            connection._connections['{0}:{1}'.format(url, replica_set)],
            connection.MongoReplicaSetClient)


class TestDatabaseIntegrations(unittest.TestCase):
    """Database integration tests"""

    @classmethod
    def setUpClass(cls):
        cls.connection = connection.connect(name='simon-integration')
        cls.db = cls.connection['simon-integration']
        cls.collection = cls.db['simon-integration']

    def tearDown(self):
        self.db.drop_collection('simon-integration')

    @classmethod
    def tearDownClass(cls):
        cls.connection.drop_database('simon-integration')

    def test_increment(self):
        """Test the `increment()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.increment('a', 2)

        self.assertEqual(m._document['a'], 3)

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['a'], 3)

    def test_pop(self):
        """Test the `pop()` method."""

        _id = self.collection.insert({'a': [1, 2, 3]})

        m = TestModel.get(_id=_id)

        m.pop('a')

        self.assertEqual(len(m._document['a']), 2)
        self.assertNotIn(3, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 2)
        self.assertNotIn(3, doc['a'])

        m.pop('-a')

        self.assertEqual(len(m._document['a']), 1)
        self.assertNotIn(1, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 1)
        self.assertNotIn(1, doc['a'])

    def test_pull(self):
        """Test the `pull()` method."""

        _id = self.collection.insert({'a': [1, 2, 3, 4, 4]})

        m = TestModel.get(_id=_id)

        # $pull

        m.pull('a', 1)

        self.assertEqual(len(m._document['a']), 4)
        self.assertNotIn(1, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 4)
        self.assertNotIn(1, doc['a'])

        m.pull(a=2)

        self.assertEqual(len(m._document['a']), 3)
        self.assertNotIn(2, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 3)
        self.assertNotIn(2, doc['a'])

        # $pullAll

        m.pull(a=[3, 4])

        self.assertEqual(len(m._document['a']), 0)
        self.assertNotIn(3, m._document['a'])
        self.assertNotIn(4, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 0)
        # With the length of 0 check, these are redundant
        self.assertNotIn(3, doc['a'])
        self.assertNotIn(4, doc['a'])

    def test_push(self):
        """Test the `push()` method."""

        _id = self.collection.insert({})

        m = TestModel.get(_id=_id)

        # $push

        m.push('a', 1)

        self.assertEqual(len(m._document['a']), 1)
        self.assertIn(1, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 1)
        self.assertIn(1, doc['a'])

        m.push(a=2)

        self.assertEqual(len(m._document['a']), 2)
        self.assertIn(2, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 2)
        self.assertIn(2, doc['a'])

        # $pushAll

        m.push(a=[3, 4])

        self.assertEqual(len(m._document['a']), 4)
        self.assertIn(3, m._document['a'])
        self.assertIn(4, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 4)
        self.assertIn(3, doc['a'])
        self.assertIn(4, doc['a'])

        # Make sure all of the values have been inserted in order
        self.assertEqual(doc['a'], [1, 2, 3, 4])

        # $addToSet

        m.push('a', 1, allow_duplicates=False)

        self.assertEqual(len(m._document['a']), 4)

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 4)

        m.push('a', 5, allow_duplicates=False)

        self.assertEqual(len(m._document['a']), 5)
        self.assertIn(5, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 5)
        self.assertIn(5, doc['a'])

        # $addToSet/$each

        m.push('a', [1, 2, 3, 4, 5, 6], allow_duplicates=False)

        self.assertEqual(len(m._document['a']), 6)
        self.assertIn(6, m._document['a'])

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(len(doc['a']), 6)
        self.assertIn(6, doc['a'])

        # Make sure all of the values have been inserted in order
        # Note that $addToSet doesn't do anything about the order of
        # values that already exist in the list, but the one new value
        # should be at the end.
        self.assertEqual(doc['a'], [1, 2, 3, 4, 5, 6])

    def test_raw_update(self):
        """Test the `raw_update()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.raw_update({'$set': {'b': 2}})

        self.assertEqual(m._document['b'], 2)

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['b'], 2)

    def test_remove_fields(self):
        """Test the `remove_fields()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.remove_fields('a')

        self.assertNotIn('a', m._document)

        doc = self.collection.find_one({'_id': _id})

        self.assertNotIn('a', doc)

    def test_rename(self):
        """Test the `rename()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.rename('a', 'b')

        doc = self.collection.find_one({'_id': _id})

        self.assertNotIn('a', doc)
        self.assertIn('b', doc)

    def test_save(self):
        """Test the `save()` method."""

        # Insert

        m = TestModel(a=1)
        m.save()

        doc = self.collection.find_one({'_id': m._document['_id']})

        self.assertEqual(doc['a'], 1)

        # Update

        m.b = 2
        m.save()

        doc = self.collection.find_one({'_id': m._document['_id']})

        self.assertEqual(doc['b'], 2)

        # Update with non-Object Id

        self.collection.insert({'_id': 1})

        m = TestUntypedModel(_id=1)
        m.a = 1
        m.save()

        doc = self.collection.find_one({'_id': 1})

        self.assertEqual(doc['a'], 1)

    def test_save_fields(self):
        """Test the `save_fields()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.b = 2
        m.save_fields('b')

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['b'], 2)

    def test_sort(self):
        """Test the `sort()` method."""

        self.collection.insert([{'a': 2}, {'a': 1}, {'a': 3}])

        qs = TestModel.all().sort('a')

        self.assertEqual(qs[0]._document['a'], 1)
        self.assertEqual(qs[1]._document['a'], 2)
        self.assertEqual(qs[2]._document['a'], 3)

        qs = TestModel.all().sort('-a')

        self.assertEqual(qs[0]._document['a'], 3)
        self.assertEqual(qs[1]._document['a'], 2)
        self.assertEqual(qs[2]._document['a'], 1)

    def test_update(self):
        """Test the `update()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.update(b=2)

        self.assertEqual(m._document['b'], 2)

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['b'], 2)
