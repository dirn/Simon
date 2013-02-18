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

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['a'], 3)

    def test_raw_update(self):
        """Test the `raw_update()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.raw_update({'$set': {'b': 2}})

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['b'], 2)

    def test_remove_fields(self):
        """Test the `remove_fields()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.remove_fields('a')

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

        m.b = 2
        m.save()

        doc = self.collection.find_one({'_id': m._document['_id']})

        self.assertEqual(doc['b'], 2)

    def test_save_fields(self):
        """Test the `save_fields()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.b = 2
        m.save_fields('b')

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['b'], 2)

    def test_update(self):
        """Test the `update()` method."""

        _id = self.collection.insert({'a': 1})

        m = TestModel.get(_id=_id)
        m.update(b=2)

        doc = self.collection.find_one({'_id': _id})

        self.assertEqual(doc['b'], 2)
