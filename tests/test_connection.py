"""Tests of connecting to a database"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock
import pymongo

from simon import connection


class TestConnection(unittest.TestCase):
    """Test database connections"""

    def tearDown(self):
        # Reset the cached connections and databases so the ones added
        # during one test don't affect another
        connection._connections = None
        connection._databases = None

    def test_connect(self):
        """Test the `connect()` method."""

        with mock.patch('simon.connection._get_connection') as mock_method:
            mock_method.return_value = ({'test': mock.Mock()}, {})
            connection.connect(name='test')
            mock_method.assert_called_with(host='localhost', port=None,
                                           replica_set=None)

            mock_method.return_value = ({'test': mock.Mock()}, {})
            connection.connect(name='test', alias='test2')
            mock_method.assert_called_with(host='localhost', port=None,
                                           replica_set=None)

            mock_method.return_value = ({'test3': mock.Mock()}, {})
            connection.connect(host='someotherhost', name='test3', port=1234)
            mock_method.assert_called_with(host='someotherhost', port=1234,
                                           replica_set=None)

            mock_method.return_value = ({'simon': mock.Mock()}, {})
            connection.connect(host='simon.mongo.com', name='simon',
                               port=27017, username='simon',
                               password='simon')
            mock_method.assert_called_with(host='simon.mongo.com', port=27017,
                                           replica_set=None)

            mock_method.return_value = ({'simon': mock.Mock()}, {
                'name': 'simon',
                'username': 'simon',
                'password': 'simon',
            })
            url = 'mongodb://simon:simon@simon.mongo.com:27017/simon'
            connection.connect(host=url, alias='remote_uri')
            mock_method.assert_called_with(host=url, port=None,
                                           replica_set=None)

            mock_method.return_value = ({'simon-rs': mock.Mock()}, {
                'name': 'simon-rs',
                'username': 'simon',
                'password': 'simon',
            })
            url = 'mongodb://simon:simon@simon.m0.mongo.com:27017/simon-rs'
            connection.connect(host=url, replica_set='simonrs',
                               alias='replica1')
            mock_method.assert_called_with(host=url, port=None,
                                           replica_set='simonrs')

            mock_method.return_value = ({'simon-rs': mock.Mock()}, {
                'name': 'simon-rs',
                'username': 'simon',
                'password': 'simon',
            })
            url = ('mongodb://simon:simon@simon.m0.mongo.com:27017,'
                   'simon.m1.mongo.com:27017/simon-rs')
            connection.connect(host=url, replica_set='simonrs',
                               alias='replica2')
            mock_method.assert_called_with(host=url, port=None,
                                           replica_set='simonrs')

        self.assertIn('test', connection._databases)
        self.assertIn('test2', connection._databases)
        self.assertIn('test3', connection._databases)
        self.assertIn('simon', connection._databases)
        self.assertIn('remote_uri', connection._databases)
        self.assertIn('replica1', connection._databases)
        self.assertIn('replica2', connection._databases)

        self.assertIn('default', connection._databases)

        self.assertEqual(connection._databases['test'],
                         connection._databases['default'])
        self.assertNotEqual(connection._databases['test'],
                            connection._databases['test2'])

    def test_connect_connectionerror(self):
        """Test that `connect()` raises `ConnectionError()`."""

        with mock.patch('simon.connection._get_connection') as mock_method:
            mock_method.return_value = ({'test': mock.Mock()}, {})

            with self.assertRaises(connection.ConnectionError):
                connection.connect()

    def test__get_connection(self):
        """Test the `_get_connection()` method."""

        with mock.patch('simon.connection.MongoClient') as mock_conn:
            connection._get_connection(host='localhost', port=None,
                                       replica_set=None)

            mock_conn.assert_called_with(host='localhost', port=None)

            self.assertIn('localhost:27017', connection._connections)

            # When calling _get_connection() the second time, the
            # connection should be returned right from _connections
            # so mock_conn() should still have the same call parameters
            # and the length of __connections should be 1
            connection._get_connection(host='localhost', port=27017,
                                       replica_set=None)

            mock_conn.assert_called_with(host='localhost', port=None)

            self.assertEqual(len(connection._connections), 1)

    def test__get_connection_with_connection(self):
        """Test the `_get_connection()` method with a `Connection`."""

        conn = mock.Mock()
        conn.database_names = mock.Mock(return_value=True)

        conn2, __ = connection._get_connection(host=conn, port=None)

        self.assertEqual(conn, conn2)

    def test__get_connection_with_replica_set(self):
        """Test the `_get_connection()` method with replica sets."""

        with mock.patch('simon.connection.MongoReplicaSetClient') as mock_conn:
            connection._get_connection(host='localhost', port=None,
                                       replica_set='simonrs')
            mock_conn.assert_called_with(hosts_or_uri='localhost',
                                         replicaSet='simonrs')

    def test__get_connection_with_uri(self):
        """Test the `_get_connection()` method with URIs."""

        with mock.patch('simon.connection.MongoClient') as mock_conn:
            url1 = ('mongodb://simonuser:simonpassword@simon.mongo.com:27017/'
                    'simon')
            conn, settings = connection._get_connection(host=url1, port=None,
                                                        replica_set=None)

            mock_conn.assert_called_with(host='simon.mongo.com', port=27017)

            self.assertEqual(settings['name'], 'simon')
            self.assertEqual(settings['username'], 'simonuser')
            self.assertEqual(settings['password'], 'simonpassword')

        with mock.patch('simon.connection.MongoReplicaSetClient') as mock_conn:
            url2 = ('mongodb://simonuser:simonpassword@simon.m0.mongo.com:'
                    '27017/simon-rs')
            conn, settings = connection._get_connection(host=url2, port=None,
                                                        replica_set='simonrs')

            mock_conn.assert_called_with(hosts_or_uri='simon.m0.mongo.com',
                                         replicaSet='simonrs')

            self.assertEqual(settings['name'], 'simon-rs')
            self.assertEqual(settings['username'], 'simonuser')
            self.assertEqual(settings['password'], 'simonpassword')

            url3 = ('mongodb://simonuser:simonpassword@'
                    'simon.m0.mongo.com:27017,simon.m1.mongo.com:27017/'
                    'simon-rs')
            conn, settings = connection._get_connection(host=url3, port=None,
                                                        replica_set='simonrs')

            mock_conn.assert_called_with(hosts_or_uri=url3,
                                         replicaSet='simonrs')

            self.assertEqual(settings['name'], 'simon-rs')
            self.assertEqual(settings['username'], 'simonuser')
            self.assertEqual(settings['password'], 'simonpassword')

            url4 = ('mongodb://simonuser:simonpassword@'
                    'simon.m1.mongo.com:27017/simon-rsh?replicaSet=simon-rs')
            conn, settings = connection._get_connection(host=url4, port=None,
                                                        replica_set=None)

            mock_conn.assert_called_with(hosts_or_uri='simon.m1.mongo.com',
                                         replicaSet='simon-rs')

            self.assertEqual(settings['name'], 'simon-rsh')
            self.assertEqual(settings['username'], 'simonuser')
            self.assertEqual(settings['password'], 'simonpassword')

        self.assertIn('simon.mongo.com:27017', connection._connections)
        self.assertIn('simon.m0.mongo.com:simonrs', connection._connections)
        self.assertIn('{0}:simonrs'.format(url3), connection._connections)

    def test__get_connection_connectionerror(self):
        """Test that `_get_connection()` raises `ConnectionError`."""

        with self.assertRaises(connection.ConnectionError):
            connection._get_connection(host='/dev/null', port=None)

    def test_get_database(self):
        """Test the `get_database() method."""

        connection._databases = {'first': 1, 'second': 2}

        self.assertEqual(connection.get_database('first'), 1)
        self.assertEqual(connection.get_database('second'), 2)

    def test_get_database_connectionerror(self):
        """Test that `get_database()` raises `ConnectionError`."""

        # First with an empty _databases
        with self.assertRaises(connection.ConnectionError):
            connection.get_database('invalidnamethatdoesntexist')

        # Then with _databases
        connection._databases = {'first': 1, 'second': 2}
        with self.assertRaises(connection.ConnectionError):
            connection.get_database('invalidnamethatdoesntexist')

    def test_pymongo_supports_mongoclient(self):
        """Test that `pymongo_supports_mongoclient` is set correctly."""

        self.assertEqual(connection.pymongo_supports_mongoclient,
                         pymongo.version_tuple[:2] >= (2, 4))
