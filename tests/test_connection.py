"""Tests of connecting to a database.

These tests require a connection to a MongoDB instance.
"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from simon import connection


class TestConnection(unittest.TestCase):
    """Test database connections"""

    def setUp(self):
        connection.__databases__ = None

    def test_connect(self):
        """Test the `connect()` method."""

        with mock.patch('simon.connection.Connection') as mock_conn:
            connection.connect(name='test')

            mock_conn.assert_called_with(host='localhost', port=None)

        with mock.patch('simon.connection.Connection') as mock_conn:
            connection.connect(name='test', alias='test2')

            mock_conn.assert_called_with(host='localhost', port=None)

        with mock.patch('simon.connection.Connection') as mock_conn:
            connection.connect(host='someotherhost', name='test3', port=1234)

            mock_conn.assert_called_with(host='someotherhost', port=1234)

        with mock.patch('simon.connection.Connection') as mock_conn:
            connection.connect(host='simon.mongo.com', name='simon',
                               port=27017, username='simon',
                               password='simon')

            mock_conn.assert_called_with(host='simon.mongo.com', port=27017)

        with mock.patch('simon.connection.Connection') as mock_conn:
            url = 'mongodb://simon:simon@simon.mongo.com:27017/simon'
            connection.connect(host=url, alias='remote_uri')

            mock_conn.assert_called_with(host=url, port=None)

        with mock.patch('simon.connection.ReplicaSetConnection') as mock_conn:
            url = 'mongodb://simon:simon@simon.m0.mongo.com:27017/simon-rs'
            connection.connect(host=url, replicaSet=True, alias='replica1')

            mock_conn.assert_called_with(host_or_uri=url)

        with mock.patch('simon.connection.ReplicaSetConnection') as mock_conn:
            url = 'mongodb://simon:simon@simon.m0.mongo.com:27017,simon.m1.mongo.com:27017/simon-rs'
            connection.connect(host=url, replicaSet=True, alias='replica2')

            mock_conn.assert_called_with(host_or_uri=url)

        self.assertTrue('test' in connection.__databases__)
        self.assertTrue('test2' in connection.__databases__)
        self.assertTrue('test3' in connection.__databases__)
        self.assertTrue('simon' in connection.__databases__)
        self.assertTrue('remote_uri' in connection.__databases__)
        self.assertTrue('replica1' in connection.__databases__)
        self.assertTrue('replica2' in connection.__databases__)

        self.assertTrue('default' in connection.__databases__)

        self.assertEqual(connection.__databases__['test'],
                         connection.__databases__['default'])

    def test_get_database(self):
        """Test the `get_database() method."""

        connection.__databases__ = {'first': 1, 'second': 2}

        self.assertEqual(connection.get_database('first'), 1)
        self.assertEqual(connection.get_database('second'), 2)

    def test_get_database_connectionerror(self):
        """Test that `get_database()` raises `ConnectionError`."""

        # First with an empty __databases__
        with self.assertRaises(connection.ConnectionError):
            connection.get_database('invalidnamethatdoesntexist')

        # Then with __databases__
        connection.__databases__ = {'first': 1, 'second': 2}
        with self.assertRaises(connection.ConnectionError):
            connection.get_database('invalidnamethatdoesntexist')
