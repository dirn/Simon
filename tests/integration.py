"""Integration tests"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import connection


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


class TestIntegrations(unittest.TestCase):
    """Integration tests"""

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
