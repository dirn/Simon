"""Manage database connections"""

import warnings

from pymongo import uri_parser
try:
    # pymongo 2.4+
    from pymongo import MongoClient, MongoReplicaSetClient
except ImportError:
    from pymongo import (Connection as MongoClient,
                         ReplicaSetConnection as MongoReplicaSetClient,
                         version)
    pymongo_supports_mongoclient = False

    message = ('Support for PyMongo {0} has been deprecated. Please upgrade to'
               ' 2.4 or newer.')
    warnings.warn(message.format(version), DeprecationWarning)
else:
    pymongo_supports_mongoclient = True

from .exceptions import ConnectionError

__all__ = ('connect', 'get_database', 'ConnectionError')

_connections = None
_databases = None


def connect(host='localhost', name=None, username=None, password=None,
            port=None, alias=None, **kwargs):
    """Connect to a database.

    :param host: Hostname, IP address, or MongoDB URI of the host.
    :type host: str.
    :param name: (optional) The name of the MongoDB database.
    :type host: str.
    :param username: (optional) The username to use for authentication.
    :type username: str.
    :param password: (optional) The password to use for authentication.
    :type password: str.
    :param port: (optional) The port of the MongoDB host.
    :type port: int.
    :param alias: (optional) An alias to use for accessing the database.
                  If no value is provided, ``name`` will be used.
    :type alias: str.
    :param \*\*kwargs: All other keyword arguments accepted by
                   :class:`pymongo.connection.Connection`.
    :type \*\*kwargs: \*\*kwargs.
    :returns: :class:`pymongo.database.Database` -- the database.
    :raises: :class:`ConnectionError`

    .. versionchanged:: 0.2.0
       ``connect()`` now accepts ``replica_set`` as a kwarg, it is
       preferred over ``replicaSet``

    """

    # The default settings, based on the arguments passed in
    settings = {
        'host': host,
        'name': name,
        'port': port,
        'username': username,
        'password': password,
    }

    # Extend the settings with all other keyword arguments
    settings.update(kwargs)

    # Get replicaSet out of **kwargs because it can be passed in as its
    # own parameter
    #
    # NOTE
    # Version 0.1.0 wanted a warnted named replicaSet. 0.2.0 changed it
    # to replica_set (replicaSet was meant for parody with PyMongo).
    # This is to maintain backwards compatibility.
    replica_set = kwargs.pop('replica_set', None)
    throw_away = kwargs.pop('replicaSet', None)
    if replica_set is None:
        replica_set = throw_away

    connection, parsed_settings = _get_connection(
        host=host, port=port, replica_set=replica_set, **kwargs)
    if parsed_settings:
        settings.update(parsed_settings)

    name = settings.get('name', None)

    if name is None:
        raise ConnectionError('No database name was provided. '
                              'Make sure to append it to the host URI.')

    if alias is None:
        alias = name

    # If a username and password have been provided, try to authenticate
    # against the database as well. Make sure to save a reference to
    # the database so it can be referenced later on.

    # Make sure that _databases is a dict before using it
    global _databases
    if not isinstance(_databases, dict):
        _databases = {}

    # Capture the database and store it in _databases under its alias
    _databases[alias] = db = connection[name]
    if 'default' not in _databases:
        _databases['default'] = db

    if username and password:
        db.authenticate(username, password)

    return connection


def _get_connection(host, port, replica_set=None, **kwargs):
    """Return a connection to the database.

    This will create a connection to a new MongoDB server and store it
    internally for later use. If a server is requested that
    ``_get_connection()`` has seen before, the stored connection will be
    used.

    If the ``host`` is actually a MongoDB URI, the username, password,
    and database name will be parsed from the URI and returned as the
    second part of the ``tuple`` returned by this function.

    :param host: Hostname, IP address, or MongoDB URI of the host.
    :type host: str.
    :param port: The port of the MongoDB host.
    :type port: int.
    :param replica_set: (optional) Name of the replica set when
                        connecting to one.
    :type replica_set: str.
    :param \*\*kwargs: All other keyword arguments accepted by
                   :class:`pymongo.connection.Connection`.
    :type \*\*kwargs: \*\*kwargs.
    :returns: tuple -- a pair of values containing a
                       :class:`pymongo.Connection` and any settings
                       parsed when a URI is provided.

    """

    parsed_settings = {}

    # If host is already a connection, get out
    if hasattr(host, 'database_names'):
        return host, parsed_settings

    # If a URI has been given for host, parse it and get the settings
    if '://' in host:
        pieces = uri_parser.parse_uri(host)

        name = pieces.get('database', None)
        username = pieces.get('username', None)
        password = pieces.get('password', None)

        # Only update the settings if values were from in the URI
        if name is not None:
            parsed_settings['name'] = name
        if username is not None:
            parsed_settings['username'] = username
        if password is not None:
            parsed_settings['password'] = password

        # Check for a replica set
        if 'replicaset' in pieces['options']:
            replica_set = pieces['options']['replicaset']

        # Check the list of nodes in the parsed URI. If there was only
        # one, get the updated host and port
        if 'nodelist' in pieces and len(pieces['nodelist']) == 1:
            host, port = pieces['nodelist'][0]

    # For the purpose of building this key, use the default port
    # instead of no port so that calls explicity requesting the default
    # port won't be treated as different than calls not requesting one.
    # When a replica set is behind used however, use the host string
    # with the name of the replica set
    #
    # NOTE: I can foresee a problem here if there are two replica sets
    # running on the same host with the same name on different ports.
    # I'll look into that more when I have a better way to test
    # replica sets than I currently do.
    connection_key = '{0}:{1}'.format(host, replica_set if replica_set else
                                      (port or 27017))

    global _connections
    if not isinstance(_connections, dict):
        _connections = {}

    if connection_key not in _connections:
        # If using a replica set, prepare the settings and class name
        if replica_set:
            connection_class = MongoReplicaSetClient
            settings = {'hosts_or_uri': host, 'replicaSet': replica_set}
        else:
            connection_class = MongoClient
            settings = {'host': host, 'port': port}

        # Open a connection to the database and try to connect to it
        try:
            connection = connection_class(**settings)
        except Exception as e:
            raise ConnectionError(
                "Cannot connection to database '{0}':\n{1}".format(host, e))

        # Store the connection in the dictionary for easier retrieval
        # next time
        _connections[connection_key] = connection

    return _connections[connection_key], parsed_settings


def get_database(name):
    """Return a reference to a database.

    :param name: The name of the database.
    :type name: str.
    :returns: ``pymongo.database.Database`` -- a database object.

    """

    if not (_databases and name in _databases):
        raise ConnectionError("There is no connection for database '{0}'. "
                              "Use `simon.connection.connect()` to connect "
                              "to it.".format(name))
    return _databases[name]
