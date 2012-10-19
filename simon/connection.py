__all__ = ('connect', 'get_database', 'ConnectionError')

from pymongo import Connection, ReplicaSetConnection, uri_parser

from .exceptions import ConnectionError


__connections__ = None
__databases__ = None


def connect(host='localhost', name=None, username=None, password=None,
            port=None, alias=None, **kwargs):
    """Connects to a database.

    :param host: Hostname, IP address, or MongoDB URI of the host.
    :type host: str.
    :param name: The name of the MongoDB database.
    :type host: str.
    :param username: The username to use for authentication.
    :type username: str.
    :param password: The password to use for authentication.
    :type password: str.
    :param port: The port of the MongoDB host.
    :type port: int.
    :param alias: An alias to use for accessing the database. If no
                  value is provided, ``name`` will be used.
    :type alias: str.
    :param kwargs: All other keyword arguments accepted by
                   :class:`pymongo.connection.Connection`.
    :type kwargs: kwargs.
    :returns: :class:`pymongo.database.Database` -- the database.
    :raises: :class:`ConnectionError`

    .. versionadded:: 0.1.0
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
    connection, parsed_settings = _get_connection(
        host=host, port=port, replica_set=kwargs.pop('replicaSet', False),
        **kwargs)
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

    # Make sure that __databases__ is a dict before using it
    global __databases__
    if not isinstance(__databases__, dict):
        __databases__ = {}

    # Capture the database and store it in __databases__ under its alias
    __databases__[alias] = db = connection[name]
    if 'default' not in __databases__:
        __databases__['default'] = db

    if username and password:
        db.authenticate(username, password)

    return connection


def _get_connection(host, port, replica_set=False, **kwargs):
    """Gets the connection to the database.

    This will create a connection to a new MongoDB server and store it
    internally for later use. If a server is requested that
    ``_get_connection()`` has seen before, the stored connection will be
    used.

    If the ``host`` is actually a MongoDB URI, the username, password,
    and database name will be parsed from the URI and returned as the
    second part of the ``tuple`` returned by this method.

    :param host: Hostname, IP address, or MongoDB URI of the host.
    :type host: str.
    :param port: The port of the MongoDB host.
    :type port: int.
    :param replica_set: Whether to connect to a replica set
    :type replica_set: bool.
    :param kwargs: All other keyword arguments accepted by
                   :class:`pymongo.connection.Connection`.
    :type kwargs: kwargs.
    :returns: tuple -- a pair of values containing a
                       :class:`pymongo.Connection` and any settings
                       parsed when a URI is provided.

    .. versionadded:: 0.1.0
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
        if 'replicaSet' in host:
            replica_set = True

        # Check the list of nodes in the parsed URI. More than one
        # means there's a replica set
        if 'nodelist' in pieces:
            number_of_nodes = len(pieces['nodelist'])
            if number_of_nodes > 1:
                replica_set = True
            elif number_of_nodes == 1:
                # If there was only one, get the updated host and port
                host, port = pieces['nodelist'][0]

    # For the purpose of building this key, use the default port
    # instead of no port so that calls explicity requesting the default
    # port won't be treated as different than calls not requesting one
    connection_key = '{0}:{1}'.format(host, port or 27017)

    global __connections__
    if not isinstance(__connections__, dict):
        __connections__ = {}

    if connection_key not in __connections__:
        # If using a replica set, prepare the settings and class name
        if replica_set:
            connection_class = ReplicaSetConnection
            settings = {'host_or_uri': host}
        else:
            connection_class = Connection
            settings = {'host': host, 'port': port}

        # Open a connection to the database and try to connect to it
        try:
            connection = connection_class(**settings)
        except Exception as e:
            raise ConnectionError(
                "Cannot connection to database '{0}':\n{1}".format(host, e))

        # Store the connection in the dictionary for easier retrieval
        # next time
        __connections__[connection_key] = connection

    return __connections__[connection_key], parsed_settings


def get_database(name):
    """Gets a reference to a database.

    :param name: The name of the database.
    :type name: str.
    :returns: ``pymongo.database.Database`` -- a database object.

    .. versionadded:: 0.1.0
    """

    if name not in __databases__:
        raise ConnectionError("There is no connection for database '{0}'. "
                              "Use `simon.connection.connect()` to connect "
                              "to it.".format(name))
    return __databases__[name]
