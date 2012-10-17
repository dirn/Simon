__all__ = ('connect', 'get_database', 'ConnectionError')

from pymongo import Connection, ReplicaSetConnection, uri_parser

from .exceptions import ConnectionError


__connections__ = None
__databases__ = None


def connect(host='localhost', name=None, username=None, password=None,
            port=None, alias=None, **kwargs):
    """Connects to a database.

    :param host: The name of the MongoDB host.
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
                   ``pymongo.connection.Connection``.
    :type kwargs: kwargs.
    :returns: ``pymongo.database.Database`` -- the database.
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

    connection = _get_connection(**settings)

    # If a URI has been given for host, parse it and update the settings
    if '://' in host:
        pieces = uri_parser.parse_uri(host)

        # For certain settings not found in the URI, the values can
        # be obtained from the arguments passed in to the method.
        name = pieces.get('database', name)
        username = pieces.get('username', username)
        password = pieces.get('password', password)

        # Update the initial settings with those from the URI.
        settings.update({
            'name': name,
            'username': username,
            'password': password,
        })

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


def _get_connection(**settings):
    """Gets the connection to the database.

    This is where the actual work happens.

    .. versionadded:: 0.1.0
    """

    # If host is already a connection, get out early
    if isinstance(settings['host'], (Connection, ReplicaSetConnection)):
        return settings['host']

    host, port = settings.get('host'), settings.get('port', None)

    # For the purpose of building this key, use the default port
    # instead of no port so that calls explicity requesting the default
    # port won't be treated as different than calls not requesting one
    connection_key = '{0}:{1}'.format(host, port or 27017)

    global __connections__
    if not isinstance(__connections__, dict):
        __connections__ = {}

    if connection_key not in __connections__:
        # If connecting to a replica set, set a flag because a
        # ReplicaSetConnection will be needed instead of a Connection.
        if 'replicaSet' in host:
            settings['replicaSet'] = True

        # I can't connect to 2.3 with these keys set. I need to explore
        # this further to understand when they should be set and when they
        # shouldn't.
        settings.pop('name', None)
        settings.pop('username', None)
        settings.pop('password', None)

        # If using a replica set, prepare the settings and class name
        if settings.get('replicaSet', False):
            connection_class = ReplicaSetConnection
            settings['host_or_uri'] = settings.pop('host')
            settings.pop('port', None)
            settings.pop('replicaSet', None)
        else:
            connection_class = Connection

        # Open a connection to the database and try to connect to it
        try:
            connection = connection_class(**settings)
        except Exception as e:
            raise ConnectionError(
                "Cannot connection to database '{0}':\n{1}".format(host, e))

        __connections__[connection_key] = connection

    return __connections__[connection_key]


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
