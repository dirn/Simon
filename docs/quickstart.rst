Quickstart
==========

Ready to dive in? Here's a quick run through the basics of using Simon.
It will guide you through defining models, saving and retrieving
documents, and connecting to a database.


Defining a Model
----------------

To define a simple model, all you need to do is inherit from the
:class:`~simon.Model` class.

.. code-block:: python

    from simon import Model

    class User(Model):
        """This is the model used for users."""

This will define the ``User`` model which will use the ``users``
collection in the database.


Using the Model
---------------

To instantiate a new ``User``:

.. code-block:: python

    user = User(name='Simon', email='simon@example.com')

Attributes can also be assigned after instantiation.

.. code-block:: python

    user = User()
    user.name = 'Simon'
    user.email = 'simon@example.com'


Saving
------

Saving the changes is as easy as calling :meth:`~simon.Model.save`.
``created`` and ``modified`` dates will be added to the document before
it is written to the database, and the
:class:`ObjectId <pymongo:bson.objectid.ObjectId>` assigned by the
database will be added to the instance. (``created`` will only be added
to documents that haven't already been saved and don't already have a
``created`` field.)

.. code-block:: python

    user.save()

    print '%r %r %r' % (user.id, user.created, user.modified)
    # ObjectId('50e467580ea5faf0b83679f7') datetime.datetime(2013, 1, 2, 16, 59, 4, 688000) datetime.datetime(2013, 1, 2, 16, 59, 4, 688000)

**By default saves do not happen with write concern set.** There is no
guarantee the document will make it to the database. Write concern can
be turned on by setting the ``safe`` parameter to ``True``.

.. code-block:: python

    user.save(safe=True)


Retrieving
----------

Once the document has been saved it can easily be retrieved from the
database. The :meth:`~simon.Model.get` method accepts the names of
fields as parameters with values to match against.

.. code-block:: python

    user = User.get(name='Simon')
    print '%r %r' % (user.name, user.email)
    # 'Simon' 'simon@example.com'

For information about the possible exceptions associated with
:meth:`~simon.Model.get`, check out `Exceptions`_.

Retrieving multiple documents instead of just one is also easy. Just use
the :meth:`~simon.Model.find` method instead of
:meth:`~simon.Model.get`. They accept parameters the same way.

.. code-block:: python

    user2 = User(name='Simon', email='simon@example.org')
    user2.save()

    users = User.find(name='simon')
    for user in users:
        print '%r %r' % (user.name, user.email)

    # 'Simon' 'simon@example.com'
    # 'Simon' 'simon@example.org'


Connecting to a Database
------------------------

Before you can use your models, you need to connect to a database. This
is done by using the :meth:`~simon.connection.connect` method.

.. code-block:: python

    from simon.connection import connect

    connect('localhost', name='simon')

This will open a connection to the ``simon`` database on ``localhost``.
It's also possible to connect to a database on a remote server.

.. code-block:: python

    connect('simon.example.com', name='simon')

Or you can specify a full URI.

.. code-block:: python

    connect('mongodb://simon.example.com/simon')

When connecting to a database that requires authentication, a username
and password can be specified either through the ``username`` and
``password`` arguments or as part of the URI.

.. code-block:: python

    connect('localhost', name='simon', username='user', password='passwd')

    # ~ or ~

    connect('mongodb://user:passwd@simon.example.com/simon')


Exceptions
----------

When using the :meth:`~simon.Model.get` method from a model class it is
important to keep in mind that there are a couple of exceptions it can
raise. It's a good idea to catch them.

.. code-block:: python

    try:
        user = User.get(name='Simon2')
    except User.NoDocumentFound:
        # This means no documents matched the query
        handle_the_exception()

    try:
        user = User.get(name='Simon')
    except User.MultipleDocumentsFound:
        # This means more than one document matched the query
        handle_the_exception()

There is also an exception that can be raised when connecting to a
database.

.. code-block:: python

    try:
        connect('locahost', name='simon')
    except ConnectionError:
        # There was a problem connecting to the database
        handle_the_exception()
