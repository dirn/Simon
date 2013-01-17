Connecting to a Database
========================

As useful as Simon is, it's of no use without a database connection.
Connecting to a database can be as simple as specifying its name.

.. code-block:: python

    from simon.connection import connect

    connect(name='simon')

This will connect to the ``mongod`` instance running on ``localhost``
and use the database named ``simon``.

Of course the :meth:`~simon.connection.connect` method can do more than
just connect to a database. For starters, it can connect to another
database.

.. code-block:: python

    connect(name='metrics')

This will use the same ``mongod`` instance as before, this time using
the database named ``metrics``.

When connecting to a database, each is given an alias. By default, Simon
tries to use the name of the database as its alias. If all of your
databases are part of the same MongoDB server, this won't be an issue.
There may be times, however, when your databases are located in multiple
locations. If two databases have the same name, the default alias
behavior won't be sufficient. Fortunately you can assign any alias you
want.

.. code-block:: python

    connect('localhost', name='simon')

    connect('legacy-server', name='simon', alias='legacy-simon')

This will connect to ``mongod`` on ``localhost`` and use the ``simon``
database with the alias ``simon``. It will also connect to ``mongod`` on
``legacy-server`` and use the ``simon`` database with the alias
``legacy-simon``.

Before moving on to more advanced concepts, there's one more thing to
point out. The first call to :meth:`~simon.connection.connect` will have
two aliases, the name of the database and ``default``. By default, all
of your Simon models will use this connection.


Authentication
--------------

As a matter of best practice, it's a good idea to use authentication
with your database. The :meth:`~simon.connection.connect` method accepts
``username`` and ``password`` parameters.

.. code-block:: python

    connect(name='simon', username='theuser', password='thepassword')

This will fail to connect to the database if the authentication fails.

Replica Sets
------------

Another good idea when working with MongoDB is to use replica sets.
:meth:`~simon.connection.connect` accepts a parameter named
``replicaSet``--stylized this way to match the `URI Connection String`_
parameter--with the name of the replica set to use.

.. code-block:: python

    connect(name='simon', replicaSet='simon-rs')


URI Connection String
---------------------

The :meth:`~simon.connection.connect` method supports connecting to a
database using a URI.

.. code-block:: python

    connect('mongodb://username:password@localhost:27017/simon?replicaSet=simon-rs')

Full details are available in the `MongoDB Docs`_.

.. _MongoDB Docs: http://docs.mongodb.org/manual/reference/connection-string/
