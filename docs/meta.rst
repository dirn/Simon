Model Meta Options
==================

When defining a class, you can do more than just give it a name. By
defining a class named ``Meta`` within a model, you can control several
aspects of its behavior. Any options that are omitted will be given
their default values, as show below.

.. code-block:: python

    class User(Model):
        class Meta:
            auto_timestamp = True
            collection = 'users'
            database = 'default'
            field_map = {'id': '_id'}
            map_id = True
            safe = False
            sort = None

``auto_timestamp``
------------------

By default, calling :meth:`~simon.Model.save` will cause the ``created``
and ``modified`` fields to update accordingly. Adding
``auto_timestamp = False`` to the ``Meta`` class will disable this
behavior.

.. code-block:: python

    class Meta:
        auto_timestamp = False  # do not automatically add timestamps


``collection``
--------------

By default, the collection associated with a model will be the name of
the model with an ``s`` appended to it. Adding ``collection`` to the
``Meta`` class will allow its value to altered.

.. code-block:: python

    class Meta:
        collection = 'simon'  # store documents in the simon collection


``database``
------------

By default, all collections will be located in the default database. If
you use the :meth:`~simon.connection.connect` method to connect to
additional databases, the database to use with a model can be controlled
by adding the ``database`` option to the ``Meta`` class.

.. code-block:: python

    connect('localhost', name='logs', alias='logs')

    class Meta:
        database = 'logs'  # use the logs database


``field_map`` and ``map_id``
----------------------------

By default, the ``_id`` field of all models is exposed through the
``id`` attribute. Additional fields can be added to the mapping by
including them in the ``field_map`` dictionary. The keys of the
dictionary represent attribute names and the values represent the keys
used in the document.

When the ``map_id`` option is ``True`` (the default), you can define a
custom mapping without having to include ``'id': '_id'``. It will be
added for you.

.. code-block:: python

    class Meta:
        # map friends to list_of_friends
        field_map = {'list_of_friends': 'friends'}
        map_id = False  # do not map _id to id

You can also use ``field_map`` to expose nested fields as top-level
attributes.

.. code-block:: python

    class Meta:
        field_map = {'x': 'location.x', 'y': 'location.y'}

Why would you want to use this behavior? Unlike a relational database
which stores its schema at the table level, MongoDB's dynamic schema
requires key names to be stored as part of each document. The longer the
names of your keys, the more storage space you will need (keep in mind
this is only really a problem with extremely large collections). When
using shortened key names, it may make the names harder to remember,
resulting in code that is harder to read and maintain. By utilizing
``field_map``, more meaningful names can be used in code while storing
shorter variations in the database.

.. code-block:: python

    class User(Model):
        class Meta:
            field_map = {
                'first_name': 'fname',
                'last_name': 'lname',
                'location': 'loc',
            }

    user = User.create(first_name='Simon', last_name='Seville',
                       location='Fresno, CA')

This query executing in the mongo Shell would look a little different:

.. code-block:: javascript

    db.users.insert({fname: 'Simon', lname: 'Seville', loc: 'Fresno, CA'})


``safe``
--------

By default updates to the database are performed without write concern
enabled. While it is possible to provide ``safe=True`` as a parameter
to each call, setting the ``safe`` option in the ``Meta`` class to
``True`` will cause all updates for that model to use write concern
without the need for using the ``safe`` parameter.

.. code-block:: python

    class Meta:
        safe = True  # always use write concern for this model

More information about write concern is available in the
`MongoDB Docs <http://docs.mongodb.org/manual/core/write-operations/#write-concern>`_.


``sort``
--------

By default, calls to :meth:`~simon.Model.all` and
:meth:`~simon.Model.find` will use natural order for sorting. If you
want to have a model default to a different sort order, you can do so
by defining the ``sort`` option on the ``Meta`` class.

.. code-block:: python

    class Meta:
        sort = 'name'  # sort by name ascending

The default sort can also handle multiple fields.

.. code-block:: python

    class Meta:
        sort = ('name', 'email')  # sort by name and email ascending

For a explanation of how to take full advantage of the ``sort`` option,
check out the :meth:`~simon.query.QuerySet.sort` method.

More information about natural sort is available in the
`MongoDB Docs <http://docs.mongodb.org/manual/reference/glossary/#term-natural-order>`_.
