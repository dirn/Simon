Basic Usage
===========

Simon offers a lot of flexibility in how you interact with the database.

All of the examples below utilize the ``User`` model defined in
:doc:`quickstart`, so if you haven't already done that, you want to
check it out first.


Retrieving
----------

At the heart of retrieving documents are three methods:
:meth:`~simon.Model.all`, :meth:`~simon.Model.find`, and
:meth:`~simon.Model.get`.

Use :meth:`~simon.Model.all` to retrieve all documents from the
``users`` collection.

.. code-block:: python

    users = Users.all()

Often times it will be necessary to filter the documents coming back. To
do so, use :meth:`~simon.Model.find`. It takes a series of named
parameters that represent keys in the documents and the values to match
against.

To find all documents whose ``name`` field has a value of ``Simon``:

.. code-block:: python

    users = Users.find(name='Simon')

To find all documents whose ``name`` field has a value of ``Simon``
and whose ``company`` field has a value of ``My Company``:

.. code-block:: python

    internal_users = Users.find(name='Simon', company='My Company')

If you were to execute these queries using the mongo Shell, they would
look like:

.. code-block:: javascript

    users = db.users.find({name: 'Simon'})

    internal_users = db.users.find({name: 'Simon', company: 'My Company'})

At this point, no real information would have been returned from the
database. Utilizing the cursor behavior built into PyMongo, documents
will only be transferred from the database when they are requested. This
is done by interacting with the result of :meth:`~simon.Model.find` like
you would with any other iterable such as a ``list``.

.. code-block:: python

    for user in users:
        print 'A document was just loaded from the users collection'

Documents can also be loaded through slicing, although this will cause
all documents in, as well as prior to, the slice to be loaded.

.. code-block:: python

    first_user = users[0]
    # the first user has been loaded

    fourth_users = users[3]
    # the first four users have been loaded

    all_users = users[:]
    # all users have been loaded

More advanced uses are covered in :doc:`querying`.


Saving
------

The main way to save a document using Simon is with
:meth:`~simon.Model.save`. Calling it on an instance with a new document
will insert the document. The document will be given an
:class:`ObjectId <pymongo:bson.objectid.ObjectId>` by the database,
which will then be associated with the instance.

.. code-block:: python

    user = User(name='Simon')
    user.save()  # insert

Calling :meth:`~simon.Model.save` on an instance with an existing
document will update the document. This will replace what's in the
database with the one associated with the instance.

.. code-block:: python

    user.email = 'simon@example.org'
    user.save()  # update

The equivalent queries in the mongo Shell would be:

.. code-block:: javascript

    db.users.insert({name: 'Simon'})

    db.users.update({_id: ObjectId(...)}, {email: 'simon@example.org'})

More advanced uses are covered in :doc:`saving`.


Deleting
--------

If you don't want a document anymore, removing it from the database is
simply a matter of calling :meth:`~simon.Model.delete`.

.. code-block:: python

    user.delete()

Be careful as this will raise a :class:`TypeError` if you try to delete
a document that was never saved.

If you were to execute this query directly in mongo Shell, it would
look like:

.. code-block:: javascript

    db.users.remove({_id: ObjectId(...)})

At the time of this writing there appears to be no way to set the
``justOne`` parameter to ``true`` using
:meth:`PyMongo <pymongo:pymongo.collection.Collection.remove>`. If you
decide to remove the unique constraint from the ``_id`` field, bad
things could happen when you use :meth:`~simon.Model.delete`.
