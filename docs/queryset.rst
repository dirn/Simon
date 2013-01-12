The QuerySet Class
==================

Unlike :meth:`~simon.Model.get`, :meth:`~simon.Model.all` and
:meth:`~simon.Model.find` return an instance of
:class:`~simon.query.QuerySet`. The :class:`~simon.query.QuerySet` class
utilizes PyMongo :class:`cursors <pymongo:pymongo.cursor.Cursor>` to
limit the amount of data that is actually transferred from the database.

Additionally it also exposes a few additional methods for controlling
the database that is returned.

Full documentation for :class:`~simon.query.QuerySet` is available in
:doc:`api`.


Sorting
-------

Instances of :class:`~simon.query.QuerySet` can be sorted through the
:meth:`~simon.query.QuerySet.sort` method. It is called by passing in a
series of field names, each one optionally prefixed by a ``-`` to denote
that the field should be sorted in descending order.

If you sort by a field that doesn't exist in all documents, a document
without the field will be treated as if it has a value less than that of
a document that has the field.

.. code-block:: python

    # sort by name ascending
    users = User.all().sort('name')

Sorting by multiple fields is just as easy.

.. code-block:: python

    # sort by name and email ascending
    users = User.all().sort('name', 'email')

    # sort by name ascending and email descending
    users = User.all().sort('name', '-email')

When sorting by multiple fields, the direction of one field's sort will
not affect the direction of other sorts.

.. code-block:: python

    # sort by name ascending, email descending, and date_of_birth ascending
    users = User.all().sort('name', '-email', 'date_of_birth')

Here are the queries in the mongo Shell:

.. code-block:: javascript

    users = db.users.find().sort({name: 1})

    users = db.users.find().sort({name: 1, email: 1})

    users = db.users.find().sort({name: 1, email: -1})

    users = db.users.find().sort({name: 1, email: -1, date_of_birth: 1})


Limiting
--------

When querying for documents, you may only want a subset of the documents
that match your query. Simon allows you to control this through two
methods, :meth:`~simon.query.QuerySet.limit` and
:meth:`~simon.query.QuerySet.skip`. These allow you to control the
number of documents returned and the number of documents to omit.

.. code-block:: python

    # retrieve the first 10 documents
    users = User.all().limit(10)

    # skip the first 10 documents
    users = User.all().skip(10)

:meth:`~simon.query.QuerySet.limit` and
:meth:`~simon.query.QuerySet.skip` can be chained together to create
paged results.

.. code-block:: python

    # retrieve the second page of 10 documents
    users = User.all().limit(10).skip(10)

The methods can be used in any order.

.. code-block:: python

    # retrieve the second page of 10 documents
    users = User.all().skip(10).limit(10)

Here are the queries in the mongo Shell:

.. code-block:: javascript

        users = db.users.find().limit(10)

        users = db.users.find().skip(10)

        users = db.users.find().limit(10).skip(10)

        users = db.users.find().skip(10).limit(10)


Distinct
--------

It is possible to get a list of unique values for a single field using
:meth:`~simon.query.QuerySet.distinct`.

.. code-block:: python

    # get a list of all email addresses for users named Simon
    emails = User.find(name='Simon').distinct('email')

Unlike Simon, the same query in the mongo Shell is handled at the
collection level:

.. code-block:: javascript

    names = db.users.distinct('email', {name: 'Simon'})


Length
------

Sometimes all you need is how many documents match your query. Simon
provides that information in :meth:`~simon.query.QuerySet.count`.

.. code-block:: python

    count = User.all().count()

Simon makes sure that the any calls to
:meth:`~simon.query.QuerySet.limit` and
:meth:`~simon.query.QuerySet.skip` are factored in. Executing the same
thing in mongo Shell would look like:

.. code-block:: javascript

    count = db.users.find().count(true)
