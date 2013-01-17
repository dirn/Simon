Querying
========

MongoDB offers a lot of flexibility when querying for documents in the
database. Simon tries to expose that flexibility in easy to use ways.

To use one of MongoDB's operators, append it to the name of the field
you want to apply it to, separated by a double underscore (``__``).
Simon will automatically translate it into the correct query syntax.

.. code-block:: python

    # match users where name is not equal to 'Simon'
    users = User.find(name__ne='Simon')

    # match users where score is greater than 1000
    users = User.find(score__gt=1000)

    # match users created in 2012
    from datetime import datetime
    jan_1_2012 = datetime(2012, 1, 1)
    jan_1_2013 = datetime(2013, 1, 1)
    users = User.find(created__gte=jan_1_2012, created__lt=jan_1_2013)

There's queries will be translated to:

.. code-block:: javascript

    users = db.users.find({name: {$ne: 'Simon'}})

    users = db.users.find({score: {$gt: 1000}})

    jan_1_2012 = new Date(2012, 1, 1)
    jan_1_2013 = new Date(2013, 1, 1)
    users = db.users.find({created: {$gte: jan_1_2012, $lt: jan_1_2013}})

More information about all of the operators offered by MongoDB is
available in the `MongoDB docs`_.

.. _MongoDB docs: http://docs.mongodb.org/manual/reference/operators/


Comparison Operators
--------------------

The full list of comparison operators available is:

gt
  Matches documents where the field's value is greater than the
  specified value.

  .. code-block:: python

    users = User.find(score__gt=1000)

gte
  Matches documents where the field's value is greater than or equal to
  the specified value.

  .. code-block:: python

    users = User.find(score__gte=1000)

lt
  Matches documents where the field's value is less than the specified
  value.

  .. code-block:: python

    users = User.find(score__lt=1000)

lte
  Matches documents where the field's value is less than or equal to the
  specified value.

  .. code-block:: python

    users = User.find(score__lte=1000)

ne
  Matches documents where the field's value is not equal to the
  specified value.

  .. code-block:: python

    users = User.find(name__ne='Simon')

in
  Matches documents where the field's value is equal to any of the
  values in the specified list.

  .. code-block:: python

    users = User.find(name__in=['Alvin', 'Simon', 'Theodore'])

nin
  Matches documents where the field's value is not equal to any of the
  values in the specified list.

  .. code-block:: python

    users = User.find(name__nin=['Alvin', 'Simon', 'Theodore'])

all
  Matches documents where the field holds a list containing all of the
  specified elements.

  .. code-block:: python

    users = User.find(friends__all=['Alvin', 'Theodore'])


Element Operators
-----------------

The full list of element operators available is:

exists
  Matches documents where the field's existence matches the specified
  value.

  .. code-block:: python

    users = User.find(email__exists=True)


Array Operators
---------------

The full list of array operators available is:

size
  Matches documents where the field is a list of the specified length.

  .. code-block:: python

    users = User.find(fields__size=2)


Geospatial Operators
--------------------

One of the most powerful ways to query with MongoDB is through
geospatial querying. Unlike the operators discussed thus far, Simon
exposes the geospatial operators through convenience methods that help
harness the full potential of each operator.

Before you can use any of these operators, you will need to create a
:data:`two-dimensional index <pymongo:pymongo.GEO2D>`.

.. code-block:: javascript

    db.users.ensureIndex({location: '2d'})

The convenience methods can be used by importing the ``geo`` module.

.. code-block:: python

    from simon import geo

near
  Matches documents from nearest to farthest with respect to the
  specified point.

  .. code-block:: python

    users = User.find(location=geo.near([x, y]))

within
  Matches documents contained within the specified shape.

  .. code-block:: python

    users = User.find(location=geo.within('box', [x1, y1], [x2, y2]))

  While :meth:`~simon.geo.within` can be used on its own, the following
  methods make it even easier.

box
  Matches documents within the specified rectangular shape.

  .. code-block:: python

    users = User.find(location=geo.box([x1, y1], [x2, y2]))

polygon
  Matches documents within the specified polygonal shape.

  .. code-block:: python

    users = User.find(location=geo.polygon([x1, y1], [x2, y2], [x3, y3]))

center
  Matches documents within the specified circular shape. **Note** the
  ``center`` operator is accessed through the :meth:`~simon.geo.circle`
  method.

  .. code-block:: python

    center = [x, y]
    users = User.find(location=geo.circle(center, radius))

Here's a quick run through of these queries in the mongo Shell:

.. code-block:: javascript

    users = db.users.find({location: {$near: [x, y]}})

    users = db.users.find({location: {$within: {$box: [[x1, y1], [x2, y2]]}}})

    users = db.users.find({location: {$within: {$polygon: [[x1, y1], [x2, y2], [x3, y3]]}}})

    users = db.users.find({location: {$within: {$center: [[x, y], radius]}}})

The full list of options offered by each method can be found in the
:ref:`geo` section of :doc:`api`.


Logical Operators
-----------------

Sometimes more complex queries require combining conditions with logical
operators, such as ``AND``, ``OR``, and ``NOT``.

not
  Performs a logical ``NOT`` operation on the specified expression.

  .. code-block:: python

    users = User.find(score__not__gt=1000)

To perform this query in the mongo Shell:

.. code-block:: javascript

    users = db.users.find({score: {$not: {$gt: 1000}}})

Using the ``AND`` and ``OR`` operators with Simon requires the
assistance of :class:`~simon.query.Q` objects. Fortunately they work
just like any other query with Simon. Instead of passing the the query
directly to a method like :meth:`~simon.Model.find`, however, the
query is passed to :class:`~simon.query.Q`.

.. code-block:: python

    from simon.query import Q
    query = Q(name='Simon')

The new object is then combined with one or more additional
:class:`~simon.query.Q` objects, the end result of which is then passed
to :meth:`~simon.Model.find`. :class:`~simon.query.Q` objects are
combined using bitwise and (``&``) and or (``|``) to represent logical
``AND`` and ``OR``, respectively.

.. code-block:: python

    # match users where name is equal to 'Simon' AND score is greater
    # than 1000
    users = User.find(Q(name='Simon') & Q(score__gt=1000))

    # match users where name is equal to 'Simon' AND score is greater
    # than 1000, OR name is either 'Alvin' or 'Theodore'
    users = User.find(Q(name='Simon', score__gt=1000) | Q(name__in=['Alvin', 'Theodore']))

    # match users who have no friends
    users = User.find(Q(friends__exists=False) | Q(friends__size=0))

Any number of :class:`~simon.query.Q` objects can be chained together.
Be careful, however, as chaining together a lot of queries through
different operators can result in deeply nested queries, which may
become inefficient.

Here's how these queries would look in the mongo Shell:

.. code-block:: javascript

    users = db.users.find({$and: [{name: 'Simon'}, {score: {$gt: 1000}}]})

    users = db.users.find({$or: [{name: 'Simon', score: {$gt: 1000}}, {name: {$in: ['Alvin', 'Theodore']}}]})

    users = db.users.find({$or: [{friends: {$exists: false}}, {friends: {$size: 0}}]})


Exceptions
----------

When using :meth:`~simon.Model.get` to retrieve a document, there are
two potential exceptions that may be raised. When one of these
exceptions is raised, it will be raised as part of the model class
being queried.

Assume the following documents for all examples below.

:class:`~simon.exceptions.MultipleDocumentsFound`
  This exception is raised when multiple documents match the specified
  query.

  .. code-block:: python

    User.create(name='Simon', email='simon@example.com')
    User.create(name='Simon', email='simon@example.org')

    try:
        user = User.get(name='Simon')
    except User.MultipleDocumentsFound:
        """Handle the exception here"""
    else:
        """Only one User was found"""

:class:`~simon.exceptions.NoDocumentFound`
  This exception is raised when no documents match the specified query.

  .. code-block:: python

    try:
        user = User.get(name='Alvin')
    except User.NoDocumentFound:
        """Handle the exception here"""
    else:
        """Only one User was found"""

In the case of :class:`~simon.exceptions.NoDocumentFound`, there may be
times when the way to handle the exception would be to create the
document. A common pattern would:

.. code-block:: python

    try:
        user = User.get(name='Simon')
    except User.NoDocumentFound:
        user = User.create(name='Simon')

Rather than making you use this pattern over and over, Simon does it for
you, inside the :meth:`~simon.Model.get_or_create` method. Not only will
:meth:`~simon.Model.get_or_create` do this, it will also let you know if
it had to create the document.

.. code-block:: python

    user, created = User.get_or_create(name='Simon')
    # user will be the newly created document and created will be True

    user, created = User.get_or_create(name='Simon')
    # user will be loaded from the database and created will be False

If multiple documents match the query,
:class:`~simon.exceptions.MultipleDocumentsFound` will still be raised.
