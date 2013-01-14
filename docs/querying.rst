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

    # match users who joined in 2012
    from datetime import datetime
    jan_1_2012 = datetime(2012, 1, 1)
    jan_1_2013 = datetime(2013, 1, 1)
    users = User.find(joined__gte=jan_1_2012, joined__lt=jan_1_2013)

There's queries will be translated to:

.. code-block:: javascript

    users = db.users.find({name: {$ne: 'Simon'}})

    users = db.users.find({score: {$gt: 1000}})

    jan_1_2012 = new Date(2012, 1, 1)
    jan_1_2013 = new Date(2013, 1, 1)
    users = db.users.find({joined: {$gte: jan_1_2012, $lt: jan_1_2013}})

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

    * ``$and``
    * ``$or``
    * ``$not``


