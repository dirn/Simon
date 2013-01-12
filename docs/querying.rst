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

The full list of operators available is:

gt
  Matches documents where the field's value is greater than the
  specified value.

gte
  Matches documents where the field's value is greater than or equal to
  the specified value.

lt
  Matches documents where the field's value is less than the specified
  value.

lte
  Matches documents where the field's value is less than or equal to the
  specified value.

ne
  Matches documents where the field's value is not equal to the
  specified value.

in
  Matches documents where the field's value is equal to any of the
  values in the specified list.

nin
  Matches documents where the field's value is not equal to any of the
  values in the specified list.

all
  Matches documents where the field holds a list containing all of the
  specified elements.


Logical Operators
-----------------

    * ``$and``
    * ``$or``
    * ``$not``


Element Operators
-----------------

    * ``$exists``


Array Operators
---------------

    * ``$size``


Geospatial Queries
------------------

    * ``$near``
