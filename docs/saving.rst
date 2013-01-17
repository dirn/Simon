Saving
======

There are a couple of different approaches that can be taken when
writing data to a MongoDB database. Simon provides a few different
methods to perform writes to help expose the full power of each.

Document Replacement
--------------------

The basic way to create or update a document is with the
:meth:`~simon.Model.save` method. It will save the document associated
with the instance to the database. If an update is being performed, the
version of the document in the database will be overwritten by the
version associated with the instance. This is known as document
replacement. Any changes made to the version of the document in the
database that have not been introduced to the instance will be lost.

.. code-block:: python

    user = User(name='Simon')
    user.save()

This can be condensed into one step using the
:meth:`~simon.Model.create` method.

.. code-block:: python

    user = User.create(name='Simon')

:meth:`~simon.Model.save` can also be used to save changes to a
document.

.. code-block:: python

    user.email = 'simon@example.com'
    user.save()

The first of these calls to :meth:`~simon.Model.save` will result in an
``insert``. The second will result in an ``update``. In the mongo Shell
they would be written as:

.. code-block:: javascript

    db.users.insert({name: 'Simon'})

    db.users.update({_id: ObjectId(...)}, {email: 'simon@example.com'})


Atomic Updates
--------------

MongoDB also offers a more powerful way to save changes to documents:
atomic updates. By utilizing atomic updates, you can write selective
changes to portions of a document without replacing the whole thing.
Simon provides several different ways to perform atomic updates.

save_fields
  The :meth:`~simon.Model.save_fields` method will perform an atomic
  update updating only the specified fields.

  .. code-block:: python

    # update only the score field
    user.score = 100
    user.save_fields('score')

  You can also update multiple fields at once.

  .. code-block:: python

    user.score = 200
    user.friends = ['Alvin', 'Theodore']
    user.save_fields(['score', 'friends'])

  In the mongo Shell these would be:

  .. code-block:: javascript

    db.users.update({_id: ObjectId(...)}, {$set: {score: 100}})

    db.users.update({_id: ObjectId(...)}, {$set: {score: 200, friends: ['Alvin', 'Theodore']}})

update
  The :meth:`~simon.Model.update` method provides a shortcut to the
  behavior offered by :meth:`~simon.Model.save_fields`.

  .. code-block:: python

    user.update(score=100)

    user.update(score=200, friends=['Alvin', 'Theodore'])

increment
  The :meth:`~simon.Model.increment` method provides a way to increment
  the values of the specified fields. If the field does not exist, it
  will be added with the initial value of ``0``.

  When incrementing only one field, only the name of the field needs to
  be given to :meth:`~simon.Model.increment`. A value can also be
  provided if incrementing by any value other than ``1``.

  .. code-block:: python

    user.increment('score')

    user.increment('score', 100)

  :meth:`~simon.Model.increment` can also be used to increment multiple
  fields at once.

  .. code-block:: python

    user.increment(score=100, level=1)

  The equivalent queries in the mongo Shell would be:

  .. code-block:: javascript

    db.users.update({_id: ObjectId(...)}, {$inc: {score: 1}})

    db.users.update({_id: ObjectId(...)}, {$inc: {score: 100}})

    db.users.update({_id: ObjectId(...)}, {$inc: {score: 100, level: 1}})

remove_fields
  The :meth:`~simon.Model.remove_fields` method will remove the
  specified fields from the document in the database.

  Using it works just like :meth:`~simon.Model.save_fields`.

  .. code-block:: python

    user.remove_fields('level')

    user.remove_fields(['level', 'friends'])

  To execute these same queries in the mongo Shell:

  .. code-block:: javascript

    db.users.update({_id: ObjectId(...)}, {$unset: {level: 1}})

    db.users.update({_id: ObjectId(...)}, {$unset: {level: 1, friends: 1}})

raw_update
  The :meth:`~simon.Model.raw_update` method allows any update query to
  be specified.

  This method will let you execute any update that can't appropriately
  be expressed through one of the other methods. Just make sure you use
  it with caution as Simon can do little to protect you.

  .. code-block:: python

    user.raw_update({'$set': {'level': 1}, '$inc': {'score': 100}, '$unset': {'friends': 1}})

  This query would be passed through to MongoDB as:

  .. code-block:: javascript

    db.users.update({_id: ObjectId(...)}, {$set: {level: 1}, $inc: {score: 100}, $unset: {friends: 1}})


Write Concern
-------------

When Simon was first started, the default behavior with MongoDB was to
perform writes without write concern. This led to faster performance but
had the potential for data loss. Queries performed with write concern
enabled will request the result of ``getLastError()`` before returning
execution to the application. More information is available in the
`MongoDB Docs`_.

.. _MongoDB Docs: http://docs.mongodb.org/manual/core/write-operations/#write-concern

Simon was built with respect for this behavior as the default. All of
the methods discussed above as well as :meth:`~simon.Model.delete`
accept an argument called ``safe`` that can override the default
behavior.

.. code-block:: python

    user = User(name='Simon')
    user.save(safe=True)

    user.update(email='simon@example.com', safe=True)

    user.delete(safe=True)

This also applies to the :meth:`~simon.Model.get_or_create` method
discussed in :doc:`querying`.
