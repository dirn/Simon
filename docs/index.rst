Simon: Simple MongoDB Models
============================

Meet Simon. He wants to help you create simple MongoDB models.


Overview
--------

Simon is a model library for MongoDB. It aims to introduce the features
of MongoDB and PyMongo in a Pythonic way. It allows you to work with
objects and methods instead of only allowing you to work with everything
as a ``dict``.

Simon tries emphasize the flexibility and power of MongoDB. It does this
in a couple of ways. First, unlike other libraries of its kind, Simon
does not enforce a schema on your documents. This allows you to take
advantage of the dynamic schemas offered by MongoDB. Second, while Simon
allows you to perform traditional saves, it also allows you full control
over performing atomic updates. This is covered in greater detail in the
:doc:`usage` section.


.. toctree::
   :maxdepth: 2

   quickstart
   usage
   queryset
   querying
   saving
   connection
   meta
   api


Installation
------------

To install the latest stable version of Simon::

    $ pip install Simon

or, if you must::

    $ easy_install Simon

To install the latest development version::

    $ git clone git@github.com:dirn/Simon.git
    $ cd Simon
    $ python setup.py install


Further Reading
---------------

For more information, check out the `PyMongo docs`_ and the
`MongoDB docs`_.

.. _MongoDB docs: http://www.mongodb.org/display/DOCS/Home
.. _PyMongo docs: http://api.mongodb.org/python/current/


Changelog
---------

0.6.0 (2013-03-19)
++++++++++++++++++

- Add support for typed fields
- Add support for non Object ID values for ``_id``
- Deprecate ``safe``
- Add ``meta`` module and place ``Meta`` in it
- Only apply sort when documents are retrieved

0.5.0 (2013-02-21)
++++++++++++++++++

- Add ``pop()`` method
- Add ``pull()`` method
- Add ``push()`` method
- Add ``rename()`` method
- Add support for ``$elemMatch`` operator

0.4.0 (2013-02-12)
++++++++++++++++++

- ``created`` will be set for all inserted documents whose model has
  ``auto_timestamp`` set to ``True``
- Fix ``create()`` bug

0.3.0 (2013-02-11)
++++++++++++++++++

- Deprecate ``Model.get()`` and ``Model.find()`` argument ``qs`` in
  favor of ``q``
- Correctly specify write concern depending on version of PyMongo
- Refactor database interaction
- Bug fixes


0.2.0 (2013-02-03)
++++++++++++++++++

- Change ``connection.connect()`` argument from ``replicaSet`` to
  ``replica_set``
- Add equality comparisons for models
- Add support for required fields
- Use write concern by default

0.1.0 (2013-01-18)
++++++++++++++++++

- Initial release


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

