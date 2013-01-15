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


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

