============================
Simon: Simple MongoDB Models
============================

.. image:: https://secure.travis-ci.org/dirn/Simon.png?branch=develop
   :target: http://travis-ci.org/dirn/Simon


Getting Started
===============

Define a model::

    from simon import Model


    class User(Model):
        """A model to represent users"""

Connect to a database::

    from simon.connection import connect

    connect('localhost', name='simon')

And start using it::

    User.create(name='Simon')

    user = User.get(name='Simon')
    user.email = 'simon@example.com'
    user.save()

Full documentation can be found on `Read the Docs`_.

.. _Read the Docs: http://simon.readthedocs.org


Installation
============

Installing Simon is easy::

    pip install Simon

or download the source and run::

    python setup.py install
