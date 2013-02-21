============================
Simon: Simple MongoDB Models
============================

.. image:: https://secure.travis-ci.org/dirn/Simon.png?branch=develop
   :target: http://travis-ci.org/dirn/Simon


Note About Releases
===================

Simon is not yet considered production-ready. While I will do my best to
maintain backward compatibility between now and version 1.0, I cannot
guarantee it will happen.

The original development releases may eventually be pulled from Read the
Docs and PyPI. You are encouraged to keep your version up-to-date until
the release of 1.0 to avoid running into problems.


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
