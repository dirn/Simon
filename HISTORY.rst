History
-------

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
