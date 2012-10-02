"""Custom decorators for Simon

Anything defined here will probably never be needed outside of internal
use.
"""

from functools import wraps

from .connection import get_database


def requires_database(f):
    """Ensures that a model has a reference to the collection in the
    database before trying to use it.
    """

    @wraps(f)
    def _inner(cls_or_self, *args, **kwargs):
        # Add the database
        # This will raise ConnectionError if the database connection
        # hasn't been made yet. If that's this case, calling this
        # first will let us get out early.
        if not hasattr(cls_or_self._meta, 'db'):
            db = get_database(
                cls_or_self._meta.database)[cls_or_self._meta.collection]
            cls_or_self._meta.db = db

        return f(cls_or_self, *args, **kwargs)
    return _inner
