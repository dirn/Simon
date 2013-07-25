"""Custom exceptions for Simon models

Most of these exceptions should never be used directly. They are meant
to be nested within each :class:`~simon.base.Model` subclass.

Some of the names and behavior are modeled after SQLAlchemy.
"""


class ConnectionError(BaseException):
    """Raised when a database connection cannot be opened."""


class MultipleDocumentsFound(BaseException):
    """Raised when multiple documents are found when only one is
    expected.
    """


class NoDocumentFound(BaseException):
    """Raised when an object matching a query is not found."""
