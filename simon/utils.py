"""Helper utilities

WARNING
The functionality in this module is intended for internal use by Simon.
If using anything in this module directly, be careful when updating
versions of Simon as no guarantees are made about the backward
compatability of its API.
"""

import collections
from datetime import datetime
import warnings
try:
    # @raymondh added the ignored() context manager in Python 3.4:
    # http://hg.python.org/cpython/rev/406b47c64480.
    # It's awesome and I want to use it...
    from contextlib import ignored
except ImportError:
    from contextlib import contextmanager

    # ... but if this is running on an older version of Python--
    # especially since 3.4 hasn't been released yet at the time of this
    # change--I'm adding it in manually. The following is taken directly
    # from Lib/contextlib.py.

    @contextmanager
    def ignored(*exceptions):
        """Context manager to ignore specifed exceptions

             with ignored(OSError):
                 os.remove(somefile)

        """
        try:
            yield
        except exceptions:
            pass

from bson import ObjectId

from .connection import pymongo_supports_mongoclient

__all__ = ('current_datetime', 'get_nested_key', 'guarantee_object_id',
           'ignored', 'is_atomic', 'map_fields', 'parse_kwargs',
           'remove_nested_key', 'set_write_concern', 'update_nested_keys')


# The logical operators are needed when mapping fields. The values
# could have been obtained through Q.AND and Q.OR, but I don't want
# the utils module to depend on other modules.
_logicals = ('$and', '$or')


def current_datetime():
    """Get the current datetime in UTC formatted for MongoDB.

    Python includes microseconds in its ``datetime`` values. MongoDB,
    on the other hand, only retains them down to milliseconds. This
    function will not only get the current time in UTC, but it will also
    remove microseconds from the value.

    :returns: datetime -- the current datetime formatted for MongoDB.

    .. versionadded:: 0.2.0

    """

    now = datetime.utcnow()
    return now.replace(microsecond=(now.microsecond // 1000 * 1000))


def get_nested_key(values, key):
    """Get a value for a nested dictionary key.

    This function can be used to retrieve the value nested within a
    dictionary. The entire path should be provided as the value for
    ``key``, using a ``.`` as the delimiter
    (e.g., ``'path.to.the.key``').

    If ``key`` does not exist in ``values``, :class:`KeyError` will be
    raised. The exception will be raised in the reverse order of the
    recursion so that the original value is used.

    :param values: The dictionary.
    :type values: dict.
    :param key': The path of the nested key.
    :type key: str.
    :returns: The value associated with the nested key.
    :raises: :class:`KeyError`

    """

    # If key exists in values, return its value, otherwise recurse
    # through key and values until either key is found or there is no
    # key left to try
    if key in values:
        return values[key]

    keys = key.split('.', 1)
    if not (len(keys) == 2 and keys[0] in values and
            isinstance(values[keys[0]], collections.Mapping)):
        raise KeyError(key)

    try:
        return get_nested_key(values[keys[0]], keys[1])
    except KeyError:
        raise KeyError(key)


def guarantee_object_id(value):
    """Convert a value into an Object ID.

    This function will convert a value to an :class:`ObjectId`. If
    ``value`` is a ``dict`` (e.g., with a comparison operator as the key
    ), the value in the ``dict`` will be converted. Any values that are
    a ``list`` or ``tuple`` will be iterated over, and replaced with a
    ``list`` containing all :class:`ObjectId` instances.

    :class:`TypeError` will be raised for any ``value`` that cannot be
    converted to an :class:`ObjectId`. :class:`InvalidId` will be raised
    for any ``value`` that is of the right type but is not a valid value
    for an :class:`ObjectId`.

    Any value of ``None`` will be replaced with a newly generated
    :class:`ObjectId`.

    :param value: the ID.
    :returns: ObjectId or dict -- the Object ID.
    :raises: :class:`TypeError`, :class:`~bson.errors.InvalidId`

    """

    # If it's already an Object ID, get out early.
    if isinstance(value, ObjectId):
        return value

    if isinstance(value, collections.Mapping):
        # Handle dicts.
        for k, v in value.items():
            if isinstance(v, ObjectId):
                # If it's already an Object ID, skip it.
                continue

            if not isinstance(v, (list, tuple)):
                value[k] = ObjectId(v)
                continue

            # Sometimes the dict can contain a list of IDs (e.g., with a
            # comparison operator). When that is the case, all items in
            # the list that are not Object IDs should be converted to
            # them.
            value[k] = [
                x if isinstance(x, ObjectId) else ObjectId(x) for x in v]

    else:
        value = ObjectId(value)

    return value


def is_atomic(document):
    """Check for atomic update operators.

    This function checks for operators in ``document``. If a spec
    document is provided instead of a document to save or update, a
    false positive will be reported if a logical operator such as
    ``$and`` or ``$or`` is used.

    :param document: The document to containing the update.
    :type document: dict.
    :returns: bool -- ``True`` if ``document`` is an atomic
              update.

    .. versionadded:: 0.3.0

    """

    return any(k[0] == '$' for k in document)


def map_fields(field_map, fields, with_operators=False, flatten_keys=False):
    """Map attribute names to document keys.

    Attribute names will be mapped to document keys using the mapping
    specified in ``field_map``. If any of the attribute names contain
    ``__``, :func:`parse_kwargs` will be called and a second pass
    through ``cls._meta.field_map`` will be performed.

    The two-pass approach is used to allow for keys in embedded
    documents to be mapped. Without the first pass, only keys of the
    root document could be mapped. Without the second pass, only keys
    that do not contain embedded document could be mapped.

    The ``$and`` and ``$or`` operators cannot be mapped to different
    keys. Any occurrences of these operators as keys should be
    accompanied by a ``list`` of ``dict``s. Each ``dict`` will be put
    back into :func:`map_fields` to ensure that keys nested within
    boolean queries are mapped properly.

    If ``with_operators`` is set, the following operators will be
    checked for and included in the result:

    * ``$gt`` the key's value is greater than the value given
    * ``$gte`` the key's value is greater than or equal to the value
      given
    * ``$lt`` the key's value is less than the value given
    * ``$lte`` the key's value is less than or equal to the value given
    * ``$ne`` the key's value is not equal to the value given
    * ``$all`` the key's value matches all values in the given list
    * ``$in`` the key's value matches a value in the given list
    * ``$nin`` the key's value is not within the given list
    * ``$exists`` the the key exists
    * ``$near`` the key's value is near the given location
    * ``$size`` the key's value has a length equal to the given value
    * ``$elemMatch`` the key's value is an array satisfying the given
      query

    The following aggregation operators are also supported:

    * ``$addToSet`` returns an unique array of all values found in the
      selected field
    * ``$push`` returns an array of all values found in the selected
      field
    * ``$first`` returns the first value encountered for its group
    * ``$last`` returns the last value encountered for its group
    * ``$max`` returns the hightest value for the field
    * ``$min`` returns the lowest non-null value for the field
    * ``$avg`` returns the average value for the field
    * ``$sum`` returns the sum of values for the field

    To utilize any of the operators, append ``__`` and the name of the
    operator sans the ``$`` (e.g., ``__gt``, ``__lt``) to the name of
    the key::

        map_fields(mapping, {'a__gt': 1, 'b__lt': 2},
                   with_operators=True)

    This will check for a greater than 1 and b less than 2 as::

        {'a': {'$gt': 1}, 'b': {'$lt': 2}}

    The ``$not`` operator can be used in conjunction with any of the
    above operators::

        map_fields(mapping, {'a__gt': 1, 'b__not__lt': 2},
                   with_operators=True)

    This will check for a greater than 1 and b not less than 2 as::

        {'a': {'$gt': 1}, 'b': {'$not': {'$lt': 2}}}

    If ``flatten_keys`` is set, all keys will be kept at the top level
    of the result dictionary, using a ``.`` to separate each part of a
    key. When this happens, the second pass will be omitted.

    :param field_map: Key/value pairs defining the field map.
    :type field_map: dict.
    :param fields: Key/value pairs to be used for queries.
    :type fields: dict.
    :param with_operators: (optional) Whether or not to process
                             operators.
    :type with_operators: bool.
    :param flatten_keys: (optional) Whether to allow the nested keys to
                         be nested.
    :type flatten_keys: bool.
    :returns: dict -- key/value pairs renamed based on ``cls``'s
              ``field_map`` mapping.

    .. versionchanged:: 0.7.0
       ``$group`` operators are supported
       All lowercase operators are supported

    """

    if with_operators:
        operators = ('addToSet', 'addtoset', 'all', 'avg', 'elemMatch',
                     'elemmatch', 'exists', 'first', 'gt', 'gte', 'in', 'last',
                     'lt', 'lte', 'max', 'min', 'ne', 'near', 'nin', 'push',
                     'size', 'sum')
        operators_cased = {'addtoset': 'addToSet', 'elemmatch': 'elemMatch'}

        for k, v in fields.items():
            # To figure out if a key includes an operator, split it
            # into two pieces. The first piece will be the actual key
            # and the second will be the operator.
            operator = k.rsplit('__', 1)

            if len(operator) == 2:
                if operator[1] in operators:
                    # If there is an operator, add the actual key to
                    # the fields dictionary, giving it a value that is
                    # a dictionary using the MongoDB operator as its key
                    # and remove the original key and operator
                    # combination from the fields dictionary.
                    if operator[0].endswith('__not'):
                        # If $not is being used, the query needs to be
                        # restructured a little, so let's trick the
                        # line that adds the operator to the query.
                        v = {'${0}'.format(operator[1]): v}
                        operator = operator[0].rstrip('__not'), 'not'

                    # Operators are case sensitive as camel case.
                    # Because the operators are intended to be used as
                    # kwargs, writing them as such will appear out of
                    # place. This check allows the operators to be
                    # specified using lowercase.
                    if operator[1] in operators_cased:
                        operator[1] = operators_cased[operator[1]]

                    fields[operator[0]] = {'${0}'.format(operator[1]): v}
                    del fields[k]

    second_pass = False

    # Map the attributes to their cooresponding document keys. If a __
    # is encountered, a second pass will be needed after processing
    # the keys through parse_kwargs()
    mapped_fields = {}
    for k, v in fields.items():
        if k in _logicals:
            # If the key is one of the logical operators, it shouldn't
            # be mapped to something. Instead, it should contain a list
            # of dictionaries. Each of those dictionaries should be fed
            # back into map_fields() and a new list should be build
            # from the mapped dictionaries.
            if isinstance(v, list):
                v = [map_fields(field_map=field_map, fields=x,
                                with_operators=with_operators,
                                flatten_keys=flatten_keys) for x in v]
        else:
            if '__' in k:
                second_pass = True
            # If the attribute contains __, use a . instead as this is
            # the syntax for mapping embedded keys. If a . exists in the
            # new key, second_pass should be set to True because
            # parse_kwargs() should be run
            k = field_map.get(k.replace('__', '.'), k)
            if '.' in k:
                second_pass = True

        mapped_fields[k.replace('.', '__')] = v

    if flatten_keys:
        # Flattened keys are not nested and are written out with .s as
        # the delimiter between each level
        fields = {}
        for k, v in mapped_fields.items():
            # A __ at either the beginning or end of a key name should
            # not be replaced by a ., to prevent this from happening,
            # the replacement should only happen on the characters
            # between the first and last ones.
            #
            # If a key is only one character long, there is obviously
            # no chance of a __ and therefore no need to process the
            # key. It's important to check for this because k[0] and
            # k[-1] would both return the single character.
            if len(k) > 1:
                k = ''.join([k[0], k[1:-1].replace('__', '.'), k[-1]])
            fields[k] = v
        mapped_fields = fields
    elif second_pass:
        # At this point a second pass is needed, put the fields through
        # the kwarg parser and then see if any of the top level fields
        # need to be mapped
        fields = parse_kwargs(**mapped_fields)

        mapped_fields = {}
        for k, v in fields.items():
            k = field_map.get(k, k)

            mapped_fields[k] = v

    return mapped_fields


def parse_kwargs(**kwargs):
    """Parse embedded documents from dictionary keys.

    This takes a kwargs dictionary whose keys contain ``__`` and convert
    them to a new dictionary with new keys created by splitting the
    originals on the ``__``.

    :param \*\*kwargs: Keyword arguments to parse.
    :type \*\*kwargs: \*\*kwargs.
    :returns: dict -- dictionary with nested keys generated from the
              names of the arguments.

    """

    parsed_kwargs = {}

    # Iterate through the original keys, if one contains __ in the
    # middle, split the key into two parts. Use the first as a new key
    # containing a dictionary in parsed_kwargs. Use the second as a key
    # in that dictionary, retaining the original value. If there is no
    # __ in the middle of the key, place it right into parsed_kwargs
    for k, v in kwargs.items():
        if '__' in k and not (k.startswith('__') or k.endswith('__')):
            parent, embedded = k.split('__', 1)

            if parent not in parsed_kwargs:
                parsed_kwargs[parent] = {}

            parsed_kwargs[parent][embedded] = v
        else:
            parsed_kwargs[k] = v

    # After the above has completed, recursively loop through all nested
    # dictionaries looking for more keys with __ in them
    for k, v in parsed_kwargs.items():
        if isinstance(v, dict):
            parsed_kwargs[k] = parse_kwargs(**v)

    return parsed_kwargs


def remove_nested_key(original, key):
    """Remove keys within a nested dictionary.

    This function can remove a key from within a nested dictionary.
    Nested keys should be specified using a ``.`` as the delimiter. If
    no delimiter is found, the key will be removed from the root
    dictionary.

    If ``original`` is not a dictionary, a :class:`TypeError` will be
    raised. If ``key`` doesn't exist in ``original``, a
    :class:`KeyError` will be raised.

    :param original: The original dictionary to be updated.
    :type original: dict.
    :param key: The key to be removed.
    :type key: str.
    :returns: dict -- the updated dictionary
    :raises: :class:`TypeError`, :class:`KeyError`

    """

    if not isinstance(original, collections.Mapping):
        raise TypeError("'original' must be a dict.")

    if '.' in key:
        # If the key contains a . it is considered nested. Split the key
        # into two parts. Using the first, find the key that matches the
        # first part, pass it and the second part of the original key to
        # the function recursively
        parts = key.split('.', 1)
        original[parts[0]] = remove_nested_key(original[parts[0]], parts[1])
    else:
        # When there is no ., remove the key
        del original[key]

    return original


def set_write_concern_as_safe(options, force_write_concern):
    """Set the safe parameter for write concern.

    If ``force_write_concern`` is ``True``, write concern will be used.

    :param options: The potential write concern settings.
    :type options: dict.
    :param force_write_concern: A value to override ``options`` with.
    :type force_write_concern: bool.

    .. versionadded:: 0.6.0

    """

    safe = options.pop('safe', None)
    w = options.pop('w', None)

    if force_write_concern:
        safe = True
    else:
        if safe is None and w:
            safe = True
        else:
            safe = safe or False
    options['safe'] = safe


def set_write_concern_as_w(options, force_write_concern):
    """Set the w parameter for write concern.

    If ``force_write_concern`` is a value greater than zero, write
    concern will be used.

    :param options: The potential write concern settings.
    :type options: dict.
    :param force_write_concern: A value to override ``options`` with.
    :type force_write_concern: int.

    .. versionadded:: 0.6.0

    """

    if 'safe' in options and options['safe'] is not None:
        message = 'safe has been deprecated. Please use w instead.'
        warnings.warn(message, DeprecationWarning)

    safe = options.pop('safe', None)
    w = options.pop('w', None)

    if force_write_concern:
        # use int() in case force_write_concern is True
        w = int(force_write_concern)
    else:
        if w is None and safe:
            w = 1
        else:
            w = w or 0
    options['w'] = w


# Export the relevant function as of set_write_concern()
if pymongo_supports_mongoclient:
    set_write_concern = set_write_concern_as_w
else:
    set_write_concern = set_write_concern_as_safe


def update_nested_keys(original, updates):
    """Update keys within nested dictionaries.

    This function simulates merging two dictionaries. It allows specific
    keys within a dictionary or nested dictionary without overwriting
    the the entire dictionary.

    If either ``original`` or ``updates`` is not a dictionary, a
    :class:`TypeError` will be raised.

    :param original: The original dictionary to be updated.
    :type original: dict.
    :param updates: The dictionary with updates to apply.
    :type updates: dict.
    :returns: dict -- the updated dictionary.
    :raises: :class:`TypeError`

    """

    # Based on http://stackoverflow.com/questions/3232943/

    if not (isinstance(original, collections.Mapping) and
            isinstance(updates, collections.Mapping)):
        raise TypeError('The values for both `original` and `updates` must be '
                        '`dict`s.')

    for k, v in updates.items():
        if isinstance(v, collections.Mapping):
            updated = update_nested_keys(original.get(k, {}), v)
            original[k] = updated
        else:
            original[k] = updates[k]
    return original
