"""Helper utilities"""

__all__ = ('get_nested_key', 'map_fields', 'parse_kwargs',
           'remove_nested_key', 'update_nested_keys')

import collections


# The logical operators are needed when mapping fields. The values
# could have been obtained through Q.AND and Q.OR, but I don't want
# the utils module to depend on other modules.
_logicals = ('$and', '$or')


def get_nested_key(values, key):
    """Gets a value for a nested dictionary key.

    This method can be used to retrieve the value nested within a
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

    .. versionadded:: 0.1.0
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


def map_fields(cls, fields, with_comparisons=False, flatten_keys=False):
    """Maps attribute names to document keys.

    Attribute names will be mapped to document keys using
    ``cls._meta.field_map``. If any of the attribute names contain
    ``__``, :meth:`parse_kwargs` will be called and a second pass
    through ``cls._meta.field_map`` will be performed.

    The two-pass approach is used to allow for keys in embedded
    documents to be mapped. Without the first pass, only keys of the
    root document could be mapped. Without the second pass, only keys
    that do not contain embedded document could be mapped.

    The ``$and`` and ``$or`` operators cannot be mapped to different
    keys. Any occurrences of these operators as keys should be
    accompanied by a ``list`` of ``dict``s. Each ``dict`` will be put
    back into :meth:`map_fields` to ensure that keys nested within
    boolean queries are mapped properly.

    If ``with_comparisons`` is set, the following comparison operators
    will be checked for and included in the result:

    * ``gt`` the key's value is greater than the value given
    * ``gte`` the key's value is greater than or equal to the value
      given
    * ``lt`` the key's value is less than the value given
    * ``lte`` the key's value is less than or equal to the value given
    * ``ne`` the key's value is not equal to the value given
    * ``all`` the key's value matches all values in the given list
    * ``in`` the key's value matches a value in the given list
    * ``nin`` the key's value is not within the given list

    If ``flatten_keys`` is set, all keys will be kept at the top level
    of the result dictionary, using a ``.`` to separate each part of a
    key. When this happens, the second pass will be omitted.

    :param cls: A subclass of :class:`~simon.MongoModel`.
    :type cls: type.
    :param fields: Key/value pairs to be used for queries.
    :type fields: dict.
    :param with_comparisons: Whether or not to process comparison
                             operators.
    :type with_comparisons: bool.
    :param flatten_keys: Whether to allow the nested keys to be nested.
    :type flatten_keys: bool.
    :returns: dict -- key/value pairs renamed based on ``cls``'s
              ``field_map`` mapping.

    .. versionadded:: 0.1.0
    """

    if with_comparisons:
        operators = ('all', 'exists', 'gt', 'gte', 'in', 'lt', 'lte', 'ne',
                     'nin')

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
                v = [map_fields(cls=cls, fields=x,
                                with_comparisons=with_comparisons,
                                flatten_keys=flatten_keys) for x in v]
        else:
            if '__' in k:
                second_pass = True
            # If the attribute contains __, use a . instead as this is
            # the syntax for mapping embedded keys. If a . exists in the
            # new key, second_pass should be set to True because
            # parse_kwargs() should be run
            k = cls._meta.field_map.get(k.replace('__', '.'), k)
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
            k = cls._meta.field_map.get(k, k)

            if isinstance(v, collections.Mapping) and k in ('$and', '$or'):
                v = map_fields(cls=cls, fields=v,
                               with_comparisons=with_comparisons,
                               flatten_keys=flatten_keys)

            mapped_fields[k] = v

    return mapped_fields


def parse_kwargs(**kwargs):
    """Parses embedded documents from dictionary keys.

    This takes a kwargs dictionary whose keys contain ``__`` and convert
    them to a new dictionary with new keys created by splitting the
    originals on the ``__``.

    :param kwargs: Keyword arguments to parse.
    :type kwargs: kwargs.
    :returns: dict -- dictionary with nested keys generated from the
              names of the arguments.

    .. versionadded:: 0.1.0
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
    """Removes keys within a nested dictionary.

    This method can remove a key from within a nested dictionary. Nested
    keys should be specified using a ``.`` as the delimiter. If no
    delimiter is found, the key will be removed from the root
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

    .. versionadded:: 0.1.0
    """

    if not isinstance(original, collections.Mapping):
        raise TypeError('`original` must be a `dict`.')

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


def update_nested_keys(original, updates):
    """Updates keys within nested dictionaries.

    This method simulates merging two dictionaries. It allows specific
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

    .. versionadded:: 0.1.0
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
