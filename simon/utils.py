"""Helper utilities"""

__all__ = ('map_fields', 'parse_kwargs', 'update_nested_keys')

import collections


def map_fields(cls, fields, flatten_keys=False):
    """Maps attribute names to document keys.

    Attribute names will be mapped to document keys using
    ``cls._meta.field_map``. If any of the attribute names contain
    ``__``, :meth:`parse_kwargs` will be called and a second pass
    through ``cls._meta.field_map`` will be performed.

    The two-pass approach is used to allow for keys in embedded
    documents to be mapped. Without the first pass, only keys of the
    root document could be mapped. Without the second pass, only keys
    that do not contain embedded document could be mapped.

    If ``flatten_keys`` is set, all keys will be kept at the top level
    of the result dictionary, using a ``.`` to separate each part of a
    key. When this happens, the second pass will be omitted.

    :param cls: A subclass of :class:`~simon.MongoModel`.
    :type cls: type.
    :param fields: Key/value pairs to be used for queries.
    :type fields: dict.
    :param flatten_keys: Whether to allow the nested keys to be nested.
    :type flatten_keys: bool.
    :returns: dict -- key/value pairs renamed based on ``cls``'s
              ``field_map`` mapping.

    .. versionadded:: 0.1.0
    """

    second_pass = False

    # Map the attributes to their cooresponding document keys. If a __
    # is encountered, a second pass will be needed after processing
    # the keys through parse_kwargs()
    mapped_fields = {}
    for k, v in fields.items():
        if '__' in k:
            second_pass = True
        # If the attribute contains __, use a . instead as this is the
        # syntax for mapping embedded keys. If a . exists in the new
        # key, second_pass should be set to True because parse_kwargs()
        # should be run
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


def update_nested_keys(original, updates):
    """Updates keys within nested dictionaries.

    This method simulates merging two dictionaries. It allows specific
    keys within a dictionary or nested dictionary without overwriting
    the the entire dictionary.

    :param original: The original dictionary to be updates.
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
