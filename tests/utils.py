"""Helpers for running tests"""

from bson import ObjectId
import pymongo
from simon import base

__all__ = ('AN_OBJECT_ID', 'AN_OBJECT_ID_STR', 'ModelFactory',)

AN_OBJECT_ID_STR = '50d4dce70ea5fae6fb84e44b'
AN_OBJECT_ID = ObjectId(AN_OBJECT_ID_STR)


def ModelFactory(name, spec=None, **kwargs):
    """Creates a model on the fly.

    .. note::
       This is meant for testing purposes only and is not intended for
       production use.

    ``ModelFactory`` can be used to create a class at run time. Items
    passed in through ``**kwargs`` will be added either to the model
    class or, when appropriate, its ``Meta`` options class.

    :param name: The name of the model class.
    :type name: str.
    :param spec: (optional) The model to inherit from.
    :type spec: :class:`~simon.Model`.
    :param \*\*kwargs: Attributes and options to associate with the
                       model.
    :type \*\*kwargs: \*\*kwargs.
    :returns: :class:`type` -- the new model class.

    .. versionadded:: 0.4.0
    """

    # Set the inheritance
    if spec:
        spec = (spec,)
    else:
        spec = ()
    spec += (base.Model,)

    # Create the type
    cls = type(name, spec, {'__module__': base})

    if spec[0] != base.Model:
        for k, v in spec[0]._meta.__dict__.items():
            if not (k.startswith('_') or k == 'core_attributes'):
                setattr(cls._meta, k, v)

    # Add the attributes
    for k, v in kwargs.items():
        if k == 'typed_fields':
            if '_id' not in v:
                v['_id'] = ObjectId
        elif k in ('safe', 'w'):
            # write concern is unique
            k = 'write_concern'
            if pymongo.version_tuple[:2] >= (2, 4):
                f = int
            else:
                f = bool
            k = 'write_concern'
            v = f(v)

        if not k.startswith('__') and hasattr(cls._meta, k):
            # Only update the Meta class for attributes that do not
            # begin with __. This allows attributes such as __str__
            # and __unicode__ to be updated on the model class. If this
            # weren't intended for testing purposes only, this could be
            # a problem.
            setattr(cls._meta, k, v)
        else:
            if k not in cls._meta.core_attributes:
                # Make sure to add the attribute to Meta.core_attributes
                # so that __setattr__() will handle it correctly.
                cls._meta.core_attributes += (k,)
            setattr(cls, k, v)

    return cls
