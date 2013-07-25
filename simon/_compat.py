import sys

PY2 = sys.version_info[0] == 2

if PY2:
    _iteritems = 'iteritems'
    _iterkeys = 'iterkeys'
    _itervalues = 'itervalues'

    get_next = lambda x: x.next

    str_types = (str, unicode)

    exec('def reraise(tp, value, tb=None):\n raise tp, value, tb')
else:
    _iteritems = 'items'
    _iterkeys = 'keys'
    _itervalues = 'values'

    get_next = lambda x: x.__next__

    str_types = (str,)

    def reraise(tp, value, tb=None):
        if getattr(value, '__traceback__', tb) is not tb:
            raise value.with_traceback(tb)
        raise value


def iteritems(d, *args, **kwargs):
    return iter(getattr(d, _iteritems)(*args, **kwargs))


def iterkeys(d, *args, **kwargs):
    return iter(getattr(d, _iterkeys)(*args, **kwargs))


def itervalues(d, *args, **kwargs):
    return iter(getattr(d, _itervalues)(*args, **kwargs))


try:
    xrange
except NameError:
    xrange = range


def with_metaclass(meta, *bases):
    # This requires a bit of explanation: the basic idea is to make a
    # dummy metaclass for one level of class instantiation that replaces
    # itself with the actual metaclass.  Because of internal type checks
    # we also need to make sure that we downgrade the custom metaclass
    # for one level to something closer to type (that's why __call__ and
    # __init__ comes back from type etc.).
    #
    # This has the advantage over six.with_metaclass in that it does not
    # introduce dummy classes into the final MRO.
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)
    return metaclass('DummyMetaClass', None, {})
