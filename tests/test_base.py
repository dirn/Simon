try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import Model, connection


class TestModel1(Model):
    attribute = 2

    class Meta:
        collection = 'test-simon'
        database = 'test-simon-mock'
        field_map = {'x': 'y'}


class TestModel2(Model):
    def __str__(self):
        return 'this is the __str__'

    def __unicode__(self):
        return u'this is the __unicode__'


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # Use a dummy connection so that these tests can be run even
        # when there is no MongoDB server. In order to do that, the
        # connection module needs _databases to be a dictionary--
        # this would normally be done during the first call to
        # connect(), but there's no guarantee that will have happened--
        # and it needs the connection placed into it
        if not isinstance(connection._databases, dict):
            connection._databases = {}

        if 'test-simon-mock' not in connection._databases:
            connection._databases['test-simon-mock'] = {
                'test-simon': None,
            }

    def test_contains(self):
        """Test the `__contains__()` method."""

        m = TestModel1()
        m._document['a'] = 1
        m._document['b'] = 2

        self.assertTrue('a' in m)
        self.assertTrue('b' in m)
        self.assertFalse('c' in m)

    def test_contains_field_map(self):
        """Test the `__contains__()` method with a mapped field."""

        m = TestModel1()
        m._document['y'] = 1

        self.assertTrue('x' in m)
        self.assertTrue('y' in m)
        self.assertFalse('x' in m._document)

    def test_db(self):
        ("Test that the `db` attribute is associated with classes and "
         "instances.")

        self.assertTrue(hasattr(TestModel1._meta, 'db'))

        m = TestModel1()
        self.assertTrue(hasattr(m._meta, 'db'))

    def test_delattr(self):
        """Test the `__delattr__()` method."""

        m = TestModel1()
        m._document['a'] = 1
        with self.assertRaises(AttributeError):
            del m.b

        del m.a
        self.assertFalse(hasattr(m, 'a'))
        self.assertFalse('a' in m._document)

        with self.assertRaises(AttributeError):
            del m.attribute

    def test_delattr_field_map(self):
        """Test the `__delattr__()` method with a mapped field."""

        m = TestModel1()
        m._document['y'] = 1

        del m.x
        self.assertFalse('y' in m._document)

    def test_getattr(self):
        """Test the `__getattr__()` method."""

        m = TestModel1()

        m._document['a'] = 1
        self.assertEqual(m.a, 1)

        with self.assertRaises(AttributeError):
            m.b

        m._document['c'] = {'d': 1}
        self.assertEqual(m.c, {'d': 1})
        self.assertEqual(m.c__d, 1)
        self.assertEqual(getattr(m, 'c.d'), 1)

        with self.assertRaises(KeyError):
            m.c__e

        self.assertEqual(m.attribute, 2)

    def test_getattr_field_map(self):
        """Test the `__getattr__()` method with a mapped field."""

        m = TestModel1()

        m._document['y'] = 1

        self.assertEqual(getattr(m, 'x'), 1)

    def test_init(self):
        """Test the `__init__()` method."""

        fields = {'a': 1}
        m = TestModel1(**fields)
        self.assertTrue(all(getattr(m, k) == v for k, v in fields.items()))

        self.assertTrue(isinstance(m._document, dict))

    def test_repr(self):
        """Test the `__repr__()` method."""

        m = TestModel1()

        expected = '<TestModel1: TestModel1 object>'
        actual = '{0!r}'.format(m)
        self.assertEqual(actual, expected)

        m2 = TestModel2()

        expected = '<TestModel2: this is the __str__>'
        actual = '{0!r}'.format(m2)
        self.assertEqual(actual, expected)

    def test_setattr(self):
        """Test the `__setattr__()` method."""

        m = TestModel1()
        m._document['a'] = 1
        self.assertFalse('b' in m._document)
        with self.assertRaises(AttributeError):
            m.b

        m.b = 2
        self.assertTrue('b' in m._document)
        self.assertEqual(m.b, 2)
        self.assertEqual(m.b, m._document['b'])

        m.attribute = 3
        self.assertEqual(m.attribute, 3)
        self.assertFalse('attribute' in m._document)

        with self.assertRaises(AttributeError):
            m._meta = 'this better not work'

    def test_setattr_field_map(self):
        """Test the `__setattr__()` method with a mapped field."""

        m = TestModel1()
        setattr(m, 'x', 1)

        self.assertTrue('y' in m._document)
        self.assertEqual(m._document['y'], 1)

    def test_str(self):
        """Test the `__str__()` method."""

        m = TestModel1()

        expected = 'TestModel1 object'
        actual = '{0!s}'.format(m)
        self.assertEqual(actual, expected)

        m2 = TestModel2()

        expected = 'this is the __str__'
        actual = '{0!s}'.format(m2)
        self.assertEqual(actual, expected)

    def test_unicode(self):
        """Test the `__unicode__()` method."""

        m = TestModel1()

        expected = u'TestModel1 object'
        actual = u'{0}'.format(m)
        self.assertEqual(actual, expected)

        m2 = TestModel2()

        expected = u'this is the __unicode__'
        actual = u'{0}'.format(m2)
        self.assertEqual(actual, expected)
