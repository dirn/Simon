try:
    import unittest2 as unittest
except ImportError:
    import unittest


from simon import MongoModel, connection


class TestModel(MongoModel):
    attribute = 2

    class Meta:
        collection = 'test'


class TestBase(unittest.TestCase):
    def setUp(self):
        connection.connect(name='test')

    def test_db(self):
        ("Test that the `db` attribute is associated with classes and "
         "instances.")

        self.assertTrue(hasattr(TestModel._meta, 'db'))

        m = TestModel()
        self.assertTrue(hasattr(m._meta, 'db'))

    def test_delattr(self):
        """Test the `__delattr__()` method."""

        m = TestModel(a=1)
        with self.assertRaises(AttributeError):
            del m.b

        del m.a
        self.assertFalse(hasattr(m, 'a'))
        self.assertFalse('a' in m._document)

        with self.assertRaises(AttributeError):
            del m.attribute

    def test_getattr(self):
        """Test the `__getattr__()` method."""

        m = TestModel(a=1)
        self.assertEqual(m.a, 1)

        with self.assertRaises(AttributeError):
            m.b

        self.assertEqual(m.attribute, 2)

    def test_init(self):
        """Test the `__init__()` method."""

        fields = {'a': 1}
        m = TestModel(**fields)
        self.assertTrue(all(getattr(m, k) == v for k, v in fields.items()))

        self.assertTrue(isinstance(m._document, dict))

    def test_setattr(self):
        """Test the `__setattr__()` method."""

        m = TestModel(a=1)
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
