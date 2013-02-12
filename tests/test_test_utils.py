try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import Model
from simon.base import Meta
from .utils import ModelFactory


class TestModelFactory(unittest.TestCase):
    def test_modelfactory(self):
        """Test the `ModelFactory()` method."""

        TestModel = ModelFactory('TestModel')
        self.assertIsInstance(TestModel(), TestModel)
        self.assertIsInstance(TestModel(), Model)

        TestModel2 = ModelFactory('TestModel2')
        self.assertIsInstance(TestModel2(), TestModel2)
        self.assertIsInstance(TestModel2(), Model)

    def test_attribute(self):
        ("Test the `ModelFactory()` method with an attribute that isn't"
         " part of `Meta`.")

        TestModel = ModelFactory('TestModel', spam=True)
        self.assertFalse(hasattr(TestModel._meta, 'spam'))
        self.assertTrue(hasattr(TestModel, 'spam'))

    def test_meta(self):
        """Test that `ModelFactory()` classes have a `Meta` class."""

        TestModel = ModelFactory('TestModel')
        self.assertTrue(hasattr(TestModel, '_meta'))
        self.assertIsInstance(TestModel._meta, Meta)

        class AModel(Model):
            pass

        self.assertTrue(all(hasattr(TestModel._meta, k) for k in
                            AModel._meta.__dict__.keys()
                            if not k.startswith('__')))

    def test_override(self):
        """Test that `ModelFactory()` overrides default attributes."""

        TestModel = ModelFactory('TestModel', auto_timestamp=False)
        self.assertFalse(TestModel._meta.auto_timestamp)

        TestModel2 = ModelFactory('TestModel2', auto_timestamp=False,
                                  database='simon')
        self.assertFalse(TestModel2._meta.auto_timestamp)
        self.assertEqual(TestModel2._meta.database, 'simon')

    def test_subclass(self):
        """Test that `ModelFactory()` subclasses models."""

        TestModel = ModelFactory('TestModel')
        TestModel2 = ModelFactory('TestModel2', spec=TestModel)

        self.assertIsInstance(TestModel2(), TestModel2)
        self.assertIsInstance(TestModel2(), TestModel)
