"""Tests of the aggregation module."""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from simon import Model, aggregation

from .utils import ModelFactory

MappedModel = ModelFactory('MappedModel', field_map={'fake': 'real'})
NotMappedModel = ModelFactory('MappedModel', map_id=False)


class TestPipeline(unittest.TestCase):
    """Test the `Pipeline` class."""

    def test___init__(self):
        """Test the `__init__()` method."""

        a = aggregation.Pipeline()

        self.assertEqual(a._project, {})
        self.assertEqual(a._match, {})
        self.assertIsNone(a._limit)
        self.assertIsNone(a._skip)
        self.assertEqual(a._unwind, {})
        self.assertEqual(a._group, {})
        self.assertEqual(a._geonear, {})

        self.assertIsNone(a._cls)

    def test__init__class(self):
        """Test the `__init__()` method with a class."""

        a = aggregation.Pipeline(cls=Model)

        self.assertTrue(a._cls is Model)

    def test_project_add_or_remove(self):
        """Test the `add_or_remove()` function."""

        a = aggregation.Pipeline()
        a.project(include='a')

        self.assertEqual(a._project, {'a': 1})

    def test_project_add_or_remove_list(self):
        """Test the `add_or_remove()` function with a list."""

        a = aggregation.Pipeline()
        a.project(include=['a'])

        self.assertEqual(a._project, {'a': 1})

    def test_project_add_or_remove_no_mapped_fields(self):
        ("Test the `add_or_remove()` function with an empty field "
         "map.")

        a = aggregation.Pipeline(cls=NotMappedModel)

        with mock.patch('simon.aggregation.get_nested_key') as get_nested_key:
            a.project(include=['a'])

            get_nested_key.assert_not_called()

    def test_consecutive_calls(self):
        """Test that `project()` properly handles consecutive calls."""

        a = aggregation.Pipeline()
        a.project(include='a')
        a.project(include='b')
        a.project(exclude=('a', 'c'))

        self.assertEqual(a._project, {'a': 0, 'b': 1, 'c': 0})

    def test_project_exclude(self):
        """Test the `project()` method with excludes."""

        a = aggregation.Pipeline()
        a.project(exclude=('a', 'b'))

        self.assertEqual(a._project, {'a': 0, 'b': 0})

    def test_project_exclude_mapped_field(self):
        """Test the `project()` method with an excluded mapped field."""

        a = aggregation.Pipeline(cls=MappedModel)
        a.project(exclude='fake')

        self.assertEqual(a._project, {'real': 0})

    def test_project_include(self):
        """Test the `project()` method with includes."""

        a = aggregation.Pipeline()
        a.project(include=('a', 'b'))

        self.assertEqual(a._project, {'a': 1, 'b': 1})

    def test_project_include_exclude(self):
        """Test the `project()` method with includes and excludes."""

        a = aggregation.Pipeline()
        a.project(include=('a', 'b'), exclude=('c', 'd'))

        self.assertEqual(a._project, {'a': 1, 'b': 1, 'c': 0, 'd': 0})

    def test_project_include_mapped_field(self):
        """Test the `project()` method with an included mapped field."""

        a = aggregation.Pipeline(cls=MappedModel)
        a.project(include='fake')

        self.assertEqual(a._project, {'real': '$fake'})
