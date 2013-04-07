"""Tests of the aggregation module."""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mock

from simon import Model, aggregation

from .utils import ModelFactory

MappedModel = ModelFactory('MappedModel', field_map={'fake': 'real'})
MapNestedModel = ModelFactory('MapNestedModel',
                              field_map={'a.b': 'c', 'd': 'e.f'})
NotMappedModel = ModelFactory('MappedModel', map_id=False)


class TestPipeline(unittest.TestCase):
    """Test the `Pipeline` class."""

    def test___init__(self):
        """Test the `__init__()` method."""

        p = aggregation.Pipeline()

        self.assertEqual(p._project, {})
        self.assertEqual(p._match, {})
        self.assertIsNone(p._limit)
        self.assertIsNone(p._skip)
        self.assertEqual(p._unwind, {})
        self.assertEqual(p._group, {})
        self.assertEqual(p._geonear, {})

        self.assertIsNone(p._cls)

    def test__init__class(self):
        """Test the `__init__()` method with a class."""

        p = aggregation.Pipeline(cls=Model)

        self.assertTrue(p._cls is Model)

    def test_project_add_or_remove(self):
        """Test the `add_or_remove()` function."""

        p = aggregation.Pipeline()
        p.project(include='a')

        self.assertEqual(p._project, {'a': 1})

    def test_project_add_or_remove_list(self):
        """Test the `add_or_remove()` function with a list."""

        p = aggregation.Pipeline()
        p.project(include=['a'])

        self.assertEqual(p._project, {'a': 1})

    def test_project_add_or_remove_no_mapped_fields(self):
        ("Test the `add_or_remove()` function with an empty field "
         "map.")

        p = aggregation.Pipeline(cls=NotMappedModel)

        with mock.patch('simon.aggregation.get_nested_key') as get_nested_key:
            p.project(include=['a'])

            get_nested_key.assert_not_called()

    def test_consecutive_calls(self):
        """Test that `project()` properly handles consecutive calls."""

        p = aggregation.Pipeline()
        p.project(include='a')
        p.project(include='b')
        p.project(exclude=('a', 'c'))

        self.assertEqual(p._project, {'a': 0, 'b': 1, 'c': 0})

    def test_project_exclude(self):
        """Test the `project()` method with excludes."""

        p = aggregation.Pipeline()
        p.project(exclude=('a', 'b'))

        self.assertEqual(p._project, {'a': 0, 'b': 0})

    def test_project_exclude_mapped_field(self):
        """Test the `project()` method with an excluded mapped field."""

        p = aggregation.Pipeline(cls=MappedModel)
        p.project(exclude='fake')
        self.assertEqual(p._project, {'real': 0})

        p = aggregation.Pipeline(cls=MapNestedModel)
        p.project(exclude=('a.b', 'd'))
        self.assertEqual(p._project, {'c': 0, 'e.f': 0})

    def test_project_include(self):
        """Test the `project()` method with includes."""

        p = aggregation.Pipeline()
        p.project(include=('a', 'b'))

        self.assertEqual(p._project, {'a': 1, 'b': 1})

    def test_project_include_exclude(self):
        """Test the `project()` method with includes and excludes."""

        p = aggregation.Pipeline()
        p.project(include=('a', 'b'), exclude=('c', 'd'))

        self.assertEqual(p._project, {'a': 1, 'b': 1, 'c': 0, 'd': 0})

    def test_project_include_mapped_field(self):
        """Test the `project()` method with an included mapped field."""

        p = aggregation.Pipeline(cls=MappedModel)
        p.project(include='fake')
        self.assertEqual(p._project, {'real': '$fake'})

        p = aggregation.Pipeline(cls=MapNestedModel)
        p.project(include=('a.b', 'd'))
        self.assertEqual(p._project, {'c': '$a.b', 'e.f': '$d'})

    def test_project_return(self):
        """Test that `project()` returns the instance."""

        p1 = aggregation.Pipeline()
        p2 = p1.project(include='a')

        self.assertEqual(p1, p2)
