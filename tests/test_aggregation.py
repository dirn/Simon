"""Tests of the aggregation module."""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from simon import aggregation


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

    def test_consecutive_calls(self):
        """Test that `project()` properly handles consecutive calls."""

        a = aggregation.Pipeline()
        a.project(include='a')
        a.project(include='b')
        a.project(exclude=('a', 'c'))

        self.assertEqual(a._project, {'a': 0, 'b': 1, 'c': 0})

    def test_project_include(self):
        """Test the `project()` method with includes."""

        a = aggregation.Pipeline()
        a.project(include=('a', 'b'))

        self.assertEqual(a._project, {'a': 1, 'b': 1})

    def test_project_exclude(self):
        """Test the `project()` method with excludes."""

        a = aggregation.Pipeline()
        a.project(exclude=('a', 'b'))

        self.assertEqual(a._project, {'a': 0, 'b': 0})

    def test_project_include_exclude(self):
        """Test the `project()` method with includes and excludes."""

        a = aggregation.Pipeline()
        a.project(include=('a', 'b'), exclude=('c', 'd'))

        self.assertEqual(a._project, {'a': 1, 'b': 1, 'c': 0, 'd': 0})
