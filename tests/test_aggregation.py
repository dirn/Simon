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
        self.assertEqual(p._unwind, [])
        self.assertEqual(p._group, {})
        self.assertEqual(p._geonear, {})

        self.assertIsNone(p._cls)

    def test__init__class(self):
        """Test the `__init__()` method with a class."""

        p = aggregation.Pipeline(cls=Model)

        self.assertTrue(p._cls is Model)

    def test_limit(self):
        """Test the `limit()` method."""

        p = aggregation.Pipeline()
        p.limit(1)

        self.assertEqual(p._limit, 1)

    def test_limit_consecutive_calls(self):
        """Test that `limit()` properly handles consecutive calls."""

        p = aggregation.Pipeline()
        p.limit(1)
        p.limit(2)

        self.assertEqual(p._limit, 2)

    def test_limit_return(self):
        """Test that `limit()` returns the instance."""

        p1 = aggregation.Pipeline()
        p2 = p1.limit(1)

        self.assertEqual(p1, p2)

    def test_match(self):
        """Test the `match()` method."""

        p = aggregation.Pipeline()
        p.match(a=1)

        self.assertEqual(p._match, {'a': 1})

    def test_match_consecutive_calls(self):
        """Test that `match()` properly handles consecutive calls."""

        p = aggregation.Pipeline()
        p.match(a=1)
        p.match(b=2)
        p.match(a=3, c=4)

        self.assertEqual(p._match, {'a': 3, 'b': 2, 'c': 4})

    def test_match_mapped_field(self):
        """Test the `match()` method with a mapped field."""

        p = aggregation.Pipeline(cls=MappedModel)
        p.match(fake=1)

        self.assertEqual(p._match, {'real': 1})

    def test_match_nested_field(self):
        """Test the `match()` method with an embedded document."""

        p = aggregation.Pipeline()
        p.match(a__b=1)

        self.assertEqual(p._match, {'a.b': 1})

    def test_match_operator(self):
        """Test the `match()` method with a comparison operator."""

        p = aggregation.Pipeline()
        p.match(a__gt=1)

        self.assertEqual(p._match, {'a': {'$gt': 1}})

    def test_match_return(self):
        """Test that `match()` returns the instance."""

        p1 = aggregation.Pipeline()
        p2 = p1.match(a=1)

        self.assertEqual(p1, p2)

    def test_project_consecutive_calls(self):
        """Test that `project()` properly handles consecutive calls."""

        p = aggregation.Pipeline()
        p.project(a=True)
        p.project(b=True)
        p.project(a=False, c=False)

        self.assertEqual(p._project, {'a': 0, 'b': 1, 'c': 0})

    def test_project_exclude(self):
        """Test the `project()` method with excludes."""

        p = aggregation.Pipeline()
        p.project(a=False, b=False)

        self.assertEqual(p._project, {'a': 0, 'b': 0})

    def test_project_exclude_mapped_field(self):
        """Test the `project()` method with an excluded mapped field."""

        p = aggregation.Pipeline(cls=MappedModel)
        p.project(fake=False)
        self.assertEqual(p._project, {'real': 0})

        p = aggregation.Pipeline(cls=MapNestedModel)
        p.project(a__b=False, d=False)
        self.assertEqual(p._project, {'c': 0, 'e.f': 0})

    def test_project_include(self):
        """Test the `project()` method with includes."""

        p = aggregation.Pipeline()
        p.project(a=True, b=True)

        self.assertEqual(p._project, {'a': 1, 'b': 1})

    def test_project_include_exclude(self):
        """Test the `project()` method with includes and excludes."""

        p = aggregation.Pipeline()
        p.project(a=True, b=True, c=False, d=False)

        self.assertEqual(p._project, {'a': 1, 'b': 1, 'c': 0, 'd': 0})

    def test_project_include_mapped_field(self):
        """Test the `project()` method with an included mapped field."""

        p = aggregation.Pipeline(cls=MappedModel)
        p.project(fake=True)
        self.assertEqual(p._project, {'real': '$fake'})

        p = aggregation.Pipeline(cls=MapNestedModel)
        p.project(a__b=True, d=True)
        self.assertEqual(p._project, {'c': '$a.b', 'e.f': '$d'})

    def test_project_return(self):
        """Test that `project()` returns the instance."""

        p1 = aggregation.Pipeline()
        p2 = p1.project(a=True)

        self.assertEqual(p1, p2)

    def test_skip(self):
        """Test the `skip()` method."""

        p = aggregation.Pipeline()
        p.skip(1)

        self.assertEqual(p._skip, 1)

    def test_skip_consecutive_calls(self):
        """Test that `skip()` properly handles consecutive calls."""

        p = aggregation.Pipeline()
        p.skip(1)
        p.skip(2)

        self.assertEqual(p._skip, 2)

    def test_skip_return(self):
        """Test that `skip()` returns the instance."""

        p1 = aggregation.Pipeline()
        p2 = p1.skip(1)

        self.assertEqual(p1, p2)

    def test_unwind(self):
        """Test the `unwind()` method."""

        p = aggregation.Pipeline()
        p.unwind('a')

        self.assertEqual(p._unwind, ['$a'])

    def test_unwind_consecutive_calls(self):
        """Test that `unwind()` properly handles consecutive calls."""

        p = aggregation.Pipeline()
        p.unwind('a')
        p.unwind('b')
        p.unwind('a', 'c')

        self.assertEqual(p._unwind, ['$a', '$b', '$c'])

    def test_match_unwind_field(self):
        """Test the `unwind()` method with a mapped field."""

        p = aggregation.Pipeline(cls=MappedModel)
        p.unwind('fake')

        self.assertEqual(p._unwind, ['$real'])

    def test_unwind_nested_field(self):
        """Test the `unwind()` method with an embedded document."""

        p = aggregation.Pipeline()
        p.unwind('a__b')

        self.assertEqual(p._unwind, ['$a.b'])

    def test_unwind_return(self):
        """Test that `unwind()` returns the instance."""

        p1 = aggregation.Pipeline()
        p2 = p1.unwind('a')

        self.assertEqual(p1, p2)
