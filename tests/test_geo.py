try:
    import unittest2 as unittest
except ImportError:
    import unittest

import collections
import mock

from simon import geo


class TestGeo(unittest.TestCase):
    """Test the helper methods in the `query` module."""

    def test_box(self):
        """Test the `box()` method."""

        with mock.patch('simon.geo.within') as within:
            geo.box([1, 2], [3, 4])
            within.assert_called_with('box', [1, 2], [3, 4])

        expected = {'$within': {'$box': [[1, 2], [3, 4]]}}
        actual = geo.box([1, 2], [3, 4])
        self.assertEqual(actual, expected)

    def test_box_typeerror(self):
        """Test that `box()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            geo.box(1, [2, 3])

        with self.assertRaises(TypeError):
            geo.box([1, 2], 3)

    def test_box_valueerror(self):
        """Test that `box()` raises `ValueError`."""

        with self.assertRaises(ValueError):
            geo.box([1, 2, 3], [4, 5])

        with self.assertRaises(ValueError):
            geo.box([1, 2], [3, 4, 5])

    def test_circle(self):
        """Test the `circle()` method."""

        with mock.patch('simon.geo.within') as within:
            geo.circle([1, 2], 3)
            within.assert_called_with('circle', [1, 2], 3)

        expected = {'$within': {'$circle': [[1, 2], 3]}}
        actual = geo.circle([1, 2], 3)
        self.assertEqual(actual, expected)

    def test_circle_typeerror(self):
        """Test that `circle()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            geo.circle(1, 2)

    def test_circle_valueerror(self):
        """Test that `circle()` raises `ValueError`."""

        with self.assertRaises(ValueError):
            geo.circle([1, 2, 3], 4)

    def test_near(self):
        """Test the `near()` method."""

        expected = {'$near': [1, 2]}
        actual = geo.near([1, 2])
        self.assertEqual(actual, expected)

        expected = {'$near': [1, 2], '$maxDistance': 3}
        actual = geo.near([1, 2], max_distance=3)
        self.assertEqual(actual, expected)

        expected = {'$near': [1, 2], '$uniqueDocs': True}
        actual = geo.near([1, 2], unique_docs=True)
        self.assertEqual(actual, expected)

        expected = {'$near': [1, 2], '$maxDistance': 3, '$uniqueDocs': True}
        actual = geo.near([1, 2], max_distance=3, unique_docs=True)
        self.assertEqual(actual, expected)

        # near() supports point as a tuple
        expected = {'$near': (1, 2)}
        actual = geo.near((1, 2))
        self.assertEqual(actual, expected)

    def test_near_typeerror(self):
        """Test that `near()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            geo.near(1)

    def test_near_valueerror(self):
        """Test that `near()` raises `ValueError`."""

        with self.assertRaises(ValueError):
            geo.near([1])

        with self.assertRaises(ValueError):
            geo.near([1, 2, 3])

    def test_polygon(self):
        """Test the `polygon()` method."""

        with mock.patch('simon.geo.within') as within:
            geo.polygon([1, 2], [3, 4], [5, 6])
            within.assert_called_with('polygon', [1, 2], [3, 4], [5, 6])

            geo.polygon({'a': {'x': 1, 'y': 2}, 'b': {'x': 3, 'y': 4}})
            within.assert_called_with('polygon', a={'x': 1, 'y': 2},
                                      b={'x': 3, 'y': 4})

        expected = {'$within': {'$polygon': [[1, 2], [3, 4], [5, 6]]}}
        actual = geo.polygon([1, 2], [3, 4], [5, 6])
        self.assertEqual(actual, expected)

        expected = {'$within': {'$polygon': {'a': {'x': 1, 'y': 2},
                                             'b': {'x': 3, 'y': 4}}}}
        actual = geo.polygon({'a': {'x': 1, 'y': 2}, 'b': {'x': 3, 'y': 4}})
        self.assertEqual(actual, expected)

    def test_polygon_mappings_typeerror(self):
        """Test that `polygon()` raises `TypeError` with mappings."""

        with self.assertRaises(TypeError):
            geo.polygon({'a': {'b': 1, 'c': 2}, 'd': 3})

    def test_polygon_mappings_valueerror(self):
        """Test that `polygon()` raises `ValueError` with mappings."""

        with self.assertRaises(ValueError):
            geo.polygon({'a': 1})

        with self.assertRaises(ValueError):
            geo.polygon({'a': {'b': 1}})

        with self.assertRaises(ValueError):
            geo.polygon({'a': {'b': 1, 'c': 2}})

        with self.assertRaises(ValueError):
            geo.polygon({'a': {'b': 1, 'c': 2}, 'd': {'e': 3}})

    def test_polygon_typeerror(self):
        """Test that `polygon()` raises `TypeError`."""

        with self.assertRaises(TypeError):
            geo.polygon()

        with self.assertRaises(TypeError):
            geo.polygon(1)

        with self.assertRaises(TypeError):
            geo.polygon([1])

        with self.assertRaises(TypeError):
            geo.polygon([1, 2, 3])

        with self.assertRaises(TypeError):
            geo.polygon([1, 2], 3)

    def test_polygon_valueerror(self):
        """Test that `polygon()` raises `ValueError`."""

        with self.assertRaises(ValueError):
            geo.polygon([1, 2], [3])

        with self.assertRaises(ValueError):
            geo.polygon([1, 2], [3, 4, 5])

    def test_validate_point_alternate_type(self):
        """Test the `_validate_point()` method with alternate types."""

        geo._validate_point({'a': 1, 'b': 2},
                            alternate_type=collections.Mapping)

    def test_validate_point_alternate_type_typeerror(self):
        ("Test that `_validate_point()` raises `TypeError` with "
         "alternate types.")

        with self.assertRaises(TypeError):
            geo._validate_point(1, alternate_type=basestring)

        with self.assertRaises(TypeError):
            geo._validate_point('a', alternate_type=int)

    def test_validate_point_alternate_type_valueerror(self):
        ("Test that `_validate_point()` raises `ValueError` with "
         "alternate types.")

        with self.assertRaises(ValueError):
            geo._validate_point({'a': 1},
                                alternate_type=collections.Mapping)

        with self.assertRaises(ValueError):
            geo._validate_point({'a': 1, 'b': 2, 'c': 3},
                                alternate_type=collections.Mapping)

    def test_validate_point_name(self):
        """Test the `_validate_point()` method with names."""

        with self.assertRaises(TypeError) as e:
            geo._validate_point(1, 'name')

        expected = 'name must be a list containing exactly 2 elements'
        actual = e.exception.message
        self.assertEqual(actual, expected)

        with self.assertRaises(ValueError) as e:
            geo._validate_point([1], 'name')

        expected = 'name must be a list containing exactly 2 elements'
        actual = e.exception.message
        self.assertEqual(actual, expected)

    def test_validate_point_typeerror(self):
        """Test that `_validate_point()` raises `TypeError`."""

        # Test with a string
        with self.assertRaises(TypeError) as e:
            geo._validate_point('a')

        expected = '`point` must be a list containing exactly 2 elements'
        actual = e.exception.message
        self.assertEqual(actual, expected)

        # Test with an int
        with self.assertRaises(TypeError) as e:
            geo._validate_point(1)

        expected = '`point` must be a list containing exactly 2 elements'
        actual = e.exception.message
        self.assertEqual(actual, expected)

    def test_validate_point_valueerror(self):
        """Test the `_validate_point()` method."""

        # Test with 1 element
        with self.assertRaises(ValueError) as e:
            geo._validate_point([1])

        expected = '`point` must be a list containing exactly 2 elements'
        actual = e.exception.message
        self.assertEqual(actual, expected)

        # Test with 3 elements
        with self.assertRaises(ValueError) as e:
            geo._validate_point([1, 2, 3])

        expected = '`point` must be a list containing exactly 2 elements'
        actual = e.exception.message
        self.assertEqual(actual, expected)

        # The following should not raise an exception
        geo._validate_point([1, 2])
        geo._validate_point((1, 2))

    def test_within(self):
        """Test the `within()` method."""

        expected = {'$within': {'$box': [[1, 2], [3, 4]]}}
        actual = geo.within('box', [1, 2], [3, 4])
        self.assertEqual(actual, expected)

        expected = {'$within': {'$polygon': {'a': {'x': 1, 'y': 2},
                                             'b': {'x': 3, 'y': 4}}}}
        actual = geo.within('polygon', a={'x': 1, 'y': 2},
                            b={'x': 3, 'y': 4})
        self.assertEqual(actual, expected)

    def test_within_runtimeerror(self):
        """Test that `within()` raises `RuntimeError`."""

        with self.assertRaises(RuntimeError):
            geo.within('shape', *[1, 2], **{'a': {'x': 3, 'y': 4}})
