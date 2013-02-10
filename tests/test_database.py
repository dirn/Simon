"""Tests of the database functionality"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from contextlib import nested

from bson import ObjectId
import mock
import pymongo

from simon import Model, connection, query

AN_OBJECT_ID_STR = '50d4dce70ea5fae6fb84e44b'
AN_OBJECT_ID = ObjectId(AN_OBJECT_ID_STR)

# Set the write concern argument
if pymongo.version_tuple[:2] >= (2, 4):
    wc_on = {'w': 1}
    wc_off = {'w': 0}
else:
    wc_on = {'safe': True}
    wc_off = {'safe': False}


class TestModel1(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        field_map = {'id': '_id', 'fake': 'real'}


class TestModel2(Model):
    class Meta:
        auto_timestamp = False
        collection = 'test-simon'
        database = 'test-simon'


class TestModel3(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        safe = False
        sort = 'a'


class TestModel4(Model):
    class Meta:
        collection = 'test-simon'
        database = 'test-simon'
        sort = '-a'


class TestModel5(Model):
    class Meta:
        auto_timestamp = False
        required_fields = ('a', 'b')


class TestModel6(Model):
    class Meta:
        required_fields = 'a.b'


class TestDatabase(unittest.TestCase):
    """Test database interaction"""

    @classmethod
    def setUpClass(cls):
        with mock.patch('simon.connection.MongoClient'):
            cls.connection = connection.connect('localhost', name='test-simon')

    @classmethod
    def tearDownClass(cls):
        # Reset the cached connections and databases so the ones added
        # during one test don't affect another
        connection._connections = None
        connection._databases = None

    def test___init__(self):
        """Test the `__init__()` method."""

        m = TestModel1(_id=AN_OBJECT_ID, a=1, b=2)

        self.assertEqual(m._id, AN_OBJECT_ID)
        self.assertEqual(m._document['_id'], AN_OBJECT_ID)

        self.assertEqual(m.a, 1)
        self.assertEqual(m._document['a'], 1)

        self.assertEqual(m.b, 2)
        self.assertEqual(m._document['b'], 2)

    def test_all(self):
        """Test the `all()` method."""

        with mock.patch('simon.Model.find') as mock_find:
            TestModel1.all()

            mock_find.assert_called_with()

    def test_delete(self):
        """Test the `delete()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1._meta.db, 'remove') as remove:
            m.delete()

            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=True)

    def test_delete_typeerror(self):
        """Test that `delete()` raises `TypeError`."""

        m = TestModel1(a=1, b=2)

        with self.assertRaises(TypeError):
            m.delete()

    def test_delete_write_concern(self):
        """Test that `delete()` respects write concern."""

        with mock.patch.object(TestModel3._meta.db, 'remove') as remove:
            m = TestModel3(_id=AN_OBJECT_ID)
            m.delete()
            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=False)

            # m needs to be given its _id again because delete() strips
            # it to prevent attempts to resave a deleted document
            m = TestModel3(_id=AN_OBJECT_ID)
            m.delete(safe=False)
            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=False)

            m = TestModel3(_id=AN_OBJECT_ID)
            m.delete(safe=True)
            remove.assert_called_with({'_id': AN_OBJECT_ID}, safe=True)

    def test_find(self):
        """Test the `find()` method."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            qs = TestModel1.find(id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID})

            self.assertIsInstance(qs, query.QuerySet)

    def test_find_comparison(self):
        """Test the `find()` method with comparison operators."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(a__gt=1)
            find.assert_called_with({'a': {'$gt': 1}})

            TestModel1.find(a__lte=1, b=2)
            find.assert_called_with({'a': {'$lte': 1}, 'b': 2})

    def test_find_embedded_document(self):
        """Test the `find()` method with an embedded document."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(a__b=1)

            find.assert_called_with({'a.b': 1})

    def test_find_id_string(self):
        """Test the `find()` method with a string `_id`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(_id=AN_OBJECT_ID_STR)

            find.assert_called_with({'_id': AN_OBJECT_ID})

    def test_find_sorted(self):
        """Test the `find()` method with a sort on the class."""

        with mock.patch('simon.base.QuerySet') as QuerySet:
            with mock.patch.object(TestModel1._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                TestModel1.find()
                QuerySet.sort.assert_not_called()

            with mock.patch.object(TestModel3._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                TestModel3.find()
                QuerySet().sort.assert_called_with('a')

            with mock.patch.object(TestModel4._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                TestModel4.find()
                QuerySet().sort.assert_called_with('-a')

    def test_find_with_q(self):
        """Test the `find()` method with `Q` objects."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            find.return_value = [{'_id': AN_OBJECT_ID}]

            TestModel1.find(query.Q(a=1))
            find.assert_called_with({'a': 1})

            TestModel1.find(query.Q(a=1) | query.Q(b=2))
            find.assert_called_with({'$or': [{'a': 1}, {'b': 2}]})

            TestModel1.find(query.Q(a=1) & query.Q(b=2))
            find.assert_called_with({'$and': [{'a': 1}, {'b': 2}]})

    def test_get(self):
        """Test the `get()` method."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__getitem__.return_value = {'_id': AN_OBJECT_ID}
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            m = TestModel1.get(_id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID})

            self.assertEqual(m._document['_id'], AN_OBJECT_ID)

    def test_get_comparison(self):
        """Test the `get()` method with comparison operators."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__getitem__.return_value = {'_id': AN_OBJECT_ID}
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(a__gt=1)

            find.assert_called_with({'a': {'$gt': 1}})

            TestModel1.get(a__lte=1, b=2)

            find.assert_called_with({'a': {'$lte': 1}, 'b': 2})

    def test_get_embedded_document(self):
        """Test the `get()` method with an embedded document."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__getitem__.return_value = {'_id': AN_OBJECT_ID}
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(a__b=1)

            find.assert_called_with({'a.b': 1})

    def test_get_id_string(self):
        """Test the `get()` method with a string `_id`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__iter__ = [{'_id': AN_OBJECT_ID}]
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(_id=AN_OBJECT_ID_STR)

            find.assert_called_with({'_id': AN_OBJECT_ID})

    def test_get_multipledocumentsfound(self):
        """Test that `get()` raises `MultipleDocumentsFound`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.Mock()
            mock_query_set.count.return_value = 2

            find.return_value = mock_query_set

            with self.assertRaises(TestModel1.MultipleDocumentsFound):
                TestModel1.get(_id=AN_OBJECT_ID)

    def test_get_or_create_create(self):
        """Test the `get_or_create()` method for creating documents."""

        with nested(mock.patch.object(TestModel1, 'get'),
                    mock.patch.object(TestModel1, 'create'),
                    ) as (get, create):
            get.side_effect = TestModel1.NoDocumentFound

            create.return_value = mock.Mock()

            m, created = TestModel1.get_or_create(_id=AN_OBJECT_ID)

            get.assert_called_with(_id=AN_OBJECT_ID)
            # Because get_or_create() is being called without explicity
            # setting a value for safe, safe's default value will be
            # passed along to creates()
            create.assert_called_with(_id=AN_OBJECT_ID, safe=False)

            self.assertTrue(created)

    def test_get_or_create_get(self):
        """Test the `get_or_create()` method for getting documents."""

        with mock.patch.object(TestModel1, 'get') as get:
            get.return_value = mock.Mock()

            m, created = TestModel1.get_or_create(_id=AN_OBJECT_ID)

            get.assert_called_with(_id=AN_OBJECT_ID)

            self.assertFalse(created)

    def test_get_nodocumentfound(self):
        """Test that `get()` raises `NoDocumentFound`."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.Mock()
            mock_query_set.count.return_value = 0

            find.return_value = mock_query_set

            with self.assertRaises(TestModel1.NoDocumentFound):
                TestModel1.get(_id=AN_OBJECT_ID)

    def test_get_with_q(self):
        """Test the `get()` method with `Q` objects."""

        with mock.patch.object(TestModel1._meta.db, 'find') as find:
            mock_query_set = mock.MagicMock()
            mock_query_set.__iter__ = [{'_id': AN_OBJECT_ID}]
            mock_query_set.count.return_value = 1

            find.return_value = mock_query_set

            TestModel1.get(query.Q(a=1))
            find.assert_called_with({'a': 1})

            TestModel1.get(query.Q(a=1) | query.Q(b=2))
            find.assert_called_with({'$or': [{'a': 1}, {'b': 2}]})

            TestModel1.get(query.Q(a=1) & query.Q(b=2))
            find.assert_called_with({'$and': [{'a': 1}, {'b': 2}]})

    def test__update(self):
        """Test the `_update()` method."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m._update({'a': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 1}, **wc_on)

    def test__update_atomic(self):
        """Test the `_update()` method with an atomic update."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TestModel1._meta.db, 'update'),
                    mock.patch.object(TestModel1._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            m._update({'$set': {'a': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'a': 1}}, **wc_on)

            find_one.assert_called_with({'_id': AN_OBJECT_ID}, {'a': 1})

            self.assertEqual(m._document['a'], 1)

    def test__update_atomic_nested_field(self):
        ("Test the `_update()` method with an atomic update of an "
         "embedded document.")

        m = TestModel1(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TestModel1._meta.db, 'update'),
                    mock.patch.object(TestModel1._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'b': 1}}

            m._update({'$set': {'a.b': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'a.b': 1}}, **wc_on)

            find_one.assert_called_with({'_id': AN_OBJECT_ID}, {'a.b': 1})

            self.assertEqual(m._document['a']['b'], 1)

    def test__update_atomic_unset(self):
        """Test the `_update()` method with `$unset`."""

        m = TestModel1(_id=AN_OBJECT_ID, a=2)

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m._update({'$unset': {'a': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$unset': {'a': 1}}, **wc_on)

            self.assertNotIn('a', m._document)

    def test__update_atomic_unset_nested_field(self):
        ("Test the `_update()` method with `$unset` with an embedded "
         "document.")

        m = TestModel1(_id=AN_OBJECT_ID, a__b=2)

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m._update({'$unset': {'a.b': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$unset': {'a.b': 1}}, **wc_on)

            self.assertNotIn('b', m._document['a'])

    def test__update_field_map(self):
        """Test the `_update()` method with a name in `field_map`."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m._update({'fake': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'real': 1}, **wc_on)

    def test__update_field_map_atomic(self):
        ("Test the `_update()` method with a name in `field_map` with "
         "an atomic update.")

        m = TestModel1(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TestModel1._meta.db, 'update'),
                    mock.patch.object(TestModel1._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'real': 1}

            m._update({'$set': {'fake': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'real': 1}}, **wc_on)

            find_one.assert_called_with({'_id': AN_OBJECT_ID}, {'real': 1})

            self.assertEqual(m._document['real'], 1)

    def test__update_insert(self):
        """Test the `_update()` method for an insert."""

        m = TestModel1()

        with mock.patch.object(TestModel1._meta.db, 'insert') as insert:
            insert.return_value = AN_OBJECT_ID

            m._update({'a': 1}, upsert=True)

            insert.assert_called_with(document={'a': 1}, **wc_on)

            self.assertEqual(m._document['_id'], AN_OBJECT_ID)

    def test__update_nested_field(self):
        """Test the `_update()` method with an embedded document."""

        m = TestModel1(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m._update({'a__b': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a.b': 1}, **wc_on)

    def test__update_required_field(self):
        """Test the `_update()` method with a required field."""

        m = TestModel5(_id=AN_OBJECT_ID)

        with mock.patch.object(TestModel5._meta.db, 'update') as update:
            m._update({'a': 1, 'b': 2})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 1, 'b': 2}, **wc_on)

    def test__update_required_field_atomic(self):
        ("Test the `_update()` method with a required field with an "
         "atomic update.")

        m = TestModel5(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TestModel5._meta.db, 'update'),
                    mock.patch.object(TestModel5._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'a': 1, 'b': 2}

            m._update({'$set': {'a': 1, 'b': 2}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'a': 1, 'b': 2}},
                                      **wc_on)

    def test__update_required_field_nested(self):
        ("Test the `_update()` method with a required field with an "
         "embedded document.")

        m = TestModel6(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TestModel6._meta.db, 'update'),
                    mock.patch.object(TestModel6._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'a.b': 1}

            m._update({'a.b': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a.b': 1}, **wc_on)

    def test__update_required_field_unset(self):
        ("Test the `_update()` method with a required field with "
         "`$unset`.")

        m = TestModel5(_id=AN_OBJECT_ID, a=1, b=2, c=3)

        with mock.patch.object(TestModel5._meta.db, 'update') as update:
            m._update({'$unset': {'c': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$unset': {'c': 1}}, **wc_on)

    def test__update_typeerror(self):
        """Test that `_update()` raises `TypeError`."""

        m1 = TestModel1()

        # TypeError should be raised when there's no _id.
        with self.assertRaises(TypeError) as e:
            m1._update({})

        expected = ("The 'TestModel1' object cannot be updated because its "
                    "'_id' attribute has not been set.")
        actual = e.exception.message
        self.assertEqual(actual, expected)

        m2 = TestModel5(_id=AN_OBJECT_ID)
        m3 = TestModel5(_id=AN_OBJECT_ID, a=1, b=2)

        # TypeError should be raised when required fields are missing.
        with self.assertRaises(TypeError) as e:
            m2._update({})

        expected = ("The 'TestModel5' object cannot be updated because it must"
                    " contain all of the required fields: a, b.")
        actual = e.exception.message
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError) as e:
            m3._update({'$unset': {'a': 1}})

        expected = ("The 'TestModel5' object cannot be updated because it must"
                    " contain all of the required fields: a, b.")
        actual = e.exception.message
        self.assertEqual(actual, expected)

    def test__update_use_internal(self):
        """Test the `_update()` method with `use_internal`."""

        m = TestModel1(_id=AN_OBJECT_ID, a=5)

        with mock.patch.object(TestModel1._meta.db, 'update') as update:
            m._update({'a': 1}, use_internal=True)

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 5}, **wc_on)

    def test__update_use_internal_atomic(self):
        ("Test the `_update()` method with `use_internal` with an "
         "atomic update.")

        m = TestModel1(_id=AN_OBJECT_ID, a__b=5)

        with nested(mock.patch.object(TestModel1._meta.db, 'update'),
                    mock.patch.object(TestModel1._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'b': 5}}

            m._update({'a.b': 1}, use_internal=True)

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a.b': 5}, **wc_on)
