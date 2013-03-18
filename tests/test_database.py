"""Tests of the database functionality"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from contextlib import nested
import datetime
import warnings

from bson import ObjectId
import mock
import pymongo

from simon import connection, query

from .utils import AN_OBJECT_ID, AN_OBJECT_ID_STR, ModelFactory

# Set the write concern argument
if pymongo.version_tuple[:2] >= (2, 4):
    wc_on = {'w': 1}
    wc_off = {'w': 0}
else:
    wc_on = {'safe': True}
    wc_off = {'safe': False}

DefaultModel = ModelFactory('DefaultModel')
MappedModel = ModelFactory('MappedModel', field_map={'fake': 'real'})
RequiredModel = ModelFactory('RequiredModel', required_fields=('a', 'b'))
TypedModel = ModelFactory('TypedModel', typed_fields={'a': int})
TypedListModel = ModelFactory('TypedListModel', typed_fields={'a': [int]})


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

        m = DefaultModel(_id=AN_OBJECT_ID, a=1, b=2)

        self.assertEqual(m._id, AN_OBJECT_ID)
        self.assertEqual(m._document['_id'], AN_OBJECT_ID)

        self.assertEqual(m.a, 1)
        self.assertEqual(m._document['a'], 1)

        self.assertEqual(m.b, 2)
        self.assertEqual(m._document['b'], 2)

    def test_delete(self):
        """Test the `delete()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel._meta.db, 'remove') as remove:
            m.delete()

            remove.assert_called_with({'_id': AN_OBJECT_ID}, **wc_on)

    def test_delete_typeerror(self):
        """Test that `delete()` raises `TypeError`."""

        m = DefaultModel(a=1, b=2)

        with self.assertRaises(TypeError):
            m.delete()

    def test_delete_write_concern(self):
        """Test that `delete()` respects write concern."""

        UnsafeModel = ModelFactory('UnsafeModel', safe=False)

        with mock.patch.object(UnsafeModel._meta.db, 'remove') as remove:
            m = UnsafeModel(_id=AN_OBJECT_ID)
            m.delete()
            remove.assert_called_with({'_id': AN_OBJECT_ID}, **wc_off)

            # m needs to be given its _id again because delete() strips
            # it to prevent attempts to resave a deleted document
            m = UnsafeModel(_id=AN_OBJECT_ID)
            m.delete(w=0)
            remove.assert_called_with({'_id': AN_OBJECT_ID}, **wc_off)

            m = UnsafeModel(_id=AN_OBJECT_ID)
            m.delete(w=1)
            remove.assert_called_with({'_id': AN_OBJECT_ID}, **wc_on)

    def test__find(self):
        """Test the `_find()` method."""

        with nested(mock.patch.object(DefaultModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            DefaultModel._find(_id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID})

    def test__find_field_map(self):
        """Test the `_find()` method with a name in `field_map`."""

        with nested(mock.patch.object(MappedModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            MappedModel._find(fake=1)

            find.assert_called_with({'real': 1})

    def test__find_find_one(self):
        """Test the `_find()` method with `find_one`."""

        with mock.patch.object(DefaultModel._meta.db, 'find') as find:
            QuerySet = mock.MagicMock(spec=query.QuerySet)
            QuerySet.__getitem__.return_value = {'_id': AN_OBJECT_ID}
            QuerySet.count.return_value = 1

            find.return_value = QuerySet

            m = DefaultModel._find(find_one=True, _id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID})

            self.assertEqual(m._document['_id'], AN_OBJECT_ID)

    def test__find_find_one_multipledocumentsfound(self):
        """Test that `_find()` raises `MultipleDocumentsFound`."""

        with mock.patch.object(DefaultModel._meta.db, 'find') as find:
            QuerySet = mock.MagicMock(spec=query.QuerySet)
            QuerySet.count.return_value = 2

            find.return_value = QuerySet
            with self.assertRaises(DefaultModel.MultipleDocumentsFound) as e:
                DefaultModel._find(find_one=True, _id=AN_OBJECT_ID)

            actual = str(e.exception)

            # Check the exception for the pieces
            expected = "The query returned more than one 'DefaultModel'."
            self.assertIn(expected, actual)

            expected = "It returned 2!"
            self.assertIn(expected, actual)

            expected = "The document spec was:"
            self.assertIn(expected, actual)

            expected = "'_id'"
            self.assertIn(expected, actual)

            expected = "ObjectId"
            self.assertIn(expected, actual)

            expected = AN_OBJECT_ID_STR
            self.assertIn(expected, actual)

    def test__find_find_one_nodocumentfound(self):
        """Test that `_find()` raises `NoDocumentFound`."""

        with mock.patch.object(DefaultModel._meta.db, 'find') as find:
            QuerySet = mock.MagicMock(spec=query.QuerySet)
            QuerySet.count.return_value = 0

            find.return_value = QuerySet
            with self.assertRaises(DefaultModel.NoDocumentFound) as e:
                DefaultModel._find(find_one=True, _id=AN_OBJECT_ID)

            expected = "'DefaultModel' matching query does not exist."
            actual = str(e.exception)
            self.assertEqual(actual, expected)

    def test__find_nested_field(self):
        """Test the `_find()` method with an embedded document."""

        with nested(mock.patch.object(DefaultModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            DefaultModel._find(a__b=1)

            find.assert_called_with({'a.b': 1})

    def test__find_objectid_none(self):
        """Test the `_find()` method with an untyped Object Id."""

        UntypedModel = ModelFactory('UntypedModel', typed_fields={'_id': None})

        with nested(mock.patch.object(UntypedModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            UntypedModel._find(_id='a')

            find.assert_called_with({'_id': 'a'})

            UntypedModel._find(_id=1)

            find.assert_called_with({'_id': 1})

    def test__find_objectid_string(self):
        """Test the `_find()` method with a string `_id`."""

        with nested(mock.patch.object(DefaultModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            DefaultModel._find(_id=AN_OBJECT_ID_STR)

            find.assert_called_with({'_id': AN_OBJECT_ID})

    def test__find_objectid_typed(self):
        """Test the `_find()` method with a typed Object Id."""

        IntModel = ModelFactory('IntModel', typed_fields={'_id': int})

        with nested(mock.patch.object(IntModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            IntModel._find(_id=1)

            find.assert_called_with({'_id': 1})

    def test__find_q(self):
        """Test the `_find()` method with a `Q` object."""

        with nested(mock.patch.object(DefaultModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            DefaultModel._find(query.Q(a=1), _id=AN_OBJECT_ID)

            find.assert_called_with({'_id': AN_OBJECT_ID, 'a': 1})

    def test__find_q_alone(self):
        """Test the `_find()` method with nothing but a `Q` object."""

        with nested(mock.patch.object(DefaultModel._meta.db, 'find'),
                    mock.patch('simon.base.QuerySet'),) as (find, QuerySet):
            find.return_value = QuerySet

            DefaultModel._find(query.Q(a=1))

            find.assert_called_with({'a': 1})

    def test__find_sorted(self):
        """Test the `_find()` method with a sort."""

        SortedAscModel = ModelFactory('SortedAscModel', sort=('a',))
        SortedDescModel = ModelFactory('SortedDescModel', sort=('-a',))

        with mock.patch('simon.base.QuerySet') as QuerySet:
            with mock.patch.object(DefaultModel._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                DefaultModel._find()
                QuerySet.sort.assert_not_called()

            with mock.patch.object(SortedAscModel._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                SortedAscModel._find()
                QuerySet().sort.assert_called_with('a')

            with mock.patch.object(SortedDescModel._meta.db, 'find') as find:
                find.return_value = [{'_id': AN_OBJECT_ID}]

                SortedDescModel._find()
                QuerySet().sort.assert_called_with('-a')

    def test__update(self):
        """Test the `_update()` method."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel._meta.db, 'update') as update:
            m._update({'a': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 1}, **wc_on)

    def test__update_atomic(self):
        """Test the `_update()` method with an atomic update."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
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

        m = DefaultModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'b': 1}}

            m._update({'$set': {'a.b': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'a.b': 1}}, **wc_on)

            find_one.assert_called_with({'_id': AN_OBJECT_ID}, {'a.b': 1})

            self.assertEqual(m._document['a']['b'], 1)

    def test__update_atomic_unset(self):
        """Test the `_update()` method with `$unset`."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=2)

        with mock.patch.object(DefaultModel._meta.db, 'update') as update:
            m._update({'$unset': {'a': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$unset': {'a': 1}}, **wc_on)

            self.assertNotIn('a', m._document)

    def test__update_atomic_unset_nested_field(self):
        ("Test the `_update()` method with `$unset` with an embedded "
         "document.")

        m = DefaultModel(_id=AN_OBJECT_ID, a__b=2)

        with mock.patch.object(DefaultModel._meta.db, 'update') as update:
            m._update({'$unset': {'a.b': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$unset': {'a.b': 1}}, **wc_on)

            self.assertNotIn('b', m._document['a'])

    def test__update_field_map(self):
        """Test the `_update()` method with a name in `field_map`."""

        m = MappedModel(_id=AN_OBJECT_ID)

        with mock.patch.object(MappedModel._meta.db, 'update') as update:
            m._update({'fake': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'real': 1}, **wc_on)

    def test__update_field_map_atomic(self):
        ("Test the `_update()` method with a name in `field_map` with "
         "an atomic update.")

        m = MappedModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(MappedModel._meta.db, 'update'),
                    mock.patch.object(MappedModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'real': 1}

            m._update({'$set': {'fake': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'real': 1}}, **wc_on)

            find_one.assert_called_with({'_id': AN_OBJECT_ID}, {'real': 1})

            self.assertEqual(m._document['real'], 1)

    def test__update_insert(self):
        """Test the `_update()` method for an insert."""

        m = DefaultModel()

        with mock.patch.object(DefaultModel._meta.db, 'insert') as insert:
            insert.return_value = AN_OBJECT_ID

            m._update({'a': 1}, upsert=True)

            insert.assert_called_with(doc_or_docs={'a': 1}, **wc_on)

            self.assertEqual(m._document['_id'], AN_OBJECT_ID)

    def test__update_nested_field(self):
        """Test the `_update()` method with an embedded document."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with mock.patch.object(DefaultModel._meta.db, 'update') as update:
            m._update({'a__b': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a.b': 1}, **wc_on)

    def test__update_objectid_none(self):
        """Test the `_update()` method with an untyped Object Id."""

        UntypedModel = ModelFactory('UntypedModel', typed_fields={'_id': None})

        with mock.patch.object(UntypedModel._meta.db, 'update') as update:
            m1 = UntypedModel(_id='a')

            m1._update({'b': 1})

            update.assert_called_with(spec={'_id': 'a'}, document={'b': 1},
                                      upsert=True, **wc_on)

            m2 = UntypedModel(_id=1)

            m2._update({'b': 2})

            update.assert_called_with(spec={'_id': 1}, document={'b': 2},
                                      upsert=True, **wc_on)

    def test__update_objectid_typed(self):
        """Test the `_update()` method with a typed Object Id."""

        IntModel = ModelFactory('IntModel', typed_fields={'_id': int})

        m = IntModel(_id=1)

        with mock.patch.object(IntModel._meta.db, 'update') as update:
            m._update({'b': 2})

            update.assert_called_with(spec={'_id': 1}, document={'b': 2},
                                      upsert=True, **wc_on)

    def test__update_pop(self):
        """Test the `_update()` method with a pop."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=[1, 2])

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': [1]}

            m._update({'$pop': {'a': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$pop': {'a': 1}}, **wc_on)

            self.assertEqual(len(m._document['a']), 1)
            self.assertNotIn(2, m._document['a'])

    def test__update_pull(self):
        """Test the `_update()` method with a pull."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=[1, 2])

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': [2]}

            m._update({'$pull': {'a': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$pull': {'a': 1}}, **wc_on)

            self.assertNotIn(1, m._document['a'])
            self.assertIn(2, m._document['a'])

            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': []}

            m._update({'$pullAll': {'a': [2]}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$pullAll': {'a': [2]}},
                                      **wc_on)

            self.assertNotIn(2, m._document['a'])

    def test__update_push(self):
        """Test the `_update()` method with a push."""

        m = DefaultModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': [1]}

            m._update({'$push': {'a': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$push': {'a': 1}}, **wc_on)

            self.assertIn(1, m._document['a'])

            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': [1, 2]}

            m._update({'$pushAll': {'a': [2]}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$pushAll': {'a': [2]}},
                                      **wc_on)

            self.assertIn(2, m._document['a'])

            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': [1, 2, 3]}

            m._update({'$addToSet': {'a': 3}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$addToSet': {'a': 3}},
                                      **wc_on)

            self.assertIn(3, m._document['a'])

    def test__update_rename(self):
        """Test the `_update()` method with a rename."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=1)

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'b': 1}

            m._update({'$rename': {'a': 'b'}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$rename': {'a': 'b'}},
                                      **wc_on)

            self.assertNotIn('a', m._document)
            self.assertIn('b', m._document)
            self.assertEqual(m._document['b'], 1)

    def test__update_rename_field_map(self):
        ("Test the `_update()` method with a rename with a name in "
         "`field_map`.")

        m = MappedModel(_id=AN_OBJECT_ID, b=1)

        with nested(mock.patch.object(MappedModel._meta.db, 'update'),
                    mock.patch.object(MappedModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'real': 1}

            m._update({'$rename': {'a': 'fake'}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$rename': {'a': 'real'}},
                                      **wc_on)

            self.assertNotIn('a', m._document)
            self.assertIn('real', m._document)
            self.assertEqual(m._document['real'], 1)

    def test__update_rename_nested(self):
        ("Test the `_update()` method with a rename with an embedded "
         "document.")

        m = DefaultModel(_id=AN_OBJECT_ID, a={'b': 1})

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'c': 1}}

            m._update({'$rename': {'a.b': 'a.c'}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$rename': {'a.b': 'a.c'}},
                                      **wc_on)

            self.assertNotIn('b', m._document['a'])
            self.assertIn('c', m._document['a'])
            self.assertEqual(m._document['a']['c'], 1)

    def test__update_required_field(self):
        """Test the `_update()` method with a required field."""

        m = RequiredModel(_id=AN_OBJECT_ID)

        with mock.patch.object(RequiredModel._meta.db, 'update') as update:
            m._update({'a': 1, 'b': 2})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 1, 'b': 2}, **wc_on)

    def test__update_required_field_atomic(self):
        ("Test the `_update()` method with a required field with an "
         "atomic update.")

        m = RequiredModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(RequiredModel._meta.db, 'update'),
                    mock.patch.object(RequiredModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1, 'b': 2}

            m._update({'$set': {'a': 1, 'b': 2}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'a': 1, 'b': 2}},
                                      **wc_on)

    def test__update_required_field_nested(self):
        ("Test the `_update()` method with a required field with an "
         "embedded document.")

        RequiredEmbeddedModel = ModelFactory('RequiredModel',
                                             required_fields=('a.b',))

        m = RequiredEmbeddedModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(RequiredEmbeddedModel._meta.db,
                                      'update'),
                    mock.patch.object(RequiredEmbeddedModel._meta.db,
                                      'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'b': 1}}

            m._update({'a.b': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a.b': 1}, **wc_on)

    def test__update_required_field_rename(self):
        ("Test the `_update()` method with a required field with a "
         "rename.")

        m = RequiredModel(_id=AN_OBJECT_ID, a=1, b=2, c=3)

        with nested(mock.patch.object(RequiredModel._meta.db, 'update'),
                    mock.patch.object(RequiredModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'd': 3}

            m._update({'$rename': {'c': 'd'}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$rename': {'c': 'd'}},
                                      **wc_on)

    def test__update_required_field_unset(self):
        ("Test the `_update()` method with a required field with "
         "`$unset`.")

        m = RequiredModel(_id=AN_OBJECT_ID, a=1, b=2, c=3)

        with mock.patch.object(RequiredModel._meta.db, 'update') as update:
            m._update({'$unset': {'c': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$unset': {'c': 1}}, **wc_on)

    def test__update_typed_field(self):
        """Test the `_update()` method with a typed field."""

        m = TypedModel(_id=AN_OBJECT_ID)

        with mock.patch.object(TypedModel._meta.db, 'update') as update:
            m._update({'a': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 1}, **wc_on)

    def test__update_typed_field_atomic(self):
        ("Test the `_update()` method with a typed field with an atomic"
         " update.")

        m = TypedModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TypedModel._meta.db, 'update'),
                    mock.patch.object(TypedModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            m._update({'$set': {'a': 1}})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'$set': {'a': 1}}, **wc_on)

    def test__update_type_field_list(self):
        ("Test the `_update()` method with a typed field that is a "
         "typed list.")

        m = TypedListModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TypedListModel._meta.db, 'update'),
                    mock.patch.object(TypedListModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': [1]}

            m._update({'a': [1]})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': [1]}, **wc_on)

    def test__update_typed_field_nested(self):
        ("Test the `_update()` method with a typed field with an "
         "embedded document.")

        TypedEmbeddedModel = ModelFactory('TypedEmbeddedModel',
                                          typed_fields={'a.b': int})

        m = TypedEmbeddedModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TypedEmbeddedModel._meta.db, 'update'),
                    mock.patch.object(TypedEmbeddedModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'b': 1}}

            m._update({'a.b': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a.b': 1}, **wc_on)

    def test__update_type_field_none(self):
        ("Test the `_update()` method with a typed field set to None "
         "instead of a type.")

        TypedNoneModel = ModelFactory('TypedNoneModel',
                                      typed_fields={'a': None})

        m = TypedNoneModel(_id=AN_OBJECT_ID)

        with nested(mock.patch.object(TypedNoneModel._meta.db, 'update'),
                    mock.patch.object(TypedNoneModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            m._update({'a': 1})

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 1}, **wc_on)

    def test__update_typed_field_rename(self):
        ("Test the `_update()` method with a typed field with a "
         "rename.")

        m = TypedModel(_id=AN_OBJECT_ID, b=1)

        # This with needs to go before catch_warnings() or it will a
        # DeprecationWarning will be included in w for Python 2.7+.
        with nested(mock.patch.object(TypedModel._meta.db, 'update'),
                    mock.patch.object(TypedModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': 1}

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter('always')

                m._update({'$rename': {'b': 'a'}})

                update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                          document={'$rename': {'b': 'a'}},
                                          **wc_on)

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, UserWarning))

            expected = ('You are renaming a typed field. Its value may not be '
                        'of the correct type.')
            actual = str(w[-1].message)
            self.assertEqual(actual, expected)

    def test__update_typed_field_types(self):
        ("Test the `_update()` method with different types of typed "
         "fields.")

        MultipleTypedModel = ModelFactory('MultipleTypedModel',
                                          typed_fields={
                                              'int': int,
                                              'string': basestring,
                                              'datetime': datetime.datetime,
                                              'list': list,
                                              'objectid': ObjectId,
                                          })

        m = MultipleTypedModel(_id=AN_OBJECT_ID)

        # Test a successful update.
        with nested(mock.patch.object(MultipleTypedModel._meta.db, 'update'),
                    mock.patch.object(MultipleTypedModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID}

            m._update({
                'int': 1,
                'string': 'value',
                'datetime': datetime.datetime.now(),
                'list': [],
                'objectid': ObjectId(),
            })

            self.assertTrue(update.called)

        # Test a bunch of bad updates.
        with self.assertRaises(TypeError):
            m._update({'int': 'value'})

        with self.assertRaises(TypeError):
            m._update({'datetime': 'value'})

        with self.assertRaises(TypeError):
            m._update({'list': 'value'})

        with self.assertRaises(TypeError):
            m._update({'objectid': 'value'})

    def test__update_typeerror(self):
        """Test that `_update()` raises `TypeError`."""

        m1 = DefaultModel()

        # TypeError should be raised when there's no _id.
        with self.assertRaises(TypeError) as e:
            m1._update({})

        expected = ("The 'DefaultModel' object cannot be updated because its "
                    "'_id' attribute has not been set.")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        # TypeError should be raised when required fields are missing.
        m2 = RequiredModel(_id=AN_OBJECT_ID)
        m3 = RequiredModel(_id=AN_OBJECT_ID, a=1, b=2)

        with self.assertRaises(TypeError) as e:
            m2._update({})

        expected = ("The 'RequiredModel' object cannot be updated because it "
                    "must contain all of the required fields: a, b.")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError) as e:
            m3._update({'$unset': {'a': 1}})

        expected = ("The 'RequiredModel' object cannot be updated because it "
                    "must contain all of the required fields: a, b.")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError) as e:
            m3._update({'$rename': {'a': 'd'}})

        expected = ("The 'RequiredModel' object cannot be updated because it "
                    "must contain all of the required fields: a, b.")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        # TypeError should be raised when typed fields are of the wrong
        # type.
        m4 = TypedModel(_id=AN_OBJECT_ID)

        with self.assertRaises(TypeError) as e:
            m4._update({'a': 'string'})

        expected = ("The 'TypedModel' object cannot be updated because its 'a'"
                    " field must be <type 'int'>.")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError) as e:
            m4._update({'$set': {'a': 'string'}})

        expected = ("The 'TypedModel' object cannot be updated because its 'a'"
                    " field must be <type 'int'>.")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        m5 = TypedListModel(_id=AN_OBJECT_ID)

        with self.assertRaises(TypeError) as e:
            m5._update({'a': ['b']})

        expected = ("The 'TypedListModel' object cannot be updated because its"
                    " 'a' field must be [<type 'int'>].")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

        with self.assertRaises(TypeError) as e:
            m5._update({'a': [1, 'b']})

        expected = ("The 'TypedListModel' object cannot be updated because its"
                    " 'a' field must be [<type 'int'>].")
        actual = str(e.exception)
        self.assertEqual(actual, expected)

    def test__update_use_internal(self):
        """Test the `_update()` method with `use_internal`."""

        m = DefaultModel(_id=AN_OBJECT_ID, a=5)

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            m._update({'a': 1}, use_internal=True)

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a': 5}, **wc_on)

            find_one.assert_not_called()

    def test__update_use_internal_atomic(self):
        ("Test the `_update()` method with `use_internal` with an "
         "atomic update.")

        m = DefaultModel(_id=AN_OBJECT_ID, a__b=5)

        with nested(mock.patch.object(DefaultModel._meta.db, 'update'),
                    mock.patch.object(DefaultModel._meta.db, 'find_one'),
                    ) as (update, find_one):
            find_one.return_value = {'_id': AN_OBJECT_ID, 'a': {'b': 5}}

            m._update({'a.b': 1}, use_internal=True)

            update.assert_called_with(spec={'_id': AN_OBJECT_ID},
                                      document={'a.b': 5}, **wc_on)
