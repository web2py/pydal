# -*- coding: utf-8 -*-
"""
    Unit tests for NoSQL adapters
"""

from __future__ import print_function
import sys
import os
import glob
import datetime
from ._compat import unittest

from pydal._compat import PY2, basestring, StringIO, integer_types

long = integer_types[-1]

from pydal import DAL, Field
from pydal.objects import Table, Query, Expression
from pydal.helpers.classes import SQLALL
from ._adapt import DEFAULT_URI, IS_IMAP, drop, IS_GAE, IS_MONGODB

if IS_IMAP:
    from pydal.adapters import IMAPAdapter
    from pydal.contrib import mockimaplib
    IMAPAdapter.driver = mockimaplib
elif IS_MONGODB:
    from pydal.adapters import MongoDBAdapter
elif IS_GAE:
    # setup GAE dummy database
    from google.appengine.ext import testbed
    gaetestbed = testbed.Testbed()
    gaetestbed.activate()
    gaetestbed.init_datastore_v3_stub()
    gaetestbed.init_memcache_stub()

print('Testing against %s engine (%s)' % (DEFAULT_URI.partition(':')[0],
                                          DEFAULT_URI))

ALLOWED_DATATYPES = [
    'string',
    'text',
    'integer',
    'boolean',
    'double',
    'blob',
    'date',
    'time',
    'datetime',
    'upload',
    'password',
    'json',
    ]

def setUpModule():
    if not IS_IMAP:
        db = DAL(DEFAULT_URI, check_reserved=['all'])

        def clean_table(db, tablename):
            try:
                db.define_table(tablename)
            except Exception as e:
                pass
            try:
                drop(db[tablename])
            except Exception as e:
                pass

        for tablename in ['tt', 't0', 't1', 't2', 't3', 't4',
                          'easy_name', 'tt_archive', 'pet_farm', 'person']:
            clean_table(db, tablename)
        db.close()


def tearDownModule():
    if os.path.isfile('sql.log'):
        os.unlink('sql.log')
    for a in glob.glob('*.table'):
        os.unlink(a)


@unittest.skipIf(not IS_MONGODB, "Skipping MongoDB Tests")
class TestMongo(unittest.TestCase):
    """ Tests specific to MongoDB,  error and side path exercisers, etc
    """

    def testVersionCheck(self):
        driver_args={'fake_version': '2.9 Phony'}
        with self.assertRaises(Exception):
            db = DAL(DEFAULT_URI, attempts=1, check_reserved=['all'],
                     driver_args=driver_args)

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', 'reference'))
        with self.assertRaises(ValueError):
            db.tt.insert(aa='x')
        with self.assertRaises(ValueError):
            db.tt.insert(aa='_')
        with self.assertRaises(TypeError):
            db.tt.insert(aa=3.1)
        self.assertEqual(isinstance(db.tt.insert(aa='<random>'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='0x1'), long), True)
        with self.assertRaises(RuntimeError):
            db(db.tt.aa+1==1).update(aa=0)
        drop(db.tt)

        db.define_table('tt', Field('aa', 'date'))
        self.assertEqual(isinstance(db.tt.insert(aa=None), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, None)
        drop(db.tt)

        db.define_table('tt', Field('aa', 'time'))
        self.assertEqual(isinstance(db.tt.insert(aa=None), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, None)
        with self.assertRaises(RuntimeError):
            db(db.tt.aa <= None).count()
        with self.assertRaises(NotImplementedError):
            db._adapter.select(Query(db, db._adapter.AGGREGATE, db.tt.aa,
                                     'UNKNOWN'), [db.tt.aa], {})
        with self.assertRaises(NotImplementedError):
            db._adapter.select(Expression(db, db._adapter.EXTRACT, db.tt.aa, 
                                          'UNKNOWN', 'integer'), [db.tt.aa], {})
        drop(db.tt)

        db.define_table('tt', Field('aa', 'integer'))
        case=(db.tt.aa == 0).case(db.tt.aa + 2)
        with self.assertRaises(SyntaxError):
            db(case).count()
        drop(db.tt)

        db.define_table('tt', Field('aa'), Field('bb', 'integer'),
                        Field('cc', 'list:integer'))
        db.tt.insert(aa="aa")

        with self.assertRaises(NotImplementedError):
            db((db.tt.aa+1).contains(db.tt.aa)).count()
        with self.assertRaises(NotImplementedError):
            db(db.tt.cc.contains(db.tt.aa)).count()
        with self.assertRaises(NotImplementedError):
            db(db.tt.aa.contains(db.tt.cc)).count()
        with self.assertRaises(NotImplementedError):
            db(db.tt.aa.contains(1.0)).count()
        with self.assertRaises(NotImplementedError):
            db().select(db.tt.aa.lower()[4:-1]).first()
        with self.assertRaises(RuntimeError):
            db(db.tt.aa.belongs(db()._select(db.tt.aa))).count()
        with self.assertRaises(RuntimeError):
            db(db.tt.aa.lower()).update(aa='bb')
        with self.assertRaises(NotImplementedError):
            db(db.tt).select(orderby='<random>')
        with self.assertRaises(RuntimeError):
            db().select()
        with self.assertRaises(RuntimeError):
            MongoDBAdapter.Expanded(db._adapter, 'delete',
                Query(db, db._adapter.EQ, db.tt.aa, 'x'), [True])
        with self.assertRaises(RuntimeError):
            MongoDBAdapter.Expanded(db._adapter, 'delete',
                Query(db, db._adapter.EQ, db.tt.aa, 'x'), [True])
        with self.assertRaises(RuntimeError):
            expanded = MongoDBAdapter.Expanded(db._adapter, 'count',
                Query(db, db._adapter.EQ, db.tt.aa, 'x'), [True])
        expanded = MongoDBAdapter.Expanded(db._adapter, 'count',
            Query(db, db._adapter.EQ, db.tt.aa, 'x'), [])
        self.assertEqual(db._adapter.expand(expanded).query_dict, {'aa': 'x'})

        if db._adapter.server_version_major >= 2.6:
            with self.assertRaises(RuntimeError):
                db(db.tt).update(id=1)
        else:
            db(db.tt).update(id=1)
        self.assertNotEqual(db(db.tt.aa=='aa').select(db.tt.id).response[0][0], 1)
        drop(db.tt)

        db.close()

        for safe in [False, True, False]:
            db = DAL(DEFAULT_URI, check_reserved=['all'])
            db.define_table('tt', Field('aa'))
            self.assertEqual(isinstance(db.tt.insert(aa='x'), long), True)
            with self.assertRaises(RuntimeError):
                db._adapter.delete('tt', 'x', safe=safe)
            self.assertEqual(db._adapter.delete(
                'tt', Query(db, db._adapter.EQ, db.tt.aa, 'x'), safe=safe), 1)
            self.assertEqual(db(db.tt.aa=='x').count(), 0)
            self.assertEqual(db._adapter.update('tt',
                    Query(db, db._adapter.EQ, db.tt.aa, 'x'),
                    db['tt']._listify({'aa':'x'}), safe=safe), 0)
            drop(db.tt)
            db.close()

    def testJoin(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', 'integer'), Field('b', 'reference tt'))
        i1 = db.tt.insert(aa=1)
        db.tt.insert(aa=4, b=i1)
        q = db.tt.b==db.tt.id
        with self.assertRaises(MongoDBAdapter.NotOnNoSqlError):
            db(db.tt).select(left=db.tt.on(q))
        with self.assertRaises(MongoDBAdapter.NotOnNoSqlError):
            db(db.tt).select(join=db.tt.on(q))
        with self.assertRaises(MongoDBAdapter.NotOnNoSqlError):
            db(db.tt).select(db.tt.on(q))
        with self.assertRaises(SyntaxError):
            db(db.tt).select(UNKNOWN=True)
        db(db.tt).select(for_update=True)
        self.assertEqual(db(db.tt).count(), 2)
        db.tt.truncate()
        self.assertEqual(db(db.tt).count(), 0)
        drop(db.tt)
        db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestFields(unittest.TestCase):

    def testFieldName(self):
        """
        - a "str" something
        - not a method or property of Table
        - "dotted-notation" friendly:
            - a valid python identifier
            - not a python keyword
            - not starting with underscore or an integer
            - not containing dots
        
        Basically, anything alphanumeric, no symbols, only underscore as
        punctuation
        """

        # Check that Fields cannot start with underscores
        self.assertRaises(SyntaxError, Field, '_abc', 'string')

        # Check that Fields cannot contain punctuation other than underscores
        self.assertRaises(SyntaxError, Field, 'a.bc', 'string')

        # Check that Fields cannot be a name of a method or property of Table
        for x in ['drop', 'on', 'truncate']:
            self.assertRaises(SyntaxError, Field, x, 'string')

        # Check that Fields allows underscores in the body of a field name.
        self.assertTrue(Field('a_bc', 'string'),
            "Field isn't allowing underscores in fieldnames.  It should.")

        # Check that Field names don't allow a python keyword
        self.assertRaises(SyntaxError, Field, 'True', 'string')
        self.assertRaises(SyntaxError, Field, 'elif', 'string')
        self.assertRaises(SyntaxError, Field, 'while', 'string')

        # Check that Field names don't allow a non-valid python identifier
        non_valid_examples = ["1x", "xx$%@%", "xx yy", "yy\na", "yy\n"]
        for a in non_valid_examples:
            self.assertRaises(SyntaxError, Field, a, 'string')

        # Check that Field names don't allow a unicode string
        non_valid_examples = non_valid_examples = ["ℙƴ☂ℌøἤ", u"ℙƴ☂ℌøἤ", 
                u'àè', u'ṧøмℯ', u'тεṧт', u'♥αłüℯṧ', 
                u'ℊεᾔ℮яαт℮∂', u'♭ƴ', u'ᾔ☤ρℌℓ☺ḓ']
        for a in non_valid_examples:
            self.assertRaises(SyntaxError, Field, a, 'string')

    def testFieldTypes(self):

        # Check that string, and password default length is 512
        for typ in ['string', 'password']:
            self.assertTrue(Field('abc', typ).length == 512,
                         "Default length for type '%s' is not 512 or 255" % typ)

        # Check that upload default length is 512
        self.assertTrue(Field('abc', 'upload').length == 512,
                     "Default length for type 'upload' is not 512")

        # Check that Tables passed in the type creates a reference
        self.assertTrue(Field('abc', Table(None, 'temp')).type
                      == 'reference temp',
                     'Passing a Table does not result in a reference type.')

    def testFieldLabels(self):

        # Check that a label is successfully built from the supplied fieldname
        self.assertTrue(Field('abc', 'string').label == 'Abc',
                     'Label built is incorrect')
        self.assertTrue(Field('abc_def', 'string').label == 'Abc Def',
                     'Label built is incorrect')

    def testFieldFormatters(self):  # Formatter should be called Validator

        # Test the default formatters
        for typ in ALLOWED_DATATYPES:
            f = Field('abc', typ)
            if typ not in ['date', 'time', 'datetime']:
                isinstance(f.formatter('test'), str)
            else:
                isinstance(f.formatter(datetime.datetime.now()), str)

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        import pickle

        # some db's only support milliseconds
        datetime_datetime_today = datetime.datetime.today()
        datetime_datetime_today = datetime_datetime_today.replace(
            microsecond = datetime_datetime_today.microsecond -
                          datetime_datetime_today.microsecond % 1000)

        insert_vals = [
            ('string', 'x', ''),
            ('string', 'A\xc3\xa9 A', ''),
            ('text', 'x', ''),
            ('password', 'x', ''),
            ('upload', 'x', ''),
            ('double', 3.1, 1),
            ('integer', 3, 1),
            ('boolean', True, True),
            ('date', datetime.date.today(), datetime.date.today()),
            ('datetime', datetime.datetime(1971, 12, 21, 10, 30, 55, 0),
                datetime_datetime_today),
            ('time', datetime_datetime_today.time(),
                datetime_datetime_today.time()),
            ('blob', 'x', ''),
            ('blob', b'xyzzy', ''),
            # pickling a tuple will create a string which is not UTF-8 able.
            ('blob', pickle.dumps((0,), pickle.HIGHEST_PROTOCOL), ''),
            ]

        if not IS_GAE:
            # these are unsupported by GAE
            insert_vals.append(('blob', bytearray('a','utf-8'), ''))
            insert_vals.append(('json', {'a': 'b', 'c': [1, 2]}, {}))

        for iv in insert_vals:
            db.define_table('tt', Field('aa', iv[0], default=iv[2]))
            # empty string stored to blob returns None
            default_return = None if iv[0] == 'blob' and iv[2] == '' else iv[2]
            self.assertTrue(isinstance(db.tt.insert(), long))
            self.assertTrue(isinstance(db.tt.insert(aa=iv[1]), long))
            self.assertTrue(isinstance(db.tt.insert(aa=None), long))
            self.assertEqual(db().select(db.tt.aa)[0].aa, default_return)
            self.assertEqual(db().select(db.tt.aa)[1].aa, iv[1])
            self.assertEqual(db().select(db.tt.aa)[2].aa, None)

            if not IS_GAE:
                ## field aliases
                row = db().select(db.tt.aa.with_alias('zz'))[1]
                self.assertEqual(row['zz'], iv[1])

            drop(db.tt)

        ## Row APIs
        db.define_table('tt', Field('aa', 'datetime',
                        default=datetime.datetime.today()))
        t0 = datetime.datetime(1971, 12, 21, 10, 30, 55, 0)
        id = db.tt.insert(aa=t0)
        self.assertEqual(isinstance(id, long), True)

        row = db().select(db.tt.aa)[0]
        self.assertEqual(db.tt[id].aa,t0)
        self.assertEqual(db.tt['aa'],db.tt.aa)
        self.assertEqual(db.tt(id).aa,t0)
        self.assertTrue(db.tt(id,aa=None)==None)
        self.assertFalse(db.tt(id,aa=t0)==None)
        self.assertEqual(row.aa,t0)
        self.assertEqual(row['aa'],t0)
        self.assertEqual(row['tt.aa'],t0)
        self.assertEqual(row('tt.aa'),t0)

        ## Lazy and Virtual fields
        db.tt.b = Field.Virtual(lambda row: row.tt.aa)
        db.tt.c = Field.Lazy(lambda row: row.tt.aa)
        row = db().select(db.tt.aa)[0]
        self.assertEqual(row.b,t0)
        self.assertEqual(row.c(),t0)

        drop(db.tt)
        db.define_table('tt', Field('aa', 'time', default='11:30'))
        t0 = datetime.time(10, 30, 55)
        self.assertEqual(isinstance(db.tt.insert(aa=t0), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        drop(db.tt)
        db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestTables(unittest.TestCase):

    def testTableNames(self):
        """
        - a "str" something
        - not a method or property of DAL
        - "dotted-notation" friendly:
            - a valid python identifier
            - not a python keyword
            - not starting with underscore or an integer
            - not containing dots
        
        Basically, anything alphanumeric, no symbols, only underscore as
        punctuation
        """

        # Check that Tables cannot start with underscores
        self.assertRaises(SyntaxError, Table, None, '_abc')

        # Check that Tables cannot contain punctuation other than underscores
        self.assertRaises(SyntaxError, Table, None, 'a.bc')

        # Check that Tables cannot be a name of a method or property of DAL
        for x in ['define_table', 'tables', 'as_dict']:
            self.assertRaises(SyntaxError, Table, None, x)

        # Check that Table allows underscores in the body of a field name.
        self.assertTrue(Table(None, 'a_bc'),
            "Table isn't allowing underscores in tablename.  It should.")

        # Check that Table names don't allow a python keyword
        self.assertRaises(SyntaxError, Table, None, 'True')
        self.assertRaises(SyntaxError, Table, None, 'elif')
        self.assertRaises(SyntaxError, Table, None, 'while')

        # Check that Table names don't allow a non-valid python identifier
        non_valid_examples = ["1x", "xx$%@%", "xx yy", "yy\na", "yy\n"]
        for a in non_valid_examples:
            self.assertRaises(SyntaxError, Table, None, a)

        # Check that Table names don't allow a unicode string
        non_valid_examples = ["ℙƴ☂ℌøἤ", u"ℙƴ☂ℌøἤ", 
                u'àè', u'ṧøмℯ', u'тεṧт', u'♥αłüℯṧ', 
                u'ℊεᾔ℮яαт℮∂', u'♭ƴ', u'ᾔ☤ρℌℓ☺ḓ']
        for a in non_valid_examples:
            self.assertRaises(SyntaxError, Table, None, a)


@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestAll(unittest.TestCase):

    def setUp(self):
        self.pt = Table(None,'PseudoTable',Field('name'),Field('birthdate'))

    def testSQLALL(self):
        ans = 'PseudoTable.id, PseudoTable.name, PseudoTable.birthdate'
        self.assertEqual(str(SQLALL(self.pt)), ans)

@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestTable(unittest.TestCase):

    def testTableCreation(self):

        # Check for error when not passing type other than Field or Table

        self.assertRaises(SyntaxError, Table, None, 'test', None)

        persons = Table(None, 'persons',
                        Field('firstname','string'),
                        Field('lastname', 'string'))

        # Does it have the correct fields?

        self.assertTrue(set(persons.fields).issuperset(set(['firstname',
                                                         'lastname'])))

        # ALL is set correctly

        self.assertTrue('persons.firstname, persons.lastname'
                      in str(persons.ALL))

    def testTableAlias(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        persons = Table(db, 'persons', Field('firstname',
                           'string'), Field('lastname', 'string'))
        aliens = persons.with_alias('aliens')

        # Are the different table instances with the same fields

        self.assertTrue(persons is not aliens)
        self.assertTrue(set(persons.fields) == set(aliens.fields))
        db.close()

    def testTableInheritance(self):
        persons = Table(None, 'persons', Field('firstname',
                           'string'), Field('lastname', 'string'))
        customers = Table(None, 'customers',
                             Field('items_purchased', 'integer'),
                             persons)
        self.assertTrue(set(customers.fields).issuperset(set(
            ['items_purchased', 'firstname', 'lastname'])))


class TestInsert(unittest.TestCase):
    def testRun(self):
        if IS_IMAP:
            imap = DAL(DEFAULT_URI)
            imap.define_tables()
            self.assertEqual(imap.Draft.insert(to="nurse@example.com",
                                               subject="Nurse!",
                                               sender="gumby@example.com",
                                               content="Nurse!\r\nNurse!"), 2)
            self.assertEqual(imap.Draft[2].subject, "Nurse!")
            self.assertEqual(imap.Draft[2].sender, "gumby@example.com")
            self.assertEqual(isinstance(imap.Draft[2].uid, long), True)
            self.assertEqual(imap.Draft[2].content[0]["text"], "Nurse!\r\nNurse!")
            imap.close()
        else:
            db = DAL(DEFAULT_URI, check_reserved=['all'])
            db.define_table('tt', Field('aa'))
            self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
            self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
            self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
            self.assertEqual(db(db.tt.aa == '1').count(), 3)
            self.assertEqual(db(db.tt.aa == '2').isempty(), True)
            self.assertEqual(db(db.tt.aa == '1').update(aa='2'), 3)
            self.assertEqual(db(db.tt.aa == '2').count(), 3)
            self.assertEqual(db(db.tt.aa == '2').isempty(), False)
            self.assertEqual(db(db.tt.aa == '2').delete(), 3)
            self.assertEqual(db(db.tt.aa == '2').isempty(), True)

            def callable():
                return 'aa'
            self.assertTrue(isinstance(db.tt.insert(aa=callable), long))
            self.assertEqual(db(db.tt.aa == 'aa').count(), 1)

            drop(db.tt)
            db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestSelect(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'))
        self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='2'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='3'), long), True)
        self.assertEqual(db(db.tt.id > 0).count(), 3)
        self.assertEqual(db(db.tt.aa).count(), 3)
        self.assertEqual(db(db.tt.id).count(), 3)
        self.assertEqual(db(db.tt.id!=None).count(), 3)

        self.assertEqual(db(db.tt.id > 0).select(orderby=~db.tt.aa | db.tt.id)[0].aa, '3')
        self.assertEqual(db(db.tt.id > 0).select(orderby=~db.tt.aa)[0].aa, '3')
        self.assertEqual(len(db(db.tt.id > 0).select(limitby=(1, 2))), 1)
        self.assertEqual(db(db.tt.id > 0).select(limitby=(1, 2))[0].aa, '2')
        self.assertEqual(len(db().select(db.tt.ALL)), 3)
        self.assertEqual(db(db.tt.aa == None).count(), 0)
        self.assertEqual(db(db.tt.aa != None).count(), 3)
        self.assertEqual(db(db.tt.aa > '1').count(), 2)
        self.assertEqual(db(db.tt.aa >= '1').count(), 3)
        self.assertEqual(db(db.tt.aa == '1').count(), 1)
        self.assertEqual(db(db.tt.aa != '1').count(), 2)
        self.assertEqual(db(db.tt.aa < '3').count(), 2)
        self.assertEqual(db(db.tt.aa <= '3').count(), 3)
        self.assertEqual(db(db.tt.aa > '1')(db.tt.aa < '3').count(), 1)
        self.assertEqual(db((db.tt.aa > '1') & (db.tt.aa < '3')).count(), 1)
        self.assertEqual(db((db.tt.aa > '1') | (db.tt.aa < '3')).count(), 3)
        # Test not operator
        self.assertEqual(db(~(db.tt.aa != '1')).count(), 1)
        self.assertEqual(db(~(db.tt.aa == '1')).count(), 2)
        self.assertEqual(db((db.tt.aa > '1') & ~(db.tt.aa > '2')).count(), 1)
        self.assertEqual(db(~(db.tt.aa > '1') & (db.tt.aa > '2')).count(), 0)
        self.assertEqual(db(~((db.tt.aa < '1') | (db.tt.aa > '2'))).count(), 2)
        self.assertEqual(db(~((db.tt.aa >= '1') & (db.tt.aa <= '2'))).count(), 1)
        drop(db.tt)
        db.close()

    def testListInteger(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', 
                        Field('aa', 'list:integer'))
        l=[0,1,2,3,4,5]
        db.tt.insert(aa=l)
        self.assertEqual(db(db.tt).select('tt.aa').first()[db.tt.aa],l)
        drop(db.tt)
        db.close()

    def testListString(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', 
                        Field('aa', 'list:string'))
        l=['a', 'b', 'c']
        db.tt.insert(aa=l)
        self.assertEqual(db(db.tt).select('tt.aa').first()[db.tt.aa],l)
        drop(db.tt)
        db.close()

    def testListReference(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        on_deletes = (
            'CASCADE',
            'SET NULL',
        )
        for ondelete in on_deletes:
            db.define_table('t0', Field('aa', 'string'))
            db.define_table('tt', Field('t0_id', 'list:reference t0',
                                        ondelete=ondelete))
            id_a1=db.t0.insert(aa='test1')
            id_a2=db.t0.insert(aa='test2')
            ref1=[id_a1]
            ref2=[id_a2]
            ref3=[id_a1, id_a2]
            db.tt.insert(t0_id=ref1)
            self.assertEqual(
                db(db.tt).select(db.tt.t0_id).last()[db.tt.t0_id], ref1)
            db.tt.insert(t0_id=ref2)
            self.assertEqual(
                db(db.tt).select(db.tt.t0_id).last()[db.tt.t0_id], ref2)
            db.tt.insert(t0_id=ref3)
            self.assertEqual(
                db(db.tt).select(db.tt.t0_id).last()[db.tt.t0_id], ref3)

            if IS_MONGODB:
                self.assertEqual(db(db.tt.t0_id.contains(id_a1)).count(), 2)
                self.assertEqual(db(db.tt.t0_id.contains(id_a2)).count(), 2)
                db(db.t0.aa == 'test1').delete()
                if ondelete == 'SET NULL':
                    self.assertEqual(db(db.tt).count(), 3)
                    self.assertEqual(db(db.tt).select()[0].t0_id, [])
                if ondelete == 'CASCADE':
                    self.assertEqual(db(db.tt).count(), 2)
                    self.assertEqual(db(db.tt).select()[0].t0_id, ref2)

            drop(db.tt)
            drop(db.t0)
        db.close()

    @unittest.skipIf(IS_GAE, "no groupby in appengine")
    def testGroupByAndDistinct(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt',
                        Field('aa'),
                        Field('bb', 'integer'),
                        Field('cc', 'integer'))
        db.tt.insert(aa='4', bb=1, cc=1)
        db.tt.insert(aa='3', bb=2, cc=1)
        db.tt.insert(aa='3', bb=1, cc=1)
        db.tt.insert(aa='1', bb=1, cc=1)
        db.tt.insert(aa='1', bb=2, cc=1)
        db.tt.insert(aa='1', bb=3, cc=1)
        db.tt.insert(aa='1', bb=4, cc=1)
        db.tt.insert(aa='2', bb=1, cc=1)
        db.tt.insert(aa='2', bb=2, cc=1)
        db.tt.insert(aa='2', bb=3, cc=1)
        self.assertEqual(db(db.tt).count(), 10)

        # test groupby
        result = db().select(db.tt.aa, db.tt.bb.sum(), groupby=db.tt.aa)
        self.assertEqual(len(result), 4)
        result = db().select(db.tt.aa, db.tt.bb.sum(),
                             groupby=db.tt.aa, orderby=db.tt.aa)
        self.assertEqual(tuple(result.response[2]), ('3', 3))
        result = db().select(db.tt.aa, db.tt.bb.sum(),
                             groupby=db.tt.aa, orderby=~db.tt.aa)
        self.assertEqual(tuple(result.response[1]), ('3', 3))
        result = db().select(db.tt.aa, db.tt.bb, db.tt.cc.sum(),
                             groupby=db.tt.aa|db.tt.bb,
                             orderby=(db.tt.aa|~db.tt.bb))
        self.assertEqual(tuple(result.response[4]), ('2', 3, 1))
        result = db().select(db.tt.aa, db.tt.bb.sum(),
                             groupby=db.tt.aa, orderby=~db.tt.aa, limitby=(1,2))
        self.assertEqual(len(result), 1)
        self.assertEqual(tuple(result.response[0]), ('3', 3))
        result = db().select(db.tt.aa, db.tt.bb.sum(),
                             groupby=db.tt.aa, limitby=(0,3))
        self.assertEqual(len(result), 3)
        self.assertEqual(tuple(result.response[2]), ('3', 3))

        # test having
        self.assertEqual(len(db().select(db.tt.aa, db.tt.bb.sum(),
                        groupby=db.tt.aa, having=db.tt.bb.sum() > 2)), 3)

        # test distinct
        result = db().select(db.tt.aa, db.tt.cc, distinct=True)
        self.assertEqual(len(result), 4)
        result = db().select(db.tt.cc, distinct=True, groupby=db.tt.cc)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].cc, 1)
        result = db().select(db.tt.aa, distinct=True, orderby=~db.tt.aa)
        self.assertEqual(result[2].aa, '2')
        self.assertEqual(result[1].aa, '3')
        result = db().select(db.tt.aa, db.tt.bb,
                             distinct=True, orderby=(db.tt.aa|~db.tt.bb))
        self.assertEqual(tuple(result.response[4]), ('2', 3))
        result = db().select(db.tt.aa,
                             distinct=db.tt.aa, orderby=~db.tt.aa, limitby=(1,2))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].aa, '3')

        # test count distinct
        db.tt.insert(aa='2', bb=3, cc=1)
        self.assertEqual(db(db.tt).count(distinct=db.tt.aa), 4)
        self.assertEqual(db(db.tt).count(distinct=db.tt.aa|db.tt.bb), 10)
        self.assertEqual(db(db.tt).count(distinct=db.tt.aa|db.tt.bb|db.tt.cc), 10)
        self.assertEqual(db(db.tt).count(distinct=True), 10)
        self.assertEqual(db(db.tt.aa).count(db.tt.aa), 4)
        self.assertEqual(db(db.tt.aa).count(), 11)
        count=db.tt.aa.count()
        self.assertEqual(db(db.tt).select(count).first()[count], 11)

        count=db.tt.aa.count(distinct=True)
        sum=db.tt.bb.sum()
        result = db(db.tt).select(count, sum)
        self.assertEqual(tuple(result.response[0]), (4, 23))
        self.assertEqual(result.first()[count], 4)
        self.assertEqual(result.first()[sum], 23)

        if not IS_MONGODB or db._adapter.server_version_major >= 2.6:
            # mongo < 2.6 does not support $size
            count=db.tt.aa.count(distinct=True)+db.tt.bb.count(distinct=True)
            self.assertEqual(db(db.tt).select(count).first()[count], 8)

        drop(db.tt)
        db.close()

    @unittest.skipIf(IS_GAE, "no coalesce in appengine")
    def testCoalesce(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), Field('bb'), Field('cc'), Field('dd'))
        db.tt.insert(aa='xx')
        db.tt.insert(aa='xx', bb='yy')
        db.tt.insert(aa='xx', bb='yy', cc='zz')
        db.tt.insert(aa='xx', bb='yy', cc='zz', dd='')
        result = db(db.tt).select(db.tt.dd.coalesce(db.tt.cc, db.tt.bb, db.tt.aa))
        self.assertEqual(result.response[0][0], 'xx')
        self.assertEqual(result.response[1][0], 'yy')
        self.assertEqual(result.response[2][0], 'zz')
        self.assertEqual(result.response[3][0], '')
        db.tt.drop()

        db.define_table('tt', Field('aa', 'integer'), Field('bb'))
        db.tt.insert(bb='')
        db.tt.insert(aa=1)
        result = db(db.tt).select(db.tt.aa.coalesce_zero())
        self.assertEqual(result.response[0][0], 0)
        self.assertEqual(result.response[1][0], 1)

        db.tt.drop()
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestAddMethod(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'))

        @db.tt.add_method.all
        def select_all(table,orderby=None):
            return table._db(table).select(orderby=orderby)
        self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='2'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='3'), long), True)
        self.assertEqual(len(db.tt.all()), 3)
        drop(db.tt)
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestBelongs(unittest.TestCase):

    def __init__(self, *args, **vars):
        unittest.TestCase.__init__(self, *args, **vars)
        self.db = None

    def setUp(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'))
        self.i_id = db.tt.insert(aa='1')
        self.assertEqual(isinstance(self.i_id, long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='2'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='3'), long), True)
        self.db = db

    def testRun(self):
        db = self.db
        self.assertEqual(db(db.tt.aa.belongs(('1', '3'))).count(), 2)
        self.assertEqual(db(db.tt.aa.belongs(['1', '3'])).count(), 2)
        self.assertEqual(db(db.tt.aa.belongs(['1', '3'])).count(), 2)
        self.assertEqual(db(db.tt.id.belongs([self.i_id])).count(), 1)
        self.assertEqual(db(db.tt.id.belongs([])).count(), 0)

    @unittest.skipIf(IS_GAE or IS_MONGODB, "Datastore/Mongodb belongs() does not accept nested queries")
    def testNested(self):
        db = self.db
        self.assertEqual(db(db.tt.aa.belongs(db(db.tt.id == self.i_id)._select(db.tt.aa))).count(), 1)

        self.assertEqual(db(db.tt.aa.belongs(db(db.tt.aa.belongs(('1',
                     '3')))._select(db.tt.aa))).count(), 2)
        self.assertEqual(db(db.tt.aa.belongs(db(db.tt.aa.belongs(db
                     (db.tt.aa.belongs(('1', '3')))._select(db.tt.aa)))._select(
                     db.tt.aa))).count(),
                     2)

    def tearDown(self):
        db = self.db
        drop(db.tt)
        db.close()
        self.db = None


@unittest.skipIf(IS_GAE or IS_IMAP, "Contains not supported on GAE Datastore. TODO: IMAP tests")
class TestContains(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', 'list:string'), Field('bb','string'))
        self.assertEqual(isinstance(db.tt.insert(aa=['aaa','bbb'],bb='aaa'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=['bbb','ddd'],bb='abb'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=['eee','aaa'],bb='acc'), long), True)
        self.assertEqual(db(db.tt.aa.contains('aaa')).count(), 2)
        self.assertEqual(db(db.tt.aa.contains('bbb')).count(), 2)
        self.assertEqual(db(db.tt.aa.contains('aa')).count(), 0)
        self.assertEqual(db(db.tt.bb.contains('a')).count(), 3)
        self.assertEqual(db(db.tt.bb.contains('b')).count(), 1)
        self.assertEqual(db(db.tt.bb.contains('d')).count(), 0)
        self.assertEqual(db(db.tt.aa.contains(db.tt.bb)).count(), 1)

        # case-sensitivity tests, if 1 it isn't
        is_case_insensitive = db(db.tt.bb.contains('AAA', case_sensitive=True)).count()
        if is_case_insensitive:
            self.assertEqual(db(db.tt.aa.contains('AAA')).count(), 2)
            self.assertEqual(db(db.tt.bb.contains('A')).count(), 3)
        else:
            self.assertEqual(db(db.tt.aa.contains('AAA', case_sensitive=True)).count(), 0)
            self.assertEqual(db(db.tt.bb.contains('A', case_sensitive=True)).count(), 0)
            self.assertEqual(db(db.tt.aa.contains('AAA', case_sensitive=False)).count(), 2)
            self.assertEqual(db(db.tt.bb.contains('A', case_sensitive=False)).count(), 3)
        db.tt.drop()

        # integers in string fields
        db.define_table('tt', Field('aa', 'list:string'), Field('bb','string'), Field('cc','integer'))
        self.assertEqual(isinstance(db.tt.insert(aa=['123','456'],bb='123', cc=12), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=['124','456'],bb='123', cc=123), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=['125','457'],bb='23', cc=125), long), True)
        self.assertEqual(db(db.tt.aa.contains(123)).count(), 1)
        self.assertEqual(db(db.tt.aa.contains(23)).count(), 0)
        self.assertEqual(db(db.tt.aa.contains(db.tt.cc)).count(), 1)
        self.assertEqual(db(db.tt.bb.contains(123)).count(), 2)
        self.assertEqual(db(db.tt.bb.contains(23)).count(), 3)
        self.assertEqual(db(db.tt.bb.contains(db.tt.cc)).count(), 2)
        db.tt.drop()

        # string field contains string field
        db.define_table('tt', Field('aa'), Field('bb'))
        db.tt.insert(aa='aaa', bb='%aaa')
        db.tt.insert(aa='aaa', bb='aaa')
        self.assertEqual(db(db.tt.aa.contains(db.tt.bb)).count(), 1)
        drop(db.tt)

        # escaping
        db.define_table('tt', Field('aa'))
        db.tt.insert(aa='perc%ent')
        db.tt.insert(aa='percent')
        db.tt.insert(aa='percxyzent')
        db.tt.insert(aa='under_score')
        db.tt.insert(aa='underxscore')
        db.tt.insert(aa='underyscore')
        self.assertEqual(db(db.tt.aa.contains('perc%ent')).count(), 1)
        self.assertEqual(db(db.tt.aa.contains('under_score')).count(), 1)
        drop(db.tt)
        db.close()


@unittest.skipIf(IS_GAE, "Like not supported on GAE Datastore.")
@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestLike(unittest.TestCase):

    def setUp(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'))
        self.assertEqual(isinstance(db.tt.insert(aa='abc'), long), True)
        self.db = db

    def tearDown(self):
        db = self.db
        drop(db.tt)
        db.close()
        self.db = None

    def testRun(self):
        db = self.db
        self.assertEqual(db(db.tt.aa.like('a%')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%b%')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%c')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%d%')).count(), 0)
        self.assertEqual(db(db.tt.aa.like('ab_')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('a_c')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('_bc')).count(), 1)

        self.assertEqual(db(db.tt.aa.like('A%', case_sensitive=False)).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%B%', case_sensitive=False)).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%C', case_sensitive=False)).count(), 1)
        self.assertEqual(db(db.tt.aa.ilike('A%')).count(), 1)
        self.assertEqual(db(db.tt.aa.ilike('%B%')).count(), 1)
        self.assertEqual(db(db.tt.aa.ilike('%C')).count(), 1)

        #DAL maps like() (and contains(), startswith(), endswith())
        #to the LIKE operator, that in ANSI-SQL is case-sensitive
        #There are backends supporting case-sensitivity by default
        #and backends that needs additional care to turn
        #case-sensitivity on. To discern among those, let's run
        #this query comparing previously inserted 'abc' with 'ABC':
        #if the result is 0, then the backend recognizes
        #case-sensitivity, if 1 it isn't
        is_case_insensitive = db(db.tt.aa.like('%ABC%')).count()
        self.assertEqual(db(db.tt.aa.like('A%')).count(), is_case_insensitive)
        self.assertEqual(db(db.tt.aa.like('%B%')).count(), is_case_insensitive)
        self.assertEqual(db(db.tt.aa.like('%C')).count(), is_case_insensitive)

    def testUpperLower(self):
        db = self.db
        self.assertEqual(db(db.tt.aa.upper().like('A%')).count(), 1)
        self.assertEqual(db(db.tt.aa.upper().like('%B%')).count(),1)
        self.assertEqual(db(db.tt.aa.upper().like('%C')).count(), 1)
        self.assertEqual(db(db.tt.aa.lower().like('%c')).count(), 1)

    def testStartsEndsWith(self):
        db = self.db
        self.assertEqual(db(db.tt.aa.startswith('a')).count(), 1)
        self.assertEqual(db(db.tt.aa.endswith('c')).count(), 1)
        self.assertEqual(db(db.tt.aa.startswith('c')).count(), 0)
        self.assertEqual(db(db.tt.aa.endswith('a')).count(), 0)

    def testEscaping(self):
        db = self.db
        term = 'ahbc'.replace('h', '\\') #funny but to avoid any doubts...
        db.tt.insert(aa='a%bc')
        db.tt.insert(aa='a_bc')
        db.tt.insert(aa=term)
        self.assertEqual(db(db.tt.aa.like('%ax%bc%', escape='x')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%ax_bc%', escape='x')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%'+term+'%')).count(), 1)
        db(db.tt.id>0).delete()
        # test "literal" like, i.e. exactly as LIKE in the backend
        db.tt.insert(aa='perc%ent')
        db.tt.insert(aa='percent')
        db.tt.insert(aa='percxyzent')
        db.tt.insert(aa='under_score')
        db.tt.insert(aa='underxscore')
        db.tt.insert(aa='underyscore')
        self.assertEqual(db(db.tt.aa.like('%perc%ent%')).count(), 3)
        self.assertEqual(db(db.tt.aa.like('%under_score%')).count(), 3)
        db(db.tt.id>0).delete()
        # escaping with startswith and endswith
        db.tt.insert(aa='%percent')
        db.tt.insert(aa='xpercent')
        db.tt.insert(aa='discount%')
        db.tt.insert(aa='discountx')
        self.assertEqual(db(db.tt.aa.endswith('discount%')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('discount%%')).count(), 2)
        self.assertEqual(db(db.tt.aa.startswith('%percent')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%%percent')).count(), 2)

    def testRegexp(self):
        db = self.db
        db(db.tt.id>0).delete()
        db.tt.insert(aa='%percent')
        db.tt.insert(aa='xpercent')
        db.tt.insert(aa='discount%')
        db.tt.insert(aa='discountx')
        try:
            self.assertEqual(db(db.tt.aa.regexp('count')).count(), 2)
        except NotImplementedError:
            pass
        else:
            self.assertEqual(db(db.tt.aa.lower().regexp('count')).count(), 2)
            self.assertEqual(db(db.tt.aa.upper().regexp('COUNT') &
                                db.tt.aa.lower().regexp('count')).count(), 2)
            self.assertEqual(db(db.tt.aa.upper().regexp('COUNT') |
                                (db.tt.aa.lower()=='xpercent')).count(), 3)

    def testLikeInteger(self):
        db = self.db
        db.tt.drop()
        db.define_table('tt', Field('aa', 'integer'))
        self.assertEqual(isinstance(db.tt.insert(aa=1111111111), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=1234567), long), True)
        self.assertEqual(db(db.tt.aa.like('1%')).count(), 2)
        self.assertEqual(db(db.tt.aa.like('1_3%')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('2%')).count(), 0)
        self.assertEqual(db(db.tt.aa.like('_2%')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('12%')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('012%')).count(), 0)
        self.assertEqual(db(db.tt.aa.like('%45%')).count(), 1)
        self.assertEqual(db(db.tt.aa.like('%54%')).count(), 0)


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestDatetime(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', 'datetime'))
        self.assertEqual(isinstance(db.tt.insert(aa=datetime.datetime(1971, 12, 21,
                         11, 30)), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=datetime.datetime(1971, 11, 21,
                         10, 30)), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=datetime.datetime(1970, 12, 21,
                         9, 31)), long), True)
        self.assertEqual(db(db.tt.aa == datetime.datetime(1971, 12,
                         21, 11, 30)).count(), 1)
        self.assertEqual(db(db.tt.aa >= datetime.datetime(1971, 1, 1)).count(), 2)

        if IS_MONGODB:
            self.assertEqual(db(db.tt.aa.year() == 1971).count(), 2)
            self.assertEqual(db(db.tt.aa.month() > 11).count(), 2)
            self.assertEqual(db(db.tt.aa.day() >= 21).count(), 3)
            self.assertEqual(db(db.tt.aa.hour() < 10).count(), 1)
            self.assertEqual(db(db.tt.aa.minutes() <= 30).count(), 2)
            self.assertEqual(db(db.tt.aa.seconds() != 31).count(), 3)
            self.assertEqual(db(db.tt.aa.epoch() < 365*24*3600).delete(), 1)
        drop(db.tt)

        db.define_table('tt', Field('aa', 'time'))
        t0 = datetime.time(10, 30, 55)
        db.tt.insert(aa=t0)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        drop(db.tt)
        
        db.define_table('tt', Field('aa', 'date'))
        t0 = datetime.date.today()
        db.tt.insert(aa=t0)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        drop(db.tt)
        db.close()


@unittest.skipIf(IS_GAE or IS_IMAP, "Expressions are not supported")
class TestExpressions(unittest.TestCase):

    def testRun(self):
        if IS_MONGODB:
            DAL_OPTS = (
                (True,  {'adapter_args': {'safe': True}}),
                (False, {'adapter_args': {'safe': False}}),
            )
        for dal_opt in DAL_OPTS:
            db = DAL(DEFAULT_URI, check_reserved=['all'], **dal_opt[1])
            db.define_table('tt', Field('aa', 'integer'), 
                            Field('bb', 'integer', default=0), Field('cc'))
            self.assertEqual(isinstance(db.tt.insert(aa=1), long), dal_opt[0])
            self.assertEqual(isinstance(db.tt.insert(aa=2), long), dal_opt[0])
            self.assertEqual(isinstance(db.tt.insert(aa=3), long), dal_opt[0])

            # test update
            self.assertEqual(db(db.tt.aa == 3).update(aa=db.tt.aa + 1,
                                                      bb=db.tt.bb + 2), 1)
            self.assertEqual(db(db.tt.aa == 4).count(), 1)
            self.assertEqual(db(db.tt.bb == 2).count(), 1)
            self.assertEqual(db(db.tt.aa == -2).count(), 0)
            self.assertEqual(db(db.tt.aa == 4).update(aa=db.tt.aa * 2, bb=5), 1)
            self.assertEqual(db(db.tt.bb == 5).count(), 1)
            self.assertEqual(db(db.tt.aa + 1 == 9).count(), 1)
            self.assertEqual(db(db.tt.aa + 1 == 9).update(aa=db.tt.aa - 2,
                                                      cc='cc'), 1)
            self.assertEqual(db(db.tt.cc == 'cc').count(), 1)
            self.assertEqual(db(db.tt.aa == 6).count(), 1)
            self.assertEqual(db(db.tt.aa == 6).update(bb=db.tt.aa *
                                                         (db.tt.bb - 3)), 1)
            self.assertEqual(db(db.tt.bb == 12).count(), 1)
            self.assertEqual(db(db.tt.aa == 6).count(), 1)
            self.assertEqual(db(db.tt.aa == 6).update(aa=db.tt.aa % 4 + 1,
                                                      cc=db.tt.cc + '1' +'1'), 1)
            self.assertEqual(db(db.tt.cc == 'cc11').count(), 1)
            self.assertEqual(db(db.tt.aa == 3).count(), 1)

            # test comparsion expression based count
            self.assertEqual(db(db.tt.aa != db.tt.aa).count(), 0)
            self.assertEqual(db(db.tt.aa == db.tt.aa).count(), 3)

            # test select aggregations
            sum = (db.tt.aa + 1).sum()
            self.assertEqual(db(db.tt.aa + 1 >= 3).select(sum).first()[sum], 7)
            self.assertEqual(db((1==0) & (db.tt.aa >= db.tt.aa)).count(), 0)
            self.assertEqual(db(db.tt.aa * 2 == -2).select(sum).first()[sum], None)

            count=db.tt.aa.count()
            avg=db.tt.aa.avg()
            min=db.tt.aa.min()
            max=db.tt.aa.max()
            result = db(db.tt).select(sum, count, avg, min, max).first()
            self.assertEqual(result[sum], 9)
            self.assertEqual(result[count], 3)
            self.assertEqual(result[avg], 2)
            self.assertEqual(result[min], 1)
            self.assertEqual(result[max], 3)

            # Test basic expressions evaluated at python level
            self.assertEqual(db((1==1) & (db.tt.aa >= 2)).count(), 2)
            self.assertEqual(db((1==1) | (db.tt.aa >= 2)).count(), 3)
            self.assertEqual(db((1==0) & (db.tt.aa >= 2)).count(), 0)
            self.assertEqual(db((1==0) | (db.tt.aa >= 2)).count(), 2)

            # test abs()
            self.assertEqual(db(db.tt.aa == 2).update(aa=db.tt.aa*-10), 1)
            abs=db.tt.aa.abs().with_alias('abs')
            result = db(db.tt.aa == -20).select(abs).first()
            self.assertEqual(result[abs], 20)
            self.assertEqual(result['abs'], 20)
            abs=db.tt.aa.abs()/10+5
            exp=abs.min()*2+1
            result = db(db.tt.aa == -20).select(exp).first()
            self.assertEqual(result[exp], 15)

            # test case()
            condition = db.tt.aa > 2
            case = condition.case(db.tt.aa + 2, db.tt.aa - 2)
            my_case = case.with_alias('my_case')
            result = db().select(my_case)
            self.assertEqual(len(result), 3)
            self.assertEqual(result[0][my_case], -1)
            self.assertEqual(result[0]['my_case'], -1)
            self.assertEqual(result[1]['my_case'], -22)
            self.assertEqual(result[2]['my_case'], 5)

            # test expression based delete
            self.assertEqual(db(db.tt.aa + 1 >= 4).count(), 1)
            self.assertEqual(db(db.tt.aa + 1 >= 4).delete(), 1)
            self.assertEqual(db(db.tt.aa).count(), 2)

            # cleanup
            drop(db.tt)
            db.close()

    def testUpdate(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])

        # some db's only support seconds
        datetime_datetime_today = datetime.datetime.today()
        datetime_datetime_today = datetime_datetime_today.replace(
            microsecond = 0)
        one_day = datetime.timedelta(1)
        one_sec = datetime.timedelta(0,1)

        update_vals = (
            ('string',   'x',  'y'),
            ('text',     'x',  'y'),
            ('password', 'x',  'y'),
            ('integer',   1,    2),
            ('bigint',    1,    2),
            ('float',     1.0,  2.0),
            ('double',    1.0,  2.0),
            ('boolean',   True, False),
            ('date', datetime.date.today(), datetime.date.today() + one_day),
            ('datetime', datetime.datetime(1971, 12, 21, 10, 30, 55, 0),
                datetime_datetime_today),
            ('time', datetime_datetime_today.time(),
                (datetime_datetime_today + one_sec).time()),
            )

        for uv in update_vals:
            db.define_table('tt', Field('aa', 'integer', default=0), 
                            Field('bb', uv[0]))
            self.assertTrue(isinstance(db.tt.insert(bb=uv[1]), long))
            self.assertEqual(db(db.tt.aa + 1 == 1).select(db.tt.bb)[0].bb, uv[1])
            self.assertEqual(db(db.tt.aa + 1 == 1).update(bb=uv[2]), 1)
            self.assertEqual(db(db.tt.aa / 3 == 0).select(db.tt.bb)[0].bb, uv[2])
            db.tt.drop()
        db.close()

    def testSubstring(self):
        if IS_MONGODB:
            # MongoDB does not support string length
            end = 3
        else:
            end = -2
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        t0 = db.define_table('t0', Field('name'))
        input_name = "web2py"
        t0.insert(name=input_name)
        exp_slice = t0.name.lower()[4:6]
        exp_slice_no_max = t0.name.lower()[4:]
        exp_slice_neg_max = t0.name.lower()[2:end]
        exp_slice_neg_start = t0.name.lower()[end:]
        exp_item = t0.name.lower()[3]
        out = db(t0).select(exp_slice, exp_item, exp_slice_no_max,
                            exp_slice_neg_max, exp_slice_neg_start).first()
        self.assertEqual(out[exp_slice], input_name[4:6])
        self.assertEqual(out[exp_item], input_name[3])
        self.assertEqual(out[exp_slice_no_max], input_name[4:])
        self.assertEqual(out[exp_slice_neg_max], input_name[2:end])
        self.assertEqual(out[exp_slice_neg_start], input_name[end:])
        t0.drop()
        db.close()

    def testOps(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        t0 = db.define_table('t0', Field('vv', 'integer'))
        self.assertTrue(isinstance(db.t0.insert(vv=1), long))
        self.assertTrue(isinstance(db.t0.insert(vv=2), long))
        self.assertTrue(isinstance(db.t0.insert(vv=3), long))
        sum = db.t0.vv.sum()
        count = db.t0.vv.count()
        avg=db.t0.vv.avg()
        op = sum/count
        op1 = (sum/count).with_alias('tot')
        self.assertEqual(db(t0).select(op).first()[op], 2)
        self.assertEqual(db(t0).select(op1).first()[op1], 2)
        self.assertEqual(db(t0).select(op1).first()['tot'], 2)
        op2 = avg*count
        self.assertEqual(db(t0).select(op2).first()[op2], 6)
        # the following is not possible at least on sqlite
        sum = db.t0.vv.sum().with_alias('s')
        count = db.t0.vv.count().with_alias('c')
        op = sum/count
        with self.assertRaises(SyntaxError):
            self.assertEqual(db(t0).select(op).first()[op], 2)
        t0.drop()
        db.close()


@unittest.skip("JOIN queries are not supported")
class TestJoin(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('t1', Field('aa'))
        db.define_table('t2', Field('aa'), Field('b', db.t1))
        i1 = db.t1.insert(aa='1')
        i2 = db.t1.insert(aa='2')
        i3 = db.t1.insert(aa='3')
        db.t2.insert(aa='4', b=i1)
        db.t2.insert(aa='5', b=i2)
        db.t2.insert(aa='6', b=i2)
        self.assertEqual(len(db(db.t1.id
                          == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)), 3)
        self.assertEqual(db(db.t1.id == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)[2].t1.aa, '2')
        self.assertEqual(db(db.t1.id == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)[2].t2.aa, '6')
        self.assertEqual(len(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)), 4)
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[2].t1.aa, '2')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[2].t2.aa, '6')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[3].t1.aa, '3')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[3].t2.aa, None)
        self.assertEqual(len(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa, groupby=db.t1.aa)),
                         3)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[0]._extra[db.t2.id.count()],
                         1)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[1]._extra[db.t2.id.count()],
                         2)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[2]._extra[db.t2.id.count()],
                         0)
        drop(db.t2)
        drop(db.t1)

        db.define_table('person',Field('name'))
        id = db.person.insert(name="max")
        self.assertEqual(id.name,'max')
        db.define_table('dog',Field('name'),Field('ownerperson','reference person'))
        db.dog.insert(name='skipper',ownerperson=1)
        row = db(db.person.id==db.dog.ownerperson).select().first()
        self.assertEqual(row[db.person.name],'max')
        self.assertEqual(row['person.name'],'max')
        drop(db.dog)
        self.assertEqual(len(db.person._referenced_by),0)
        drop(db.person)
        db.close()


@unittest.skipIf(IS_GAE or IS_IMAP, 'TODO: Datastore throws "AttributeError: Row object has no attribute _extra"')
class TestMinMaxSumAvg(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', 'integer'))
        self.assertEqual(isinstance(db.tt.insert(aa=1), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=2), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa=3), long), True)
        s = db.tt.aa.min()
        self.assertEqual(db(db.tt.id > 0).select(s)[0]._extra[s], 1)
        self.assertEqual(db(db.tt.id > 0).select(s).first()[s], 1)
        self.assertEqual(db().select(s).first()[s], 1)
        s = db.tt.aa.max()
        self.assertEqual(db().select(s).first()[s], 3)
        s = db.tt.aa.sum()
        self.assertEqual(db().select(s).first()[s], 6)
        s = db.tt.aa.count()
        self.assertEqual(db().select(s).first()[s], 3)
        s = db.tt.aa.avg()
        self.assertEqual(db().select(s).first()[s], 2)
        drop(db.tt)
        db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestMigrations(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), migrate='.storage.table')
        db.commit()
        db.close()
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), Field('b'),
                        migrate='.storage.table')
        db.commit()
        db.close()
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), Field('b', 'text'),
                        migrate='.storage.table')
        db.commit()
        db.close()
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), migrate='.storage.table')
        drop(db.tt)
        db.commit()
        db.close()

    def tearDown(self):
        if os.path.exists('.storage.db'):
            os.unlink('.storage.db')
        if os.path.exists('.storage.table'):
            os.unlink('.storage.table')


@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestReference(unittest.TestCase):
    def testRun(self):
        scenarios = (
            (True,  'CASCADE'),
            (False, 'CASCADE'),
            (False, 'SET NULL'),
        )
        for (b, ondelete) in scenarios:
            db = DAL(DEFAULT_URI, check_reserved=['all'], bigint_id=b)
            db.define_table('tt', Field('name'),
                            Field('aa','reference tt',ondelete=ondelete))
            db.commit()
            x = db.tt.insert(name='xxx')
            self.assertTrue(isinstance(x, long))
            self.assertEqual(x.id, x)
            self.assertEqual(x['id'], x)
            x.aa = x
            x.update_record()
            x1 = db.tt[x]
            self.assertEqual(x1.aa, x)
            self.assertEqual(x1.aa.aa.aa.aa.aa.aa.name, 'xxx')
            y=db.tt.insert(name='yyy', aa = x1)
            self.assertEqual(y.aa, x1.id)
            self.assertTrue(isinstance(db.tt.insert(name='zzz'), long))
            self.assertEqual(db(db.tt.name).count(), 3)
            if IS_MONGODB:
                db(db.tt.id == x).delete()
                expected_count = {
                    'SET NULL': 2,
                    'CASCADE': 1,
                }
                self.assertEqual(db(db.tt.name).count(), expected_count[ondelete])
                if ondelete == 'SET NULL':
                    self.assertEqual(db(db.tt.name == 'yyy').select()[0].aa, 0)
            drop(db.tt)
            db.commit()
            db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP")
class TestClientLevelOps(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'))
        db.commit()
        db.tt.insert(aa="test")
        rows1 = db(db.tt.aa=='test').select()
        rows2 = db(db.tt.aa=='test').select()
        rows3 = rows1 & rows2
        assert len(rows3) == 2
        rows4 = rows1 | rows2
        assert len(rows4) == 1
        rows5 = rows1.find(lambda row: row.aa=="test")
        assert len(rows5) == 1
        rows6 = rows2.exclude(lambda row: row.aa=="test")
        assert len(rows6) == 1
        rows7 = rows5.sort(lambda row: row.aa)
        assert len(rows7) == 1
        drop(db.tt)
        db.commit()
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestVirtualFields(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'))
        db.commit()
        db.tt.insert(aa="test")
        class Compute:
            def a_upper(row): return row.tt.aa.upper()
        db.tt.virtualfields.append(Compute())
        assert db(db.tt.id>0).select().first().a_upper == 'TEST'
        drop(db.tt)
        db.commit()
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestComputedFields(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt',
                        Field('aa'),
                        Field('bb',default='x'),
                        Field('cc',compute=lambda r: r.aa+r.bb))
        db.commit()
        id = db.tt.insert(aa="z")
        self.assertEqual(db.tt[id].cc,'zx')
        drop(db.tt)
        db.commit()

        # test checking that a compute field can refer to earlier-defined computed fields
        db.define_table('tt',
                        Field('aa'),
                        Field('bb',default='x'),
                        Field('cc',compute=lambda r: r.aa+r.bb),
                        Field('dd',compute=lambda r: r.bb + r.cc))
        db.commit()
        id = db.tt.insert(aa="z")
        self.assertEqual(db.tt[id].dd,'xzx')
        drop(db.tt)
        db.commit()
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestCommonFilters(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('t1', Field('aa', 'integer'))
        db.define_table('t2', Field('aa', 'integer'), Field('b', db.t1))
        i1 = db.t1.insert(aa=1)
        i2 = db.t1.insert(aa=2)
        i3 = db.t1.insert(aa=3)
        db.t2.insert(aa=4, b=i1)
        db.t2.insert(aa=5, b=i2)
        db.t2.insert(aa=6, b=i2)
        db.t1._common_filter = lambda q: db.t1.aa>1
        self.assertEqual(db(db.t1).count(),2)
        self.assertEqual(db(db.t1).count(),2)
        db.t2._common_filter = lambda q: db.t2.aa<6
        # test delete
        self.assertEqual(db(db.t2).count(),2)
        db(db.t2).delete()
        self.assertEqual(db(db.t2).count(),0)
        db.t2._common_filter = None
        self.assertEqual(db(db.t2).count(),1)
        # test update
        db.t2.insert(aa=4, b=i1)
        db.t2.insert(aa=5, b=i2)
        db.t2._common_filter = lambda q: db.t2.aa<6
        self.assertEqual(db(db.t2).count(),2)
        db(db.t2).update(aa=6)
        self.assertEqual(db(db.t2).count(),0)
        db.t2._common_filter = None
        self.assertEqual(db(db.t2).count(),3)
        drop(db.t2)
        drop(db.t1)
        db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP test")
class TestImportExportFields(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('person', Field('name'))
        db.define_table('pet',Field('friend',db.person),Field('name'))
        for n in range(2):
            db(db.pet).delete()
            db(db.person).delete()
            for k in range(10):
                id = db.person.insert(name=str(k))
                db.pet.insert(friend=id,name=str(k))
        db.commit()
        stream = StringIO()
        db.export_to_csv_file(stream)
        db(db.pet).delete()
        db(db.person).delete()
        stream = StringIO(stream.getvalue())
        db.import_from_csv_file(stream)
        assert db(db.person).count()==10
        assert db(db.pet.name).count()==10
        drop(db.pet)
        drop(db.person)
        db.commit()
        db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP test")
class TestImportExportUuidFields(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('person', Field('name'),Field('uuid'))
        db.define_table('pet',Field('friend',db.person),Field('name'))
        for n in range(2):
            db(db.pet).delete()
            db(db.person).delete()
            for k in range(10):
                id = db.person.insert(name=str(k),uuid=str(k))
                db.pet.insert(friend=id,name=str(k))
        db.commit()
        stream = StringIO()
        db.export_to_csv_file(stream)
        db(db.person).delete()
        db(db.pet).delete()
        stream = StringIO(stream.getvalue())
        db.import_from_csv_file(stream)
        assert db(db.person).count()==10
        assert db(db.pet).count()==10
        drop(db.pet)
        drop(db.person)
        db.commit()
        db.close()


@unittest.skipIf(IS_IMAP, "Skip IMAP test")
class TestDALDictImportExport(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('person', Field('name', default="Michael"),Field('uuid'))
        db.define_table('pet',Field('friend',db.person),Field('name'))
        dbdict = db.as_dict(flat=True, sanitize=False)
        assert isinstance(dbdict, dict)
        uri = dbdict["uri"]
        assert isinstance(uri, basestring) and uri
        assert len(dbdict["tables"]) == 2
        assert len(dbdict["tables"][0]["fields"]) == 3
        assert dbdict["tables"][0]["fields"][1]["type"] == db.person.name.type
        assert dbdict["tables"][0]["fields"][1]["default"] == db.person.name.default

        db2 = DAL(**dbdict)
        assert len(db.tables) == len(db2.tables)
        assert hasattr(db2, "pet") and isinstance(db2.pet, Table)
        assert hasattr(db2.pet, "friend") and isinstance(db2.pet.friend, Field)
        drop(db.pet)
        db.commit()

        db2.commit()

        have_serializers = True
        try:
            import serializers
            dbjson = db.as_json(sanitize=False)
            assert isinstance(dbjson, basestring) and len(dbjson) > 0

            unicode_keys = True
            if sys.version < "2.6.5":
                unicode_keys = False
            db3 = DAL(**serializers.loads_json(dbjson,
                          unicode_keys=unicode_keys))
            assert hasattr(db3, "person") and hasattr(db3.person, "uuid") and\
            db3.person.uuid.type == db.person.uuid.type
            drop(db3.person)
            db3.commit()
            db3.close()
        except ImportError:
            pass

        mpfc = "Monty Python's Flying Circus"
        dbdict4 = {"uri": DEFAULT_URI,
                   "tables":[{"tablename": "tvshow",
                              "fields": [{"fieldname": "name",
                                          "default":mpfc},
                                         {"fieldname": "rating",
                                          "type":"double"}]},
                             {"tablename": "staff",
                              "fields": [{"fieldname": "name",
                                          "default":"Michael"},
                                         {"fieldname": "food",
                                          "default":"Spam"},
                                         {"fieldname": "tvshow",
                                          "type": "reference tvshow"}]}]}
        db4 = DAL(**dbdict4)
        assert "staff" in db4.tables
        assert "name" in db4.staff
        assert db4.tvshow.rating.type == "double"
        assert (isinstance(db4.tvshow.insert(), long), isinstance(db4.tvshow.insert(name="Loriot"), long),
                isinstance(db4.tvshow.insert(name="Il Mattatore"), long)) == (True, True, True)
        assert isinstance(db4(db4.tvshow).select().first().id, long) == True
        assert db4(db4.tvshow).select().first().name == mpfc

        drop(db4.staff)
        drop(db4.tvshow)
        db4.commit()

        dbdict5 = {"uri": DEFAULT_URI}
        db5 = DAL(**dbdict5)
        assert db5.tables in ([], None)
        assert not (str(db5) in ("", None))

        dbdict6 = {"uri": DEFAULT_URI,
                   "tables":[{"tablename": "staff"},
                             {"tablename": "tvshow",
                              "fields": [{"fieldname": "name"},
                                         {"fieldname": "rating", "type":"double"}
                                        ]
                             }]
                  }
        db6 = DAL(**dbdict6)

        assert len(db6["staff"].fields) == 1
        assert "name" in db6["tvshow"].fields

        assert db6.staff.insert() is not None
        assert isinstance(db6(db6.staff).select().first().id, long) == True


        drop(db6.staff)
        drop(db6.tvshow)
        db6.commit()
        db.close()
        db2.close()
        db4.close()
        db5.close()
        db6.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestSelectAsDict(unittest.TestCase):

    def testSelect(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table(
            'a_table',
            Field('b_field'),
            Field('a_field'),
            )
        db.a_table.insert(a_field="aa1", b_field="bb1")
        rtn = db(db.a_table).select(db.a_table.id, db.a_table.b_field, db.a_table.a_field).as_list()
        self.assertEqual(rtn[0]['b_field'], 'bb1')
        keys = rtn[0].keys()
        self.assertEqual(len(keys), 3)
        self.assertEqual(("id" in keys, "b_field" in keys, "a_field" in keys), (True, True, True))
        drop(db.a_table)
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestRNameTable(unittest.TestCase):
    #tests for highly experimental rname attribute
    def testSelect(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        rname = db._adapter.QUOTE_TEMPLATE % 'a very complicated tablename'
        db.define_table(
            'easy_name',
            Field('a_field'),
            rname=rname
            )
        rtn = db.easy_name.insert(a_field='a')
        self.assertEqual(isinstance(rtn.id, long), True)
        rtn = db(db.easy_name.a_field == 'a').select()
        self.assertEqual(len(rtn), 1)
        self.assertEqual(isinstance(rtn[0].id, long), True)
        self.assertEqual(rtn[0].a_field, 'a')
        db.easy_name.insert(a_field='b')
        self.assertEqual(db(db.easy_name).count(), 2)
        rtn = db(db.easy_name.a_field == 'a').update(a_field='c')
        self.assertEqual(rtn, 1)

        #clean up
        drop(db.easy_name)
        db.close()


    @unittest.skip("JOIN queries are not supported")
    def testJoin(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        rname = db._adapter.QUOTE_TEMPLATE % 'this is table t1'
        rname2 = db._adapter.QUOTE_TEMPLATE % 'this is table t2'
        db.define_table('t1', Field('aa'), rname=rname)
        db.define_table('t2', Field('aa'), Field('b', db.t1), rname=rname2)
        i1 = db.t1.insert(aa='1')
        i2 = db.t1.insert(aa='2')
        i3 = db.t1.insert(aa='3')
        db.t2.insert(aa='4', b=i1)
        db.t2.insert(aa='5', b=i2)
        db.t2.insert(aa='6', b=i2)
        self.assertEqual(len(db(db.t1.id
                          == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)), 3)
        self.assertEqual(db(db.t1.id == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)[2].t1.aa, '2')
        self.assertEqual(db(db.t1.id == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)[2].t2.aa, '6')
        self.assertEqual(len(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)), 4)
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[2].t1.aa, '2')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[2].t2.aa, '6')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[3].t1.aa, '3')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[3].t2.aa, None)
        self.assertEqual(len(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa, groupby=db.t1.aa)),
                         3)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[0]._extra[db.t2.id.count()],
                         1)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[1]._extra[db.t2.id.count()],
                         2)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[2]._extra[db.t2.id.count()],
                         0)
        drop(db.t2)
        drop(db.t1)

        db.define_table('person',Field('name'), rname=rname)
        id = db.person.insert(name="max")
        self.assertEqual(id.name,'max')
        db.define_table('dog',Field('name'),Field('ownerperson','reference person'), rname=rname2)
        db.dog.insert(name='skipper',ownerperson=1)
        row = db(db.person.id==db.dog.ownerperson).select().first()
        self.assertEqual(row[db.person.name],'max')
        self.assertEqual(row['person.name'],'max')
        drop(db.dog)
        self.assertEqual(len(db.person._referenced_by),0)
        drop(db.person)
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
@unittest.skipIf(IS_GAE, 'TODO: Datastore AGGREGATE Not Supported')
class TestRNameFields(unittest.TestCase):
    # tests for highly experimental rname attribute
    def testSelect(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        rname = db._adapter.__class__.QUOTE_TEMPLATE % 'a very complicated fieldname'
        rname2 = db._adapter.__class__.QUOTE_TEMPLATE % 'rating from 1 to 10'
        db.define_table(
            'easy_name',
            Field('a_field', rname=rname),
            Field('rating', 'integer', rname=rname2, default=2)
            )
        rtn = db.easy_name.insert(a_field='a')
        self.assertEqual(isinstance(rtn.id, long), True)
        rtn = db(db.easy_name.a_field == 'a').select()
        self.assertEqual(len(rtn), 1)
        self.assertEqual(isinstance(rtn[0].id, long), True)
        self.assertEqual(rtn[0].a_field, 'a')
        db.easy_name.insert(a_field='b')
        rtn = db(db.easy_name.id > 0).delete()
        self.assertEqual(rtn, 2)
        rtn = db(db.easy_name.id > 0).count()
        self.assertEqual(rtn, 0)
        db.easy_name.insert(a_field='a')
        db.easy_name.insert(a_field='b')
        rtn = db(db.easy_name.id > 0).count()
        self.assertEqual(rtn, 2)
        rtn = db(db.easy_name.a_field == 'a').update(a_field='c')
        rtn = db(db.easy_name.a_field == 'c').count()
        self.assertEqual(rtn, 1)
        rtn = db(db.easy_name.a_field != 'c').count()
        self.assertEqual(rtn, 1)
        avg = db.easy_name.rating.avg()
        rtn = db(db.easy_name.id > 0).select(avg)
        self.assertEqual(rtn[0][avg], 2)

        rname = db._adapter.__class__.QUOTE_TEMPLATE % 'this is the person name'
        db.define_table(
            'person',
            Field('name', default="Michael", rname=rname),
            Field('uuid')
            )
        michael = db.person.insert() #default insert
        john = db.person.insert(name='John')
        luke = db.person.insert(name='Luke')

        rtn = db(db.person.id > 0).select()
        self.assertEqual(len(rtn), 3)
        self.assertEqual(rtn[0].id, michael)
        self.assertEqual(rtn[0].name, 'Michael')
        self.assertEqual(rtn[1].id, john)
        self.assertEqual(rtn[1].name, 'John')
        #fetch owners, eventually with pet
        #main point is retrieving Luke with no pets
        rtn = db(db.person.id > 0).select()
        self.assertEqual(rtn[0].id, michael)
        self.assertEqual(rtn[0].name, 'Michael')
        self.assertEqual(rtn[2].name, 'Luke')
        self.assertEqual(rtn[2].id, luke)
        #as dict
        rtn = db(db.person.id > 0).select().as_dict()
        self.assertEqual(rtn[michael]['name'], 'Michael')
        #as list
        rtn = db(db.person.id > 0).select().as_list()
        self.assertEqual(rtn[0]['name'], 'Michael')
        #isempty
        rtn = db(db.person.id > 0).isempty()
        self.assertEqual(rtn, False)

        #clean up
        drop(db.person)
        drop(db.easy_name)
        db.close()

    @unittest.skipIf(IS_GAE, 'TODO: Datastore does not accept dict objects as json field input.')
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        rname = db._adapter.QUOTE_TEMPLATE % 'a very complicated fieldname'
        for ft in ['string', 'text', 'password', 'upload', 'blob']:
            db.define_table('tt', Field('aa', ft, default='', rname=rname))
            self.assertEqual(isinstance(db.tt.insert(aa='x'), long), True)
            self.assertEqual(db().select(db.tt.aa)[0].aa, 'x')
            drop(db.tt)
        db.define_table('tt', Field('aa', 'integer', default=1, rname=rname))
        self.assertEqual(isinstance(db.tt.insert(aa=3), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, 3)
        drop(db.tt)
        db.define_table('tt', Field('aa', 'double', default=1, rname=rname))
        self.assertEqual(isinstance(db.tt.insert(aa=3.1), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, 3.1)
        drop(db.tt)
        db.define_table('tt', Field('aa', 'boolean', default=True, rname=rname))
        self.assertEqual(isinstance(db.tt.insert(aa=True), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, True)
        drop(db.tt)
        db.define_table('tt', Field('aa', 'json', default={}, rname=rname))
        self.assertEqual(isinstance(db.tt.insert(aa={}), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, {})
        drop(db.tt)
        db.define_table('tt', Field('aa', 'date',
                        default=datetime.date.today(), rname=rname))
        t0 = datetime.date.today()
        self.assertEqual(isinstance(db.tt.insert(aa=t0), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        drop(db.tt)
        db.define_table('tt', Field('aa', 'datetime',
                        default=datetime.datetime.today(), rname=rname))
        t0 = datetime.datetime(
            1971,
            12,
            21,
            10,
            30,
            55,
            0,
            )
        id = db.tt.insert(aa=t0)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)

        ## Row APIs
        row = db().select(db.tt.aa)[0]
        self.assertEqual(db.tt[id].aa,t0)
        self.assertEqual(db.tt['aa'],db.tt.aa)
        self.assertEqual(db.tt(id).aa,t0)
        self.assertTrue(db.tt(id,aa=None)==None)
        self.assertFalse(db.tt(id,aa=t0)==None)
        self.assertEqual(row.aa,t0)
        self.assertEqual(row['aa'],t0)
        self.assertEqual(row['tt.aa'],t0)
        self.assertEqual(row('tt.aa'),t0)

        ## Lazy and Virtual fields
        db.tt.b = Field.Virtual(lambda row: row.tt.aa)
        db.tt.c = Field.Lazy(lambda row: row.tt.aa)
        row = db().select(db.tt.aa)[0]
        self.assertEqual(row.b,t0)
        self.assertEqual(row.c(),t0)

        drop(db.tt)
        db.define_table('tt', Field('aa', 'time', default='11:30', rname=rname))
        t0 = datetime.time(10, 30, 55)
        self.assertEqual(isinstance(db.tt.insert(aa=t0), long), True)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        drop(db.tt)
        db.close()

    def testInsert(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        rname = db._adapter.QUOTE_TEMPLATE % 'a very complicated fieldname'
        db.define_table('tt', Field('aa', rname=rname))
        self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
        self.assertEqual(isinstance(db.tt.insert(aa='1'), long), True)
        self.assertEqual(db(db.tt.aa == '1').count(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), True)
        self.assertEqual(db(db.tt.aa == '1').update(aa='2'), 3)
        self.assertEqual(db(db.tt.aa == '2').count(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), False)
        self.assertEqual(db(db.tt.aa == '2').delete(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), True)
        drop(db.tt)
        db.close()


    @unittest.skip("JOIN queries are not supported")
    def testJoin(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        rname = db._adapter.QUOTE_TEMPLATE % 'this is field aa'
        rname2 = db._adapter.QUOTE_TEMPLATE % 'this is field b'
        db.define_table('t1', Field('aa', rname=rname))
        db.define_table('t2', Field('aa', rname=rname), Field('b', db.t1, rname=rname2))
        i1 = db.t1.insert(aa='1')
        i2 = db.t1.insert(aa='2')
        i3 = db.t1.insert(aa='3')
        db.t2.insert(aa='4', b=i1)
        db.t2.insert(aa='5', b=i2)
        db.t2.insert(aa='6', b=i2)
        self.assertEqual(len(db(db.t1.id
                          == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)), 3)
        self.assertEqual(db(db.t1.id == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)[2].t1.aa, '2')
        self.assertEqual(db(db.t1.id == db.t2.b).select(orderby=db.t1.aa
                          | db.t2.aa)[2].t2.aa, '6')
        self.assertEqual(len(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)), 4)
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[2].t1.aa, '2')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[2].t2.aa, '6')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[3].t1.aa, '3')
        self.assertEqual(db().select(db.t1.ALL, db.t2.ALL,
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa | db.t2.aa)[3].t2.aa, None)
        self.assertEqual(len(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa, groupby=db.t1.aa)),
                         3)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[0]._extra[db.t2.id.count()],
                         1)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[1]._extra[db.t2.id.count()],
                         2)
        self.assertEqual(db().select(db.t1.aa, db.t2.id.count(),
                         left=db.t2.on(db.t1.id == db.t2.b),
                         orderby=db.t1.aa,
                         groupby=db.t1.aa)[2]._extra[db.t2.id.count()],
                         0)
        drop(db.t2)
        drop(db.t1)

        db.define_table('person',Field('name', rname=rname))
        id = db.person.insert(name="max")
        self.assertEqual(id.name,'max')
        db.define_table('dog',Field('name', rname=rname),Field('ownerperson','reference person', rname=rname2))
        db.dog.insert(name='skipper',ownerperson=1)
        row = db(db.person.id==db.dog.ownerperson).select().first()
        self.assertEqual(row[db.person.name],'max')
        self.assertEqual(row['person.name'],'max')
        drop(db.dog)
        self.assertEqual(len(db.person._referenced_by),0)
        drop(db.person)
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestQuoting(unittest.TestCase):

    # tests for case sensitivity
    def testCase(self):
        return
        db = DAL(DEFAULT_URI, check_reserved=['all'], ignore_field_case=False)

        # test table case
        t0 = db.define_table('B',
                        Field('f', 'string'))

        t1 = db.define_table('b',
                             Field('B', t0),
                             Field('words', 'text'))

        blather = 'blah blah and so'
        t0[0] = {'f': 'content'}
        t1[0] = {'B': int(t0[1]['id']),
                 'words': blather}

        r = db(db.B.id==db.b.B).select()

        self.assertEqual(r[0].b.words, blather)

        drop(t1)
        drop(t0)

        # test field case
        t0 = db.define_table('table is a test',
                             Field('a_a'),
                             Field('a_A'))

        t0[0] = dict(a_a='a_a', a_A='a_A')

        self.assertEqual(t0[1].a_a, 'a_a')
        self.assertEqual(t0[1].a_A, 'a_A')

        drop(t0)
        db.close()

    def testPKFK(self):

        # test primary keys

        db = DAL(DEFAULT_URI, check_reserved=['all'], ignore_field_case=False)
        # test table without surrogate key. Length must is limited to
        # 100 because of MySQL limitations: it cannot handle more than
        # 767 bytes in unique keys.

        t0 = db.define_table('t0', Field('Code', length=100), primarykey=['Code'])
        t2 = db.define_table('t2', Field('f'), Field('t0_Code', 'reference t0'))
        t3 = db.define_table('t3', Field('f', length=100), Field('t0_Code', t0.Code), primarykey=['f'])
        t4 = db.define_table('t4', Field('f', length=100), Field('t0', t0), primarykey=['f'])

        try:
            t5 = db.define_table('t5', Field('f', length=100), Field('t0', 'reference no_table_wrong_reference'), primarykey=['f'])
        except Exception as e:
            self.assertTrue(isinstance(e, KeyError))

        drop(t0, 'cascade')
        drop(t2)
        drop(t3)
        drop(t4)
        db.close()


class TestTableAndFieldCase(unittest.TestCase):
    """
    at the Python level we should not allow db.C and db.c because of .table conflicts on windows
    but it should be possible to map two different names into distinct tables "c" and "C" at the Python level
    By default Python models names should be mapped into lower case table names and assume case insensitivity.
    """
    def testme(self):
        return


class TestQuotesByDefault(unittest.TestCase):
    """
    all default tables names should be quoted unless an explicit mapping has been given for a table.
    """
    def testme(self):
        return


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestRecordVersioning(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        tt = db.define_table('tt', Field('name'),
                        Field('is_active', 'boolean', default=True))
        db.tt._enable_record_versioning(archive_name='tt_archive')
        self.assertTrue('tt_archive' in db)
        i_id = db.tt.insert(name='web2py1')
        db.tt.insert(name='web2py2')
        db(db.tt.name == 'web2py2').delete()
        self.assertEqual(len(db(db.tt).select()), 1)
        self.assertEqual(db(db.tt).count(), 1)
        db(db.tt.id == i_id).update(name='web2py3')
        self.assertEqual(len(db(db.tt).select()), 1)
        self.assertEqual(db(db.tt).count(), 1)
        self.assertEqual(len(db(db.tt_archive).select()), 2)
        self.assertEqual(db(db.tt_archive).count(), 2)
        drop(db.tt_archive)
        # it allows tt to be dropped
        db.tt._before_delete = []
        drop(tt)
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestConnection(unittest.TestCase):

    def testRun(self):
        # check for adapter reconnect without parameters
        db1 = DAL(DEFAULT_URI, check_reserved=['all'])
        db1.define_table('tt', Field('aa', 'integer'))
        self.assertEqual(isinstance(db1.tt.insert(aa=1), long), True)
        self.assertEqual(db1(db1.tt.aa == 1).count(), 1)
        drop(db1.tt)
        db1._adapter.close()
        db1._adapter.reconnect()
        db1.define_table('tt', Field('aa', 'integer'))
        self.assertEqual(isinstance(db1.tt.insert(aa=1), long), True)
        self.assertEqual(db1(db1.tt.aa == 1).count(), 1)
        drop(db1.tt)
        db1.close()

        # check connection are reused with pool_size
        connections = {}
        for a in range(10):
            db2 = DAL(DEFAULT_URI, check_reserved=['all'], pool_size=5)
            c = db2._adapter.connection
            connections[id(c)] = c
            db2.close()
        self.assertEqual(len(connections), 1)
        c = [connections[x] for x in connections][0]
        c.commit()
        c.close()

        # check correct use of pool_size
        dbs = []
        for a in range(10):
            db3 = DAL(DEFAULT_URI, check_reserved=['all'], pool_size=5)
            dbs.append(db3)
        for db in dbs:
            db.close()
        self.assertEqual(len(db3._adapter.POOLS[DEFAULT_URI]), 5)
        for c in db3._adapter.POOLS[DEFAULT_URI]:
            c.close()
        db3._adapter.POOLS[DEFAULT_URI] = []

@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestBasicOps(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        tt = db.define_table('tt', Field('name'),
                        Field('is_active', 'boolean', default=True))
        i_id = db.tt.insert(name='web2py1')
        db.tt.insert(name='web2py2')
        db(db.tt.name == 'web2py2').delete()
        self.assertEqual(len(db(db.tt).select()), 1)
        self.assertEqual(db(db.tt).count(), 1)
        db(db.tt.id == i_id).update(name='web2py3')
        self.assertEqual(len(db(db.tt).select()), 1)
        self.assertEqual(db(db.tt).count(), 1)
        drop(tt)
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
@unittest.skipIf(IS_GAE, 'TODO: Datastore "unsupported operand type"')
class TestSQLCustomType(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        from pydal.helpers.classes import SQLCustomType
        native_double = "double"
        native_string = "string"
        if hasattr(db._adapter, 'types'):
            native_double = db._adapter.types['double']
            try:
                native_string = db._adapter.types['string'] % {'length': 256}
            except:
                native_string = db._adapter.types['string']
        basic_t = SQLCustomType(type = "double", native = native_double)
        basic_t_str = SQLCustomType(type = "string", native = native_string)
        t0=db.define_table('t0', Field("price", basic_t), Field("product", basic_t_str))
        r_id = t0.insert(price=None, product=None)
        row = db(t0.id == r_id).select(t0.ALL).first()
        self.assertEqual(row['price'], None)
        self.assertEqual(row['product'], None)
        r_id = t0.insert(price=1.2, product="car")
        row=db(t0.id == r_id).select(t0.ALL).first()
        self.assertEqual(row['price'], 1.2)
        self.assertEqual(row['product'], 'car')
        t0.drop()
        db.close()


@unittest.skipIf(IS_GAE or IS_IMAP, "Skip test lazy")
class TestLazy(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'], lazy_tables=True)
        t0 = db.define_table('t0', Field('name'))
        self.assertTrue(('t0' in db._LAZY_TABLES.keys()))
        db.t0.insert(name='1')
        self.assertFalse(('t0' in db._LAZY_TABLES.keys()))
        db.t0.drop()
        db.close()

    def testLazyGetter(self):
        db=DAL(DEFAULT_URI, lazy_tables=True)
        db.define_table('tt',  Field('value', 'integer'))
        db.define_table('ttt',
            Field('value', 'integer'),
            Field('tt_id', 'reference tt'),
        )
        # Force table definition
        db.ttt.value.writable=False
        idd=db.tt.insert(value=0)
        db.ttt.insert(tt_id=idd)
        db.ttt.drop()
        db.tt.drop()
        db.close()

    def testRowNone(self):
        db=DAL(DEFAULT_URI, lazy_tables=True)
        tt = db.define_table('tt',  Field('value', 'integer'))
        db.tt.insert(value=None)
        row = db(db.tt).select(db.tt.ALL).first()
        self.assertEqual(row.value, None)
        self.assertEqual(row[db.tt.value], None)
        self.assertEqual(row['tt.value'], None)
        self.assertEqual(row.get('tt.value'), None)
        self.assertEqual(row['value'], None)
        self.assertEqual(row.get('value'), None)
        db.tt.drop()
        db.close()


class TestRedefine(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'], lazy_tables=True, migrate=False)
        db.define_table('t_a', Field('code'))
        self.assertTrue('code' in db.t_a)
        self.assertTrue('code' in db['t_a'])
        db.define_table('t_a', Field('code_a'), redefine=True)
        self.assertFalse('code' in db.t_a)
        self.assertFalse('code' in db['t_a'])
        self.assertTrue('code_a' in db.t_a)
        self.assertTrue('code_a' in db['t_a'])
        db.close()

@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestUpdateInsert(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        t0 = db.define_table('t0', Field('name'))
        i_id = db.t0.update_or_insert((db.t0.id == 1), name='web2py')
        u_id = db.t0.update_or_insert((db.t0.id == i_id), name='web2py2')
        self.assertTrue(i_id != None)
        self.assertTrue(u_id == None)
        self.assertEqual(len(db(db.t0).select()), 1)
        self.assertEqual(db(db.t0).count(), 1)
        self.assertEqual(db(db.t0.name == 'web2py').count(), 0)
        self.assertEqual(db(db.t0.name == 'web2py2').count(), 1)
        drop(t0)
        db.close()


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestBulkInsert(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        t0 = db.define_table('t0', Field('name'))
        global ctr
        ctr = 0
        def test_after_insert(i, r):
            self.assertIsInstance(i, dict)
            global ctr
            ctr += 1
            return True
        t0._after_insert.append(test_after_insert)
        items = [{'name':'web2py_%s' % pos} for pos in range(0, 10, 1)]
        t0.bulk_insert(items)
        self.assertTrue(db(t0).count() == len(items))
        for pos in range(0, 10, 1):
            self.assertEqual(len(db(t0.name == 'web2py_%s' % pos).select()), 1)
            self.assertEqual(db(t0.name == 'web2py_%s' % pos).count(), 1)
        self.assertTrue(ctr == len(items))
        drop(t0)
        db.close()


if __name__ == '__main__':
    unittest.main()
    tearDownModule()
