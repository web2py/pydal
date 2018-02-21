# -*- coding: utf-8 -*-
"""
    Basic unit tests
"""

from __future__ import print_function
import os
import glob
import datetime
import json
import pickle

from pydal._compat import basestring, StringIO, integer_types, xrange, BytesIO, to_bytes
from pydal import DAL, Field
from pydal.helpers.classes import SQLALL, OpRow
from pydal.objects import Table, Expression, Row
from ._compat import unittest
from ._adapt import (
    DEFAULT_URI, IS_POSTGRESQL, IS_SQLITE, IS_MSSQL, IS_MYSQL, IS_TERADATA)
from ._helpers import DALtest

long = integer_types[-1]

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
    'bigint'
    ]



def setUpModule():
    if IS_MYSQL or IS_TERADATA:
        db = DAL(DEFAULT_URI, check_reserved=['all'])

        def clean_table(db, tablename):
            try:
                db.define_table(tablename)
            except Exception as e:
                pass
            try:
                db[tablename].drop()
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


class TestFields(DALtest):

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

    def testUploadField(self):
        import tempfile

        stream = tempfile.NamedTemporaryFile()
        content = b"this is the stream content"
        stream.write(content)
        # rewind before inserting
        stream.seek(0)


        db = self.connect()
        db.define_table('tt', Field('fileobj', 'upload',
                                    uploadfolder=tempfile.gettempdir(),
                                    autodelete=True))
        f_id = db.tt.insert(fileobj=stream)

        row = db.tt[f_id]
        (retr_name, retr_stream) = db.tt.fileobj.retrieve(row.fileobj)

        # name should be the same
        self.assertEqual(retr_name, os.path.basename(stream.name))
        # content should be the same
        retr_content = retr_stream.read()
        self.assertEqual(retr_content, content)

        # close streams!
        retr_stream.close()

        # delete
        row.delete_record()

        # drop
        db.tt.drop()

        # this part is triggered only if fs (AKA pyfilesystem) module is installed
        try:
            from fs.memoryfs import MemoryFS

            # rewind before inserting
            stream.seek(0)
            db.define_table('tt', Field('fileobj', 'upload',
                                        uploadfs=MemoryFS(),
                                        autodelete=True))

            f_id = db.tt.insert(fileobj=stream)

            row = db.tt[f_id]
            (retr_name, retr_stream) = db.tt.fileobj.retrieve(row.fileobj)

            # name should be the same
            self.assertEqual(retr_name, os.path.basename(stream.name))
            # content should be the same
            retr_content = retr_stream.read()
            self.assertEqual(retr_content, content)

            # close streams
            retr_stream.close()
            stream.close()

            # delete
            row.delete_record()

            # drop
            db.tt.drop()

        except ImportError:
            pass

    def testBlobBytes(self):
        #Test blob with latin1 encoded bytes
        db = self.connect()
        obj = pickle.dumps('0')
        db.define_table('tt', Field('aa', 'blob'))
        self.assertEqual(db.tt.insert(aa=obj), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, obj)
        self.assertEqual(db.tt[1].aa, obj)
        self.assertEqual(BytesIO(to_bytes(db.tt[1].aa)).read(), obj)
        db.tt.drop()

    def testRun(self):
        """Test all field types and their return values"""
        db = self.connect()
        for ft in ['string', 'text', 'password', 'upload', 'blob']:
            db.define_table('tt', Field('aa', ft, default=''))
            self.assertEqual(db.tt.insert(aa='ö'), 1)
            self.assertEqual(db().select(db.tt.aa)[0].aa, 'ö')
            db.tt.drop()
        db.define_table('tt', Field('aa', 'integer', default=1))
        self.assertEqual(db.tt.insert(aa=3), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, 3)
        db.tt.drop()

        db.define_table('tt', Field('aa', 'string'))
        ucs = 'A\xc3\xa9 A'
        self.assertEqual(db.tt.insert(aa=ucs), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, ucs)
        self.assertEqual(db().select(db.tt.aa.with_alias('zz'))[0].zz, ucs)
        db.tt.drop()

        db.define_table('tt', Field('aa', 'double', default=1))
        self.assertEqual(db.tt.insert(aa=3.1), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, 3.1)
        db.tt.drop()
        db.define_table('tt', Field('aa', 'boolean', default=True))
        self.assertEqual(db.tt.insert(aa=True), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, True)
        db.tt.drop()
        db.define_table('tt', Field('aa', 'json', default={}))
        # test different python objects for correct serialization in json
        objs = [
            {'a' : 1, 'b' : 2},
            [1, 2, 3],
            'abc',
            True,
            False,
            None,
            11,
            14.3,
            long(11)
        ]
        for obj in objs:
            rtn_id = db.tt.insert(aa=obj)
            rtn = db(db.tt.id == rtn_id).select().first().aa
            self.assertEqual(obj, rtn)
        db.tt.drop()
        db.define_table('tt', Field('aa', 'date',
                        default=datetime.date.today()))
        t0 = datetime.date.today()
        self.assertEqual(db.tt.insert(aa=t0), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        db.tt.drop()
        db.define_table('tt', Field('aa', 'datetime',
                        default=datetime.datetime.today()))
        t0 = datetime.datetime(
            1971,
            12,
            21,
            10,
            30,
            55,
            0,
            )
        self.assertEqual(db.tt.insert(aa=t0), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)

        ## Row APIs
        row = db().select(db.tt.aa)[0]
        self.assertEqual(db.tt[1].aa,t0)
        self.assertEqual(db.tt['aa'],db.tt.aa)
        self.assertEqual(db.tt(1).aa,t0)
        self.assertTrue(db.tt(1,aa=None)==None)
        self.assertFalse(db.tt(1,aa=t0)==None)
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

        db.tt.drop()
        db.define_table('tt', Field('aa', 'time', default='11:30'))
        t0 = datetime.time(10, 30, 55)
        self.assertEqual(db.tt.insert(aa=t0), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        db.tt.drop()

        # aggregation type detection
        db.define_table('tt', Field('aa', 'datetime',
                        default=datetime.datetime.today()))
        t0 = datetime.datetime(1971, 12, 21, 10, 30, 55, 0)
        self.assertEqual(db.tt.insert(aa=t0), 1)
        self.assertEqual(db().select(db.tt.aa.min())[0][db.tt.aa.min()], t0)
        db.tt.drop()


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


class TestAll(unittest.TestCase):

    def setUp(self):
        self.pt = Table(None,'PseudoTable',Field('name'),Field('birthdate'))

    def testSQLALL(self):
        ans = 'PseudoTable.id, PseudoTable.name, PseudoTable.birthdate'
        self.assertEqual(str(SQLALL(self.pt)), ans)


class TestTable(DALtest):

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
        db = self.connect()
        persons = Table(db, 'persons', Field('firstname',
                           'string'), Field('lastname', 'string'))
        aliens = persons.with_alias('aliens')

        # Are the different table instances with the same fields

        self.assertTrue(persons is not aliens)
        self.assertTrue(set(persons.fields) == set(aliens.fields))

    def testTableInheritance(self):
        persons = Table(None, 'persons', Field('firstname',
                           'string'), Field('lastname', 'string'))
        customers = Table(None, 'customers',
                             Field('items_purchased', 'integer'),
                             persons)
        self.assertTrue(set(customers.fields).issuperset(set(
            ['items_purchased', 'firstname', 'lastname'])))


class TestInsert(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa'))
        self.assertEqual(db.tt.insert(aa='1'), 1)
        if not IS_TERADATA:
            self.assertEqual(db.tt.insert(aa='1'), 2)
            self.assertEqual(db.tt.insert(aa='1'), 3)
        else:
            self.assertEqual(db.tt.insert(aa='1'), 1)
            self.assertEqual(db.tt.insert(aa='1'), 1)

        self.assertEqual(db(db.tt.aa == '1').count(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), True)
        self.assertEqual(db(db.tt.aa == '1').update(aa='2'), 3)
        self.assertEqual(db(db.tt.aa == '2').count(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), False)
        self.assertEqual(db(db.tt.aa == '2').delete(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), True)


class TestSelect(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa'))
        self.assertEqual(db.tt.insert(aa='1'), 1)
        if not IS_TERADATA:
            self.assertEqual(db.tt.insert(aa='2'), 2)
            self.assertEqual(db.tt.insert(aa='3'), 3)
        else:
            self.assertEqual(db.tt.insert(aa='2'), 1)
            self.assertEqual(db.tt.insert(aa='3'), 1)
        self.assertEqual(db(db.tt.id > 0).count(), 3)
        self.assertEqual(db(db.tt.id > 0).select(orderby=~db.tt.aa
                          | db.tt.id)[0].aa, '3')
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
        self.assertEqual(db((db.tt.aa > '1') & ~(db.tt.aa > '2')).count(), 1)
        self.assertEqual(db(~(db.tt.aa > '1') & (db.tt.aa > '2')).count(), 0)
        # Test for REGEX_TABLE_DOT_FIELD
        self.assertEqual(db(db.tt).select('tt.aa').first()[db.tt.aa], '1')

    def testTestQuery(self):
        db = self.connect()
        db._adapter.test_connection()

    def testListInteger(self):
        db = self.connect()
        db.define_table('tt',
                        Field('aa', 'list:integer'))
        l=[1,2,3,4,5]
        db.tt.insert(aa=l)
        self.assertEqual(db(db.tt).select('tt.aa').first()[db.tt.aa],l)

    def testListString(self):
        db = self.connect()
        db.define_table('tt',
                        Field('aa', 'list:string'))
        l=['a', 'b', 'c']
        db.tt.insert(aa=l)
        self.assertEqual(db(db.tt).select('tt.aa').first()[db.tt.aa],l)

    def testListReference(self):
        db = self.connect()
        db.define_table('t0', Field('aa', 'string'))
        db.define_table('tt', Field('t0_id', 'list:reference t0'))
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

        self.assertEqual(db(db.tt.t0_id == ref3).count(), 1)

    def testGroupByAndDistinct(self):
        db = self.connect()
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
                             distinct=True, orderby=~db.tt.aa, limitby=(1,2))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].aa, '3')

        # test count distinct
        db.tt.insert(aa='2', bb=3, cc=1)
        self.assertEqual(db(db.tt).count(distinct=db.tt.aa), 4)
        self.assertEqual(db(db.tt.aa).count(db.tt.aa), 4)
        self.assertEqual(db(db.tt.aa).count(), 11)
        count=db.tt.aa.count()
        self.assertEqual(db(db.tt).select(count).first()[count], 11)

        count=db.tt.aa.count(distinct=True)
        sum=db.tt.bb.sum()
        result = db(db.tt).select(count, sum)
        self.assertEqual(tuple(result.response[0]), (4, 23))

    def testCoalesce(self):
        db = self.connect()
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

    def testTableAliasCollisions(self):
        db = self.connect()
        db.define_table('t1', Field('aa'))
        db.define_table('t2', Field('bb'))
        t1, t2 = db.t1, db.t2
        t1.with_alias('t2')
        t2.with_alias('t1')

        # Passing tables by name will result in exception
        t1.insert(aa='test')
        t2.insert(bb='foo')
        db(t1.id > 0).update(aa='bar')
        having = (t1.aa != None)
        join = [t2.on(t1.aa == t2.bb)]
        db(t1.aa == t2.bb).select(t1.aa, groupby=t1.aa, having=having,
            orderby=t1.aa)
        db(t1.aa).select(t1.aa, join=join, groupby=t1.aa, having=having,
            orderby=t1.aa)
        db(t1.aa).select(t1.aa, left=join, groupby=t1.aa, having=having,
            orderby=t1.aa)
        db(t1.id > 0).delete()


class TestSubselect(DALtest):

    def testMethods(self):
        db = self.connect()
        db.define_table('tt', Field('aa', 'integer'), Field('bb'))
        data = [
            dict(aa=1, bb='foo'), dict(aa=1, bb='bar'), dict(aa=2, bb='foo')
        ]
        for item in data:
            db.tt.insert(**item)
        fields = [db.tt.aa, db.tt.bb, db.tt.aa+2,
            (db.tt.aa+1).with_alias('exp')]
        sub = db(db.tt).nested_select(*fields, orderby=db.tt.id)
        # Check the fields provided by the object
        self.assertEqual(sorted(['aa', 'bb', 'exp']), sorted(list(sub.fields)))
        for name in sub.fields:
            self.assertIsInstance(sub[name], Field)
        for item in sub:
            self.assertIsInstance(item, Field)
        self.assertEqual(len(list(sub)), len(sub.fields))
        for key, val in zip(sub.fields, sub):
            self.assertIs(sub[key], val)
            self.assertIs(getattr(sub, key), val)
        tmp = sub._filter_fields(dict(aa=1, exp=2, foo=3))
        self.assertEqual(tmp, dict(aa=1, exp=2))
        # Check result from executing the query
        result = sub()
        self.assertEqual(len(result), len(data))
        for idx, row in enumerate(data):
            self.assertEqual(result[idx]['tt'].as_dict(), row)
            self.assertEqual(result[idx]['exp'], row['aa']+1)
        result = db.executesql(str(sub))
        for idx, row in enumerate(data):
            tmp = [row['aa'], row['bb'], row['aa']+2, row['aa']+1]
            self.assertEqual(list(result[idx]), tmp)
        # Check that query expansion methods don't work without alias
        self.assertEqual(sub._rname, None)
        self.assertEqual(sub._raw_rname, None)
        self.assertEqual(sub._dalname, None)
        with self.assertRaises(SyntaxError):
            sub.query_name()
        with self.assertRaises(SyntaxError):
            sub.sql_shortref
        with self.assertRaises(SyntaxError):
            sub.on(sub.aa != None)
        # Alias checks
        sub = sub.with_alias('foo')
        result = sub()
        for idx, row in enumerate(data):
            self.assertEqual(result[idx]['tt'].as_dict(), row)
            self.assertEqual(result[idx]['exp'], row['aa']+1)
        # Check query expansion methods again
        self.assertEqual(sub._rname, None)
        self.assertEqual(sub._raw_rname, None)
        self.assertEqual(sub._dalname, None)
        self.assertEqual(sub.query_name()[0], str(sub))
        self.assertEqual(sub.sql_shortref, db._adapter.dialect.quote('foo'))
        self.assertIsInstance(sub.on(sub.aa != None), Expression)

    def testSelectArguments(self):
        db = self.connect()
        db.define_table('tt', Field('aa', 'integer'), Field('bb'))
        data = [
            dict(aa=1, bb='foo'), dict(aa=1, bb='bar'), dict(aa=2, bb='foo'),
            dict(aa=3, bb='foo'), dict(aa=3, bb='baz')
        ]
        expected = [(1, None, 0), (2, 2, 2), (2, 2, 2), (3, 4, 3), (3, 8, 6)]
        for item in data:
            db.tt.insert(**item)

        # Check that select clauses work as expected in stand-alone query
        t1 = db.tt.with_alias('t1')
        t2 = db.tt.with_alias('t2')
        fields = [t1.aa, t2.aa.sum().with_alias('total'),
            t2.aa.count().with_alias('cnt')]
        join = t1.on(db.tt.bb != t1.bb)
        left = t2.on(t1.aa > t2.aa)
        group = db.tt.bb | t1.aa
        having = db.tt.aa.count() > 1
        order = t1.aa | t2.aa.count()
        limit = (1,6)
        sub = db(db.tt.aa != 2).nested_select(*fields, join=join, left=left,
            orderby=order, groupby=group, having=having, limitby=limit)
        result = sub()
        self.assertEqual(len(result), len(expected))
        for idx, val in enumerate(expected):
            self.assertEqual(result[idx]['t1']['aa'], val[0])
            self.assertEqual(result[idx]['total'], val[1])
            self.assertEqual(result[idx]['cnt'], val[2])

        # Check again when nested inside another query
        # Also check that the alias will not conflict with existing table
        t3 = db.tt.with_alias('t3')
        sub = sub.with_alias('tt')
        query = (t3.bb == 'foo') & (t3.aa == sub.aa)
        order = t3.aa | sub.cnt
        result = db(query).select(t3.aa, sub.total, sub.cnt, orderby=order)
        for idx, val in enumerate(expected):
            self.assertEqual(result[idx]['t3']['aa'], val[0])
            self.assertEqual(result[idx]['tt']['total'], val[1])
            self.assertEqual(result[idx]['tt']['cnt'], val[2])

        # Check "distinct" modifier separately
        sub = db(db.tt.aa != 2).nested_select(db.tt.aa, distinct=True)
        result = sub().as_list()
        self.assertEqual(result, [dict(aa=1), dict(aa=3)])

    def testCorrelated(self):
        db = self.connect()
        db.define_table('t1', Field('aa', 'integer'), Field('bb'),
            Field('mark', 'integer'))
        db.define_table('t2', Field('aa', 'integer'), Field('cc'))
        db.define_table('t3', Field('aa', 'integer'))
        data_t1 = [
            dict(aa=1, bb='bar'), dict(aa=1, bb='foo'), dict(aa=2, bb='foo'),
            dict(aa=2, bb='test'), dict(aa=3, bb='baz'), dict(aa=3, bb='foo')
        ]
        data_t2 = [
            dict(aa=1, cc='foo'), dict(aa=2, cc='bar'), dict(aa=3, cc='baz')
        ]
        expected_cor = [(1, 'foo'), (3, 'baz')]
        expected_leftcor = [(1, 'foo'), (2, None), (3, 'baz')]
        expected_uncor = [
            (1, 'bar'), (1, 'foo'), (2, 'foo'), (3, 'baz'), (3, 'foo')
        ]
        for item in data_t1:
            db.t1.insert(**item)
        for item in data_t2:
            db.t2.insert(**item)
            db.t3.insert(aa=item['aa'])

        # Correlated subqueries
        subquery = db.t1.aa == db.t2.aa
        subfields = [db.t2.cc]
        sub = db(subquery).nested_select(*subfields).with_alias('sub')
        query = db.t1.bb.belongs(sub)
        order = db.t1.aa | db.t1.bb
        result = db(query).select(db.t1.aa, db.t1.bb, orderby=order)
        self.assertEqual(len(result), len(expected_cor))
        for idx, val in enumerate(expected_cor):
            self.assertEqual(result[idx]['aa'], val[0])
            self.assertEqual(result[idx]['bb'], val[1])

        join = [db.t1.on((db.t3.aa == db.t1.aa) & db.t1.bb.belongs(sub))]
        order = db.t3.aa | db.t1.bb
        result = db(db.t3).select(db.t3.aa, db.t1.bb, join=join, orderby=order)
        self.assertEqual(len(result), len(expected_cor))
        for idx, val in enumerate(expected_cor):
            self.assertEqual(result[idx]['t3']['aa'], val[0])
            self.assertEqual(result[idx]['t1']['bb'], val[1])

        left = [db.t1.on((db.t3.aa == db.t1.aa) & db.t1.bb.belongs(sub))]
        result = db(db.t3).select(db.t3.aa, db.t1.bb, left=left, orderby=order)
        self.assertEqual(len(result), len(expected_leftcor))
        for idx, val in enumerate(expected_leftcor):
            self.assertEqual(result[idx]['t3']['aa'], val[0])
            self.assertEqual(result[idx]['t1']['bb'], val[1])

        order = db.t1.aa | db.t1.bb
        db(db.t1.bb.belongs(sub)).update(mark=1)
        result = db(db.t1.mark == 1).select(db.t1.aa, db.t1.bb, orderby=order)
        self.assertEqual(len(result), len(expected_cor))
        for idx, val in enumerate(expected_cor):
            self.assertEqual(result[idx]['aa'], val[0])
            self.assertEqual(result[idx]['bb'], val[1])

        db(~db.t1.bb.belongs(sub)).delete()
        result = db(db.t1.id > 0).select(db.t1.aa, db.t1.bb, orderby=order)
        self.assertEqual(len(result), len(expected_cor))
        for idx, val in enumerate(expected_cor):
            self.assertEqual(result[idx]['aa'], val[0])
            self.assertEqual(result[idx]['bb'], val[1])

        db(db.t1.id > 0).delete()
        for item in data_t1:
            db.t1.insert(**item)

        # Uncorrelated subqueries
        kwargs = dict(correlated=False)
        sub = db(subquery).nested_select(*subfields, **kwargs)
        query = db.t1.bb.belongs(sub)
        order = db.t1.aa | db.t1.bb
        result = db(query).select(db.t1.aa, db.t1.bb, orderby=order)
        self.assertEqual(len(result), len(expected_uncor))
        for idx, val in enumerate(expected_uncor):
            self.assertEqual(result[idx]['aa'], val[0])
            self.assertEqual(result[idx]['bb'], val[1])

        join = [db.t1.on((db.t3.aa == db.t1.aa) & db.t1.bb.belongs(sub))]
        order = db.t3.aa | db.t1.bb
        result = db(db.t3).select(db.t3.aa, db.t1.bb, join=join, orderby=order)
        self.assertEqual(len(result), len(expected_uncor))
        for idx, val in enumerate(expected_uncor):
            self.assertEqual(result[idx]['t3']['aa'], val[0])
            self.assertEqual(result[idx]['t1']['bb'], val[1])

        left = [db.t1.on((db.t3.aa == db.t1.aa) & db.t1.bb.belongs(sub))]
        result = db(db.t3).select(db.t3.aa, db.t1.bb, left=left, orderby=order)
        self.assertEqual(len(result), len(expected_uncor))
        for idx, val in enumerate(expected_uncor):
            self.assertEqual(result[idx]['t3']['aa'], val[0])
            self.assertEqual(result[idx]['t1']['bb'], val[1])
        # MySQL does not support subqueries with uncorrelated references
        # to target table

        # Correlation prevented by alias in parent select
        tmp = db.t1.with_alias('tmp')
        sub = db(subquery).nested_select(*subfields)
        query = tmp.bb.belongs(sub)
        order = tmp.aa | tmp.bb
        result = db(query).select(tmp.aa, tmp.bb, orderby=order)
        self.assertEqual(len(result), len(expected_uncor))
        for idx, val in enumerate(expected_uncor):
            self.assertEqual(result[idx]['aa'], val[0])
            self.assertEqual(result[idx]['bb'], val[1])

        join = [tmp.on((db.t3.aa == tmp.aa) & tmp.bb.belongs(sub))]
        order = db.t3.aa | tmp.bb
        result = db(db.t3).select(db.t3.aa, tmp.bb, join=join, orderby=order)
        self.assertEqual(len(result), len(expected_uncor))
        for idx, val in enumerate(expected_uncor):
            self.assertEqual(result[idx]['t3']['aa'], val[0])
            self.assertEqual(result[idx]['tmp']['bb'], val[1])

        left = [tmp.on((db.t3.aa == tmp.aa) & tmp.bb.belongs(sub))]
        result = db(db.t3).select(db.t3.aa, tmp.bb, left=left, orderby=order)
        self.assertEqual(len(result), len(expected_uncor))
        for idx, val in enumerate(expected_uncor):
            self.assertEqual(result[idx]['t3']['aa'], val[0])
            self.assertEqual(result[idx]['tmp']['bb'], val[1])
        # SQLite does not support aliasing target table in UPDATE/DELETE
        # MySQL does not support subqueries with uncorrelated references
        # to target table

class TestAddMethod(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa'))
        @db.tt.add_method.all
        def select_all(table,orderby=None):
            return table._db(table).select(orderby=orderby)
        self.assertEqual(db.tt.insert(aa='1'), 1)
        if not IS_TERADATA:
            self.assertEqual(db.tt.insert(aa='1'), 2)
            self.assertEqual(db.tt.insert(aa='1'), 3)
        else:
            self.assertEqual(db.tt.insert(aa='1'), 1)
            self.assertEqual(db.tt.insert(aa='1'), 1)
        self.assertEqual(len(db.tt.all()), 3)


class TestBelongs(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa'))
        self.assertEqual(db.tt.insert(aa='1'), 1)
        if not IS_TERADATA:
            self.assertEqual(db.tt.insert(aa='2'), 2)
            self.assertEqual(db.tt.insert(aa='3'), 3)
        else:
            self.assertEqual(db.tt.insert(aa='2'), 1)
            self.assertEqual(db.tt.insert(aa='3'), 1)
        self.assertEqual(db(db.tt.aa.belongs(('1', '3'))).count(),
                         2)
        self.assertEqual(db(db.tt.aa.belongs(db(db.tt.id
                          > 2)._select(db.tt.aa))).count(), 1)
        self.assertEqual(db(db.tt.aa.belongs(db(db.tt.aa.belongs(('1',
                         '3')))._select(db.tt.aa))).count(), 2)
        self.assertEqual(db(db.tt.aa.belongs(db(db.tt.aa.belongs(db
                         (db.tt.aa.belongs(('1', '3')))._select(db.tt.aa)))._select(
                         db.tt.aa))).count(),
                         2)


class TestContains(DALtest):
    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa', 'list:string'), Field('bb','string'))
        self.assertEqual(db.tt.insert(aa=['aaa','bbb'],bb='aaa'), 1)
        if not IS_TERADATA:
            self.assertEqual(db.tt.insert(aa=['bbb','ddd'],bb='abb'), 2)
            self.assertEqual(db.tt.insert(aa=['eee','aaa'],bb='acc'), 3)
        else:
            self.assertEqual(db.tt.insert(aa=['bbb','ddd'],bb='abb'), 1)
            self.assertEqual(db.tt.insert(aa=['eee','aaa'],bb='acc'), 1)
        self.assertEqual(db(db.tt.aa.contains('aaa')).count(), 2)
        self.assertEqual(db(db.tt.aa.contains('bbb')).count(), 2)
        self.assertEqual(db(db.tt.aa.contains('aa')).count(), 0)
        self.assertEqual(db(db.tt.bb.contains('a')).count(), 3)
        self.assertEqual(db(db.tt.bb.contains('b')).count(), 1)
        self.assertEqual(db(db.tt.bb.contains('d')).count(), 0)
        self.assertEqual(db(db.tt.aa.contains(db.tt.bb)).count(), 1)
        #case-sensitivity tests, if 1 it isn't
        is_case_insensitive = db(db.tt.bb.like('%AA%')).count()
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
        self.assertEqual(db.tt.insert(aa=['123','456'],bb='123', cc=12), 1)
        if not IS_TERADATA:
            self.assertEqual(db.tt.insert(aa=['124','456'],bb='123', cc=123), 2)
            self.assertEqual(db.tt.insert(aa=['125','457'],bb='23', cc=125),  3)
        else:
            self.assertEqual(db.tt.insert(aa=['124','456'],bb='123', cc=123), 1)
            self.assertEqual(db.tt.insert(aa=['125','457'],bb='23', cc=125),  1)
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
        db.tt.drop()

        #escaping
        db.define_table('tt', Field('aa'))
        db.tt.insert(aa='perc%ent')
        db.tt.insert(aa='percent')
        db.tt.insert(aa='percxyzent')
        db.tt.insert(aa='under_score')
        db.tt.insert(aa='underxscore')
        db.tt.insert(aa='underyscore')
        self.assertEqual(db(db.tt.aa.contains('perc%ent')).count(), 1)
        self.assertEqual(db(db.tt.aa.contains('under_score')).count(), 1)


class TestLike(DALtest):

    def setUp(self):
        db = self.connect()
        db.define_table('tt', Field('aa'))
        self.assertEqual(isinstance(db.tt.insert(aa='abc'), long), True)
        self.db = db

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

    @unittest.skipIf(IS_MSSQL, "No Regexp on MSSQL")
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

class TestDatetime(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa', 'datetime'))
        self.assertEqual(db.tt.insert(aa=datetime.datetime(1971, 12, 21,
                         11, 30)), 1)
        self.assertEqual(db.tt.insert(aa=datetime.datetime(1971, 11, 21,
                         10, 30)), 2)
        self.assertEqual(db.tt.insert(aa=datetime.datetime(1970, 12, 21,
                         9, 31)), 3)
        self.assertEqual(db(db.tt.aa == datetime.datetime(1971, 12,
                         21, 11, 30)).count(), 1)
        self.assertEqual(db(db.tt.aa.year() == 1971).count(), 2)
        self.assertEqual(db(db.tt.aa.month() > 11).count(), 2)
        self.assertEqual(db(db.tt.aa.day() >= 21).count(), 3)
        self.assertEqual(db(db.tt.aa.hour() < 10).count(), 1)
        self.assertEqual(db(db.tt.aa.minutes() <= 30).count(), 2)
        self.assertEqual(db(db.tt.aa.seconds() != 31).count(), 3)
        self.assertEqual(db(db.tt.aa.epoch() < 365*24*3600).delete(), 1)
        db.tt.drop()

        db.define_table('tt', Field('aa', 'time'))
        t0 = datetime.time(10, 30, 55)
        db.tt.insert(aa=t0)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        db.tt.drop()

        db.define_table('tt', Field('aa', 'date'))
        t0 = datetime.date.today()
        db.tt.insert(aa=t0)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)


class TestExpressions(DALtest):

    @unittest.skipIf(IS_POSTGRESQL, "PG8000 does not like these")
    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa', 'integer'),
                        Field('bb', 'integer'), Field('cc'))
        self.assertEqual(db.tt.insert(aa=1, bb=0), 1)
        self.assertEqual(db.tt.insert(aa=2, bb=0), 2)
        self.assertEqual(db.tt.insert(aa=3, bb=0), 3)

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

    def testUpdate(self):
        db = self.connect()

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

    def testSubstring(self):
        db = self.connect()
        t0 = db.define_table('t0', Field('name'))
        input_name = "web2py"
        t0.insert(name=input_name)
        exp_slice = t0.name.lower()[4:6]
        exp_slice_no_max = t0.name.lower()[4:]
        exp_slice_neg_max = t0.name.lower()[2:-2]
        exp_slice_neg_start = t0.name.lower()[-2:]
        exp_item = t0.name.lower()[3]
        out = db(t0).select(exp_slice, exp_item, exp_slice_no_max, exp_slice_neg_max, exp_slice_neg_start).first()
        self.assertEqual(out[exp_slice], input_name[4:6])
        self.assertEqual(out[exp_item], input_name[3])
        self.assertEqual(out[exp_slice_no_max], input_name[4:])
        self.assertEqual(out[exp_slice_neg_max], input_name[2:-2])
        self.assertEqual(out[exp_slice_neg_start], input_name[-2:])

    def testOps(self):
        db = self.connect()
        t0 = db.define_table('t0', Field('vv', 'integer'))
        self.assertEqual(db.t0.insert(vv=1), 1)
        self.assertEqual(db.t0.insert(vv=2), 2)
        self.assertEqual(db.t0.insert(vv=3), 3)
        sum = db.t0.vv.sum()
        count=db.t0.vv.count()
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
        count=db.t0.vv.count().with_alias('c')
        op = sum/count
        #self.assertEqual(db(t0).select(op).first()[op], 2)


class TestTableAliasing(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('t1', Field('aa'))
        db.define_table('t2',
            Field('pk', type='id', unique=True, notnull=True),
            Field('bb', type='integer'), rname='tt')
        tab1 = db.t1.with_alias('test1')
        tab2 = db.t2.with_alias('test2')
        self.assertIs(tab2.id, tab2.pk)
        self.assertIs(tab2._id, tab2.pk)
        self.assertEqual(tab1._dalname, 't1')
        self.assertEqual(tab1._tablename, 'test1')
        self.assertEqual(tab2._dalname, 't2')
        self.assertEqual(tab2._tablename, 'test2')
        self.assertEqual(tab2._rname, 'tt')
        tab1.insert(aa='foo')
        tab1.insert(aa='bar')
        result = db(tab1).select(tab1.aa, orderby=tab1.aa)
        self.assertEqual(result.as_list(), [{'aa': 'bar'}, {'aa': 'foo'}])

        if not IS_SQLITE:
            db(tab1.aa == 'foo').update(aa='baz')
            result = db(tab1).select(tab1.aa, orderby=tab1.aa)
            self.assertEqual(result.as_list(), [{'aa': 'bar'}, {'aa': 'baz'}])
            db(tab1.aa == 'bar').delete()
            result = db(tab1).select(tab1.aa, orderby=tab1.aa)
            self.assertEqual(result.as_list(), [{'aa': 'baz'}])
        else:
            with self.assertRaises(SyntaxError):
                db(tab1.aa == 'foo').update(aa='baz')
            with self.assertRaises(SyntaxError):
                db(tab1.aa == 'bar').delete()

        tab2.insert(bb=123)
        tab2.insert(bb=456)
        result = db(tab2).select(tab2.bb, orderby=tab2.bb)
        self.assertEqual(result.as_list(), [{'bb': 123}, {'bb': 456}])

        if not IS_SQLITE:
            db(tab2.bb == 456).update(bb=789)
            result = db(tab2).select(tab2.bb, orderby=tab2.bb)
            self.assertEqual(result.as_list(), [{'bb': 123}, {'bb': 789}])
            db(tab2.bb == 123).delete()
            result = db(tab2).select(tab2.bb, orderby=tab2.bb)
            self.assertEqual(result.as_list(), [{'bb': 789}])
        else:
            with self.assertRaises(SyntaxError):
                db(tab2.bb == 456).update(bb=789)
            with self.assertRaises(SyntaxError):
                db(tab2.bb == 123).delete()

class TestJoin(DALtest):

    def testRun(self):
        db = self.connect()
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
        db.t2.drop()
        db.t1.drop()

        db.define_table('person',Field('name'))
        id = db.person.insert(name="max")
        self.assertEqual(id.name,'max')
        db.define_table('dog',Field('name'),Field('ownerperson','reference person'))
        db.dog.insert(name='skipper',ownerperson=1)
        row = db(db.person.id==db.dog.ownerperson).select().first()
        self.assertEqual(row[db.person.name],'max')
        self.assertEqual(row['person.name'],'max')
        db.dog.drop()
        self.assertEqual(len(db.person._referenced_by),0)


class TestMinMaxSumAvg(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa', 'integer'))
        self.assertEqual(db.tt.insert(aa=1), 1)
        self.assertEqual(db.tt.insert(aa=2), 2)
        self.assertEqual(db.tt.insert(aa=3), 3)
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


class TestMigrations(unittest.TestCase):

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), Field('BB'),
                        migrate='.storage.table')
        db.define_table('t1', Field('aa'), Field('BB'),
                        migrate='.storage.rname', rname='foo')
        db.commit()
        db.close()
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), migrate='.storage.table')
        db.define_table('t1', Field('aa'), migrate='.storage.rname',
                        rname='foo')
        db.commit()
        db.close()
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), Field('b'),
                        migrate='.storage.table')
        db.define_table('t1', Field('aa'), Field('b'),
                        migrate='.storage.rname', rname='foo')
        db.commit()
        db.close()
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), Field('b', 'text'),
                        migrate='.storage.table')
        db.define_table('t1', Field('aa'), Field('b', 'text'),
                        migrate='.storage.rname', rname='foo')
        db.commit()
        db.close()
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa'), migrate='.storage.table')
        db.define_table('t1', Field('aa'), migrate='.storage.rname',
                        rname='foo')
        db.tt.drop()
        db.t1.drop()
        db.commit()
        db.close()

    def testFieldRName(self):
        def checkWrite(db, table, data):
            rowid = table.insert(**data)
            query = (table._id == rowid)
            fields = [table[x] for x in data.keys()]
            row = db(query).select(*fields).first()
            self.assertIsNot(row, None)
            self.assertEqual(row.as_dict(), data)
            db(query).delete()

        # Create tables
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', rname='faa'),
            Field('BB', rname='fbb'), migrate='.storage.table')
        db.define_table('t1', Field('aa', rname='faa'),
            Field('BB', rname='fbb'), migrate='.storage.rname', rname='foo')
        data = dict(aa='aa1', BB='BB1')
        checkWrite(db, db.tt, data)
        checkWrite(db, db.t1, data)
        db.commit()
        db.close()

        # Drop field defined by CREATE TABLE
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', rname='faa'),
            migrate='.storage.table')
        db.define_table('t1', Field('aa', rname='faa'),
            migrate='.storage.rname', rname='foo')
        data = dict(aa='aa2')
        checkWrite(db, db.tt, data)
        checkWrite(db, db.t1, data)
        db.commit()
        db.close()

        # Add new field
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', rname='faa'), Field('b', rname='fb'),
            migrate='.storage.table')
        db.define_table('t1', Field('aa', rname='faa'), Field('b', rname='fb'),
            migrate='.storage.rname', rname='foo')
        data = dict(aa='aa3', b='b3')
        integrity = dict(aa='data', b='integrity')
        checkWrite(db, db.tt, data)
        checkWrite(db, db.t1, data)
        db.tt.insert(**integrity)
        db.t1.insert(**integrity)
        db.commit()
        db.close()

        # Change field type
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', rname='faa'),
            Field('b', 'text', rname='fb'), migrate='.storage.table')
        db.define_table('t1', Field('aa', rname='faa'),
            Field('b', 'text', rname='fb'), migrate='.storage.rname',
            rname='foo')
        data = dict(aa='aa4', b='b4')
        checkWrite(db, db.tt, data)
        checkWrite(db, db.t1, data)
        row = db(db.tt).select(*[db.tt[x] for x in integrity.keys()]).first()
        self.assertIsNot(row, None)
        self.assertEqual(row.as_dict(), integrity)
        row2 = db(db.t1).select(*[db.t1[x] for x in integrity.keys()]).first()
        self.assertIsNot(row2, None)
        self.assertEqual(row2.as_dict(), integrity)
        db.commit()
        db.close()

        # Change field rname
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', rname='faa'),
            Field('b', 'text', rname='xb'), migrate='.storage.table')
        db.define_table('t1', Field('aa', rname='faa'),
            Field('b', 'text', rname='xb'), migrate='.storage.rname',
            rname='foo')
        data = dict(aa='aa4', b='b4')
        checkWrite(db, db.tt, data)
        checkWrite(db, db.t1, data)
        row = db(db.tt).select(*[db.tt[x] for x in integrity.keys()]).first()
        self.assertIsNot(row, None)
        self.assertEqual(row.as_dict(), integrity)
        row2 = db(db.t1).select(*[db.t1[x] for x in integrity.keys()]).first()
        self.assertIsNot(row2, None)
        self.assertEqual(row2.as_dict(), integrity)
        db.commit()
        db.close()

        # Drop field defined by ALTER TABLE
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('aa', rname='faa'),
            migrate='.storage.table')
        db.define_table('t1', Field('aa', rname='faa'),
            migrate='.storage.rname', rname='foo')
        data = dict(aa='aa5')
        checkWrite(db, db.tt, data)
        checkWrite(db, db.t1, data)
        db.tt.drop()
        db.t1.drop()
        db.commit()
        db.close()


    def tearDown(self):
        if os.path.exists('.storage.db'):
            os.unlink('.storage.db')
        if os.path.exists('.storage.table'):
            os.unlink('.storage.table')
        if os.path.exists('.storage.rname'):
            os.unlink('.storage.rname')


class TestReference(DALtest):

    def testRun(self):
        scenarios = (
            (True,  'CASCADE'),
            (False, 'CASCADE'),
            (False, 'SET NULL'),
        )
        for (b, ondelete) in scenarios:
            db = self.connect(bigint_id=b)
            if DEFAULT_URI.startswith('mssql'):
                #multiple cascade gotcha
                for key in ['reference','reference FK']:
                    db._adapter.types[key]=db._adapter.types[key].replace(
                    '%(on_delete_action)s','NO ACTION')
            db.define_table('tt', Field('name'),
                            Field('aa','reference tt',ondelete=ondelete))
            db.commit()
            x = db.tt.insert(name='xxx')
            self.assertEqual(x.id, 1)
            self.assertEqual(x['id'], 1)
            x.aa = x
            self.assertEqual(x.aa, 1)
            x.update_record()
            x1 = db.tt[1]
            self.assertEqual(x1.aa, 1)
            self.assertEqual(x1.aa.aa.aa.aa.aa.aa.name, 'xxx')
            y=db.tt.insert(name='yyy', aa = x1)
            self.assertEqual(y.aa, x1.id)

            if not DEFAULT_URI.startswith('mssql'):
                self.assertEqual(db.tt.insert(name='zzz'), 3)
                self.assertEqual(db(db.tt.name).count(), 3)
                db(db.tt.id == x).delete()
                expected_count = {
                    'SET NULL': 2,
                    'NO ACTION': 2,
                    'CASCADE': 1,
                }
                self.assertEqual(db(db.tt.name).count(), expected_count[ondelete])
                if ondelete == 'SET NULL':
                    self.assertEqual(db(db.tt.name == 'yyy').select()[0].aa, None)

            self.tearDown()


class TestClientLevelOps(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa', represent=lambda x,r:'x'+x),
            Field('bb', type='integer', represent=lambda x,r:'y'+str(x)))
        db.commit()
        db.tt.insert(aa="test", bb=1)
        rows1 = db(db.tt.id<0).select()
        rows2 = db(db.tt.id>0).select()
        self.assertNotEqual(rows1, rows2)
        rows1 = db(db.tt.id>0).select()
        rows2 = db(db.tt.id>0).select()
        self.assertEqual(rows1, rows2)
        rows3 = rows1 & rows2
        self.assertEqual(len(rows3), 2)
        rows4 = rows1 | rows2
        self.assertEqual(len(rows4), 1)
        rows5 = rows1.find(lambda row: row.aa=="test")
        self.assertEqual(len(rows5), 1)
        rows6 = rows2.exclude(lambda row: row.aa=="test")
        self.assertEqual(len(rows6), 1)
        rows7 = rows5.sort(lambda row: row.aa)
        self.assertEqual(len(rows7), 1)
        def represent(f, v, r):
            return 'z' + str(v)

        db.representers = {
            'rows_render': represent,
        }
        db.tt.insert(aa="foo", bb=2)
        rows = db(db.tt.id>0).select()
        exp1 = [Row(aa='ztest', bb='z1', id=rows[0]['id']),
            Row(aa='zfoo', bb='z2', id=rows[1]['id'])]
        exp2 = [Row(aa='ztest', bb=1, id=rows[0]['id']),
            Row(aa='zfoo', bb=2, id=rows[1]['id'])]
        exp3 = [Row(aa='test', bb='z1', id=rows[0]['id']),
            Row(aa='foo', bb='z2', id=rows[1]['id'])]
        self.assertEqual(rows.render(i=0), exp1[0])
        self.assertEqual(rows.render(i=0, fields=[db.tt.aa, db.tt.bb]),
            exp1[0])
        self.assertEqual(rows.render(i=0, fields=[db.tt.aa]), exp2[0])
        self.assertEqual(rows.render(i=0, fields=[db.tt.bb]), exp3[0])
        self.assertEqual(list(rows.render()), exp1)
        self.assertEqual(list(rows.render(fields=[db.tt.aa, db.tt.bb])), exp1)
        self.assertEqual(list(rows.render(fields=[db.tt.aa])), exp2)
        self.assertEqual(list(rows.render(fields=[db.tt.bb])), exp3)
        ret = rows.render(i=0)
        rows = db(db.tt.id>0).select()
        rows.compact=False
        row = rows[0]
        self.assertIn('tt', row)
        self.assertIn('id', row.tt)
        self.assertNotIn('id', row)
        rows.compact=True
        row = rows[0]
        self.assertNotIn('tt', row)
        self.assertIn('id', row)

        rows = db(db.tt.id>0).select(db.tt.id.max())
        rows.compact=False
        row = rows[0]
        self.assertNotIn('tt', row)
        self.assertIn('_extra', row)

        rows = db(db.tt.id>0).select(db.tt.id.max())
        rows.compact=True
        row = rows[0]
        self.assertNotIn('tt', row)
        self.assertIn('_extra', row)
        db.tt.drop()

        db.define_table('tt', Field('aa'), Field.Virtual('bb', lambda row: ':p'))
        db.tt.insert(aa="test")
        rows = db(db.tt.id>0).select()
        row = rows.first()
        self.assertNotIn('tt', row)
        self.assertIn('id', row)
        self.assertIn('bb', row)

        rows.compact = False
        row = rows.first()
        self.assertIn('tt', row)
        self.assertEqual(len(row.keys()), 1)
        self.assertIn('id', row.tt)
        self.assertIn('bb', row.tt)
        self.assertNotIn('id', row)
        self.assertNotIn('bb', row)


class TestVirtualFields(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt', Field('aa'))
        db.commit()
        db.tt.insert(aa="test")
        class Compute:
            def a_upper(row): return row.tt.aa.upper()
        db.tt.virtualfields.append(Compute())
        assert db(db.tt.id>0).select().first().a_upper == 'TEST'


class TestComputedFields(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('tt',
                        Field('aa'),
                        Field('bb',default='x'),
                        Field('cc',compute=lambda r: r.aa+r.bb))
        db.commit()
        id = db.tt.insert(aa="z")
        self.assertEqual(db.tt[id].cc,'zx')
        db.tt.drop()
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


class TestCommonFilters(DALtest):

    def testRun(self):
        db = self.connect()
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
        q = db.t2.b==db.t1.id
        self.assertEqual(db(q).count(),2)
        self.assertEqual(db(q).count(),2)
        self.assertEqual(len(db(db.t1).select(left=db.t2.on(q))),3)
        db.t2._common_filter = lambda q: db.t2.aa<6
        self.assertEqual(db(q).count(),1)
        self.assertEqual(db(q).count(),1)
        self.assertEqual(len(db(db.t1).select(left=db.t2.on(q))),2)
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


class TestImportExportFields(DALtest):

    def testRun(self):
        db = self.connect()
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
        assert db(db.person.id==db.pet.friend)(db.person.name==db.pet.name).count()==10


class TestImportExportUuidFields(DALtest):

    def testRun(self):
        db = self.connect()
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
        stream = StringIO(stream.getvalue())
        db.import_from_csv_file(stream)
        assert db(db.person).count()==10
        assert db(db.person.id==db.pet.friend)(db.person.name==db.pet.name).count()==20


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
        db.pet.drop()
        db.commit()

        db2.commit()

        dbjson = db.as_json(sanitize=False)
        assert isinstance(dbjson, basestring) and len(dbjson) > 0
        db3 = DAL(**json.loads(dbjson))
        assert hasattr(db3, "person") and hasattr(db3.person, "uuid")
        assert db3.person.uuid.type == db.person.uuid.type
        db3.person.drop()
        db3.commit()
        db3.close()

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
        assert (db4.tvshow.insert(), db4.tvshow.insert(name="Loriot"),
                db4.tvshow.insert(name="Il Mattatore")) == (1, 2, 3)
        assert db4(db4.tvshow).select().first().id == 1
        assert db4(db4.tvshow).select().first().name == mpfc

        db4.staff.drop()
        db4.tvshow.drop()
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
        assert db6(db6.staff).select().first().id == 1


        db6.staff.drop()
        db6.tvshow.drop()
        db6.commit()

        db.close()
        db2.close()
        db4.close()
        db5.close()
        db6.close()


class TestSelectAsDict(DALtest):

    def testSelect(self):
        db = self.connect()
        db.define_table(
            'a_table',
            Field('b_field'),
            Field('a_field'),
            )
        db.a_table.insert(a_field="aa1", b_field="bb1")
        rtn = db.executesql("SELECT id, b_field, a_field FROM a_table", as_dict=True)
        self.assertEqual(rtn[0]['b_field'], 'bb1')
        rtn = db.executesql("SELECT id, b_field, a_field FROM a_table", as_ordered_dict=True)
        self.assertEqual(rtn[0]['b_field'], 'bb1')
        self.assertEqual(list(rtn[0].keys()), ['id', 'b_field', 'a_field'])


class TestExecuteSQL(DALtest):

    def testSelect(self):
        db = self.connect(DEFAULT_URI, entity_quoting=False)
        db.define_table(
            'a_table',
            Field('b_field'),
            Field('a_field'),
            )
        db.a_table.insert(a_field="aa1", b_field="bb1")
        rtn = db.executesql("SELECT id, b_field, a_field FROM a_table", as_dict=True)
        self.assertEqual(rtn[0]['b_field'], 'bb1')
        rtn = db.executesql("SELECT id, b_field, a_field FROM a_table", as_ordered_dict=True)
        self.assertEqual(rtn[0]['b_field'], 'bb1')
        self.assertEqual(rtn[0]['b_field'], 'bb1')
        self.assertEqual(list(rtn[0].keys()), ['id', 'b_field', 'a_field'])

        rtn = db.executesql("select id, b_field, a_field from a_table", fields=db.a_table)
        self.assertTrue(all(x in rtn[0].keys() for x in ['id', 'b_field', 'a_field']))
        self.assertEqual(rtn[0].b_field, 'bb1')

        rtn = db.executesql("select id, b_field, a_field from a_table", fields=db.a_table,
                            colnames=['a_table.id', 'a_table.b_field', 'a_table.a_field'])

        self.assertTrue(all(x in rtn[0].keys() for x in ['id', 'b_field', 'a_field']))
        self.assertEqual(rtn[0].b_field, 'bb1')
        rtn = db.executesql("select COUNT(*) from a_table", fields=[db.a_table.id.count()], colnames=['foo'])
        self.assertEqual(rtn[0].foo, 1)

class TestRNameTable(DALtest):
    #tests for highly experimental rname attribute

    def testSelect(self):
        db = self.connect()
        rname = 'a_very_complicated_tablename'
        db.define_table(
            'easy_name',
            Field('a_field'),
            rname=rname
            )
        rtn = db.easy_name.insert(a_field='a')
        self.assertEqual(rtn.id, 1)
        rtn = db(db.easy_name.a_field == 'a').select()
        self.assertEqual(len(rtn), 1)
        self.assertEqual(rtn[0].id, 1)
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
        avg = db.easy_name.id.avg()
        rtn = db(db.easy_name.id > 0).select(avg)
        self.assertEqual(rtn[0][avg], 3)
        rname = 'this_is_the_person_table'
        db.define_table(
            'person',
            Field('name', default="Michael"),
            Field('uuid'),
            rname=rname
            )
        rname = 'this_is_the_pet_table'
        db.define_table(
            'pet',
            Field('friend','reference person'),
            Field('name'),
            rname=rname
            )
        michael = db.person.insert() #default insert
        john = db.person.insert(name='John')
        luke = db.person.insert(name='Luke')

        #michael owns Phippo
        phippo = db.pet.insert(friend=michael, name="Phippo")
        #john owns Dunstin and Gertie
        dunstin = db.pet.insert(friend=john, name="Dunstin")
        gertie = db.pet.insert(friend=john, name="Gertie")

        rtn = db(db.person.id == db.pet.friend).select(orderby=db.person.id|db.pet.id)
        self.assertEqual(len(rtn), 3)
        self.assertEqual(rtn[0].person.id, michael)
        self.assertEqual(rtn[0].person.name, 'Michael')
        self.assertEqual(rtn[0].pet.id, phippo)
        self.assertEqual(rtn[0].pet.name, 'Phippo')
        self.assertEqual(rtn[1].person.id, john)
        self.assertEqual(rtn[1].person.name, 'John')
        self.assertEqual(rtn[1].pet.name, 'Dunstin')
        self.assertEqual(rtn[2].pet.name, 'Gertie')
        #fetch owners, eventually with pet
        #main point is retrieving Luke with no pets
        rtn = db(db.person.id > 0).select(
            orderby=db.person.id|db.pet.id,
            left=db.pet.on(db.person.id == db.pet.friend)
            )
        self.assertEqual(rtn[0].person.id, michael)
        self.assertEqual(rtn[0].person.name, 'Michael')
        self.assertEqual(rtn[0].pet.id, phippo)
        self.assertEqual(rtn[0].pet.name, 'Phippo')
        self.assertEqual(rtn[3].person.name, 'Luke')
        self.assertEqual(rtn[3].person.id, luke)
        self.assertEqual(rtn[3].pet.name, None)
        #lets test a subquery
        subq = db(db.pet.name == "Gertie")._select(db.pet.friend)
        rtn = db(db.person.id.belongs(subq)).select()
        self.assertEqual(rtn[0].id, 2)
        self.assertEqual(rtn[0]('person.name'), 'John')
        #as dict
        rtn = db(db.person.id > 0).select().as_dict()
        self.assertEqual(rtn[1]['name'], 'Michael')
        #as list
        rtn = db(db.person.id > 0).select().as_list()
        self.assertEqual(rtn[0]['name'], 'Michael')
        #isempty
        rtn = db(db.person.id > 0).isempty()
        self.assertEqual(rtn, False)
        #join argument
        rtn = db(db.person).select(orderby=db.person.id|db.pet.id,
                                   join=db.pet.on(db.person.id==db.pet.friend))
        self.assertEqual(len(rtn), 3)
        self.assertEqual(rtn[0].person.id, michael)
        self.assertEqual(rtn[0].person.name, 'Michael')
        self.assertEqual(rtn[0].pet.id, phippo)
        self.assertEqual(rtn[0].pet.name, 'Phippo')
        self.assertEqual(rtn[1].person.id, john)
        self.assertEqual(rtn[1].person.name, 'John')
        self.assertEqual(rtn[1].pet.name, 'Dunstin')
        self.assertEqual(rtn[2].pet.name, 'Gertie')

        #aliases
        if DEFAULT_URI.startswith('mssql'):
            #multiple cascade gotcha
            for key in ['reference','reference FK']:
                db._adapter.types[key]=db._adapter.types[key].replace(
                '%(on_delete_action)s','NO ACTION')
        rname = 'the_cubs'
        db.define_table('pet_farm',
            Field('name'),
            Field('father','reference pet_farm'),
            Field('mother','reference pet_farm'),
            rname=rname
        )

        minali = db.pet_farm.insert(name='Minali')
        osbert = db.pet_farm.insert(name='Osbert')
        #they had a cub
        selina = db.pet_farm.insert(name='Selina', father=osbert, mother=minali)

        father = db.pet_farm.with_alias('father')
        mother = db.pet_farm.with_alias('mother')

        #fetch pets with relatives
        rtn = db().select(
            db.pet_farm.name, father.name, mother.name,
            left=[
                father.on(father.id == db.pet_farm.father),
                mother.on(mother.id == db.pet_farm.mother)
            ],
            orderby=db.pet_farm.id
        )

        self.assertEqual(len(rtn), 3)
        self.assertEqual(rtn[0].pet_farm.name, 'Minali')
        self.assertEqual(rtn[0].father.name, None)
        self.assertEqual(rtn[0].mother.name, None)
        self.assertEqual(rtn[1].pet_farm.name, 'Osbert')
        self.assertEqual(rtn[2].pet_farm.name, 'Selina')
        self.assertEqual(rtn[2].father.name, 'Osbert')
        self.assertEqual(rtn[2].mother.name, 'Minali')

    def testJoin(self):
        db = self.connect()
        rname = 'this_is_table_t1'
        rname2 = 'this_is_table_t2'
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
        db.t2.drop()
        db.t1.drop()

        db.define_table('person',Field('name'), rname=rname)
        id = db.person.insert(name="max")
        self.assertEqual(id.name,'max')
        db.define_table('dog',Field('name'),Field('ownerperson','reference person'), rname=rname2)
        db.dog.insert(name='skipper',ownerperson=1)
        row = db(db.person.id==db.dog.ownerperson).select().first()
        self.assertEqual(row[db.person.name],'max')
        self.assertEqual(row['person.name'],'max')
        db.dog.drop()
        self.assertEqual(len(db.person._referenced_by),0)


class TestRNameFields(DALtest):
    # tests for highly experimental rname attribute
    def testSelect(self):
        db = self.connect()
        rname = 'a_very_complicated_fieldname'
        rname2 = 'rrating_from_1_to_10'
        db.define_table(
            'easy_name',
            Field('a_field', rname=rname),
            Field('rating', 'integer', rname=rname2, default=2)
            )
        rtn = db.easy_name.insert(a_field='a')
        self.assertEqual(rtn.id, 1)
        rtn = db(db.easy_name.a_field == 'a').select()
        self.assertEqual(len(rtn), 1)
        self.assertEqual(rtn[0].id, 1)
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
        avg = db.easy_name.id.avg()
        rtn = db(db.easy_name.id > 0).select(avg)
        self.assertEqual(rtn[0][avg], 3)

        avg = db.easy_name.rating.avg()
        rtn = db(db.easy_name.id > 0).select(avg)
        self.assertEqual(rtn[0][avg], 2)

        rname = 'this_is_the_person_name'
        db.define_table(
            'person',
            Field('id', type='id', rname='fooid'),
            Field('name', default="Michael", rname=rname),
            Field('uuid')
            )
        rname = 'this_is_the_pet_name'
        db.define_table(
            'pet',
            Field('friend','reference person'),
            Field('name', rname=rname)
            )
        michael = db.person.insert() #default insert
        john = db.person.insert(name='John')
        luke = db.person.insert(name='Luke')

        #michael owns Phippo
        phippo = db.pet.insert(friend=michael, name="Phippo")
        #john owns Dunstin and Gertie
        dunstin = db.pet.insert(friend=john, name="Dunstin")
        gertie = db.pet.insert(friend=john, name="Gertie")

        rtn = db(db.person.id == db.pet.friend).select(orderby=db.person.id|db.pet.id)
        self.assertEqual(len(rtn), 3)
        self.assertEqual(rtn[0].person.id, michael)
        self.assertEqual(rtn[0].person.name, 'Michael')
        self.assertEqual(rtn[0].pet.id, phippo)
        self.assertEqual(rtn[0].pet.name, 'Phippo')
        self.assertEqual(rtn[1].person.id, john)
        self.assertEqual(rtn[1].person.name, 'John')
        self.assertEqual(rtn[1].pet.name, 'Dunstin')
        self.assertEqual(rtn[2].pet.name, 'Gertie')
        #fetch owners, eventually with pet
        #main point is retrieving Luke with no pets
        rtn = db(db.person.id > 0).select(
            orderby=db.person.id|db.pet.id,
            left=db.pet.on(db.person.id == db.pet.friend)
            )
        self.assertEqual(rtn[0].person.id, michael)
        self.assertEqual(rtn[0].person.name, 'Michael')
        self.assertEqual(rtn[0].pet.id, phippo)
        self.assertEqual(rtn[0].pet.name, 'Phippo')
        self.assertEqual(rtn[3].person.name, 'Luke')
        self.assertEqual(rtn[3].person.id, luke)
        self.assertEqual(rtn[3].pet.name, None)
        #lets test a subquery
        subq = db(db.pet.name == "Gertie")._select(db.pet.friend)
        rtn = db(db.person.id.belongs(subq)).select()
        self.assertEqual(rtn[0].id, 2)
        self.assertEqual(rtn[0]('person.name'), 'John')
        #as dict
        rtn = db(db.person.id > 0).select().as_dict()
        self.assertEqual(rtn[1]['name'], 'Michael')
        #as list
        rtn = db(db.person.id > 0).select().as_list()
        self.assertEqual(rtn[0]['name'], 'Michael')
        #isempty
        rtn = db(db.person.id > 0).isempty()
        self.assertEqual(rtn, False)
        #join argument
        rtn = db(db.person).select(orderby=db.person.id|db.pet.id,
                                   join=db.pet.on(db.person.id==db.pet.friend))
        self.assertEqual(len(rtn), 3)
        self.assertEqual(rtn[0].person.id, michael)
        self.assertEqual(rtn[0].person.name, 'Michael')
        self.assertEqual(rtn[0].pet.id, phippo)
        self.assertEqual(rtn[0].pet.name, 'Phippo')
        self.assertEqual(rtn[1].person.id, john)
        self.assertEqual(rtn[1].person.name, 'John')
        self.assertEqual(rtn[1].pet.name, 'Dunstin')
        self.assertEqual(rtn[2].pet.name, 'Gertie')

        #aliases
        rname = 'the_cub_name'
        if DEFAULT_URI.startswith('mssql'):
            #multiple cascade gotcha
            for key in ['reference','reference FK']:
                db._adapter.types[key]=db._adapter.types[key].replace(
                '%(on_delete_action)s','NO ACTION')
        db.define_table('pet_farm',
            Field('name', rname=rname),
            Field('father','reference pet_farm'),
            Field('mother','reference pet_farm'),
        )

        minali = db.pet_farm.insert(name='Minali')
        osbert = db.pet_farm.insert(name='Osbert')
        #they had a cub
        selina = db.pet_farm.insert(name='Selina', father=osbert, mother=minali)

        father = db.pet_farm.with_alias('father')
        mother = db.pet_farm.with_alias('mother')

        #fetch pets with relatives
        rtn = db().select(
            db.pet_farm.name, father.name, mother.name,
            left=[
                father.on(father.id == db.pet_farm.father),
                mother.on(mother.id == db.pet_farm.mother)
            ],
            orderby=db.pet_farm.id
        )

        self.assertEqual(len(rtn), 3)
        self.assertEqual(rtn[0].pet_farm.name, 'Minali')
        self.assertEqual(rtn[0].father.name, None)
        self.assertEqual(rtn[0].mother.name, None)
        self.assertEqual(rtn[1].pet_farm.name, 'Osbert')
        self.assertEqual(rtn[2].pet_farm.name, 'Selina')
        self.assertEqual(rtn[2].father.name, 'Osbert')
        self.assertEqual(rtn[2].mother.name, 'Minali')

    def testRun(self):
        db = self.connect()
        rname = 'a_very_complicated_fieldname'
        for ft in ['string', 'text', 'password', 'upload', 'blob']:
            db.define_table('tt', Field('aa', ft, default='', rname=rname))
            self.assertEqual(db.tt.insert(aa='x'), 1)
            self.assertEqual(db().select(db.tt.aa)[0].aa, 'x')
            db.tt.drop()
        db.define_table('tt', Field('aa', 'integer', default=1, rname=rname))
        self.assertEqual(db.tt.insert(aa=3), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, 3)
        db.tt.drop()
        db.define_table('tt', Field('aa', 'double', default=1, rname=rname))
        self.assertEqual(db.tt.insert(aa=3.1), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, 3.1)
        db.tt.drop()
        db.define_table('tt', Field('aa', 'boolean', default=True, rname=rname))
        self.assertEqual(db.tt.insert(aa=True), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, True)
        db.tt.drop()
        db.define_table('tt', Field('aa', 'json', default={}, rname=rname))
        self.assertEqual(db.tt.insert(aa={}), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, {})
        db.tt.drop()
        db.define_table('tt', Field('aa', 'date',
                        default=datetime.date.today(), rname=rname))
        t0 = datetime.date.today()
        self.assertEqual(db.tt.insert(aa=t0), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)
        db.tt.drop()
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
        self.assertEqual(db.tt.insert(aa=t0), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)

        ## Row APIs
        row = db().select(db.tt.aa)[0]
        self.assertEqual(db.tt[1].aa,t0)
        self.assertEqual(db.tt['aa'],db.tt.aa)
        self.assertEqual(db.tt(1).aa,t0)
        self.assertTrue(db.tt(1,aa=None)==None)
        self.assertFalse(db.tt(1,aa=t0)==None)
        self.assertEqual(row.aa,t0)
        self.assertEqual(row['aa'],t0)
        self.assertEqual(row['tt.aa'],t0)
        self.assertEqual(row('tt.aa'),t0)
        self.assertTrue('aa' in row)
        self.assertTrue('pydal' not in row)
        self.assertTrue(hasattr(row, 'aa'))
        self.assertFalse(hasattr(row, 'pydal'))

        ## Lazy and Virtual fields
        db.tt.b = Field.Virtual(lambda row: row.tt.aa)
        db.tt.c = Field.Lazy(lambda row: row.tt.aa)
        row = db().select(db.tt.aa)[0]
        self.assertEqual(row.b,t0)
        self.assertEqual(row.c(),t0)

        db.tt.drop()
        db.define_table('tt', Field('aa', 'time', default='11:30', rname=rname))
        t0 = datetime.time(10, 30, 55)
        self.assertEqual(db.tt.insert(aa=t0), 1)
        self.assertEqual(db().select(db.tt.aa)[0].aa, t0)

    def testInsert(self):
        db = self.connect()
        rname = 'a_very_complicated_fieldname'
        db.define_table('tt', Field('aa', rname=rname))
        self.assertEqual(db.tt.insert(aa='1'), 1)
        self.assertEqual(db.tt.insert(aa='1'), 2)
        self.assertEqual(db.tt.insert(aa='1'), 3)
        self.assertEqual(db(db.tt.aa == '1').count(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), True)
        self.assertEqual(db(db.tt.aa == '1').update(aa='2'), 3)
        self.assertEqual(db(db.tt.aa == '2').count(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), False)
        self.assertEqual(db(db.tt.aa == '2').delete(), 3)
        self.assertEqual(db(db.tt.aa == '2').isempty(), True)

    def testJoin(self):
        db = self.connect()
        rname = 'this_is_field_aa'
        rname2 = 'this_is_field_b'
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
        db.t2.drop()
        db.t1.drop()

        db.define_table('person',Field('name', rname=rname))
        id = db.person.insert(name="max")
        self.assertEqual(id.name,'max')
        db.define_table('dog',Field('name', rname=rname),Field('ownerperson','reference person', rname=rname2))
        db.dog.insert(name='skipper',ownerperson=1)
        row = db(db.person.id==db.dog.ownerperson).select().first()
        self.assertEqual(row[db.person.name],'max')
        self.assertEqual(row['person.name'],'max')
        db.dog.drop()
        self.assertEqual(len(db.person._referenced_by),0)

    def testTFK(self):
        db = self.connect()
        if 'reference TFK' not in db._adapter.types:
            self.skipTest('Adapter does not support TFK references')
        db.define_table('t1',
            Field('id1', type='string', length=1, rname='foo1'),
            Field('id2', type='integer', rname='foo2'),
            Field('val', type='integer'),
            primarykey=['id1', 'id2'])
        db.define_table('t2',
            Field('ref1', type=db.t1.id1, rname='bar1'),
            Field('ref2', type=db.t1.id2, rname='bar2'))
        db.t1.insert(id1='a', id2=1, val=10)
        db.t1.insert(id1='a', id2=2, val=30)
        db.t2.insert(ref1='a', ref2=1)
        query = (db.t1.id1 == db.t2.ref1) & (db.t1.id2 == db.t2.ref2)
        result = db(query).select(db.t1.ALL)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id1'], 'a')
        self.assertEqual(result[0]['id2'], 1)
        self.assertEqual(result[0]['val'], 10)


class TestQuoting(DALtest):

    # tests for case sensitivity
    def testCase(self):
        db = self.connect(ignore_field_case=False, entity_quoting=True)
        if DEFAULT_URI.startswith('mssql'):
            #multiple cascade gotcha
            for key in ['reference','reference FK']:
                db._adapter.types[key]=db._adapter.types[key].replace(
                '%(on_delete_action)s','NO ACTION')

        t0 = db.define_table('t0',
                        Field('f', 'string'))
        t1 = db.define_table('b',
                             Field('B', t0),
                             Field('words', 'text'))

        blather = 'blah blah and so'
        t0[0] = {'f': 'content'}
        t1[0] = {'B': int(t0[1]['id']),
                 'words': blather}

        r = db(db.t0.id==db.b.B).select()

        self.assertEqual(r[0].b.words, blather)

        t1.drop()
        t0.drop()

        # test field case
        try:
            t0 = db.define_table('table_is_a_test',
                                 Field('a_a'),
                                 Field('a_A'))
        except Exception as e:
            # some db does not support case sensitive field names mysql is one of them.
            if DEFAULT_URI.startswith('mysql:') or DEFAULT_URI.startswith('sqlite:'):
                db.rollback()
                return
            if 'Column names in each table must be unique' in e.args[1]:
                db.rollback()
                return
            raise e

        t0[0] = dict(a_a = 'a_a', a_A='a_A')

        self.assertEqual(t0[1].a_a, 'a_a')
        self.assertEqual(t0[1].a_A, 'a_A')

    def testPKFK(self):

        # test primary keys

        db = self.connect(ignore_field_case=False)
        if DEFAULT_URI.startswith('mssql'):
            #multiple cascade gotcha
            for key in ['reference','reference FK']:
                db._adapter.types[key]=db._adapter.types[key].replace(
                '%(on_delete_action)s','NO ACTION')
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

        if DEFAULT_URI.startswith('mssql'):
            #there's no drop cascade in mssql
            t3.drop()
            t4.drop()
            t2.drop()
            t0.drop()
        else:
            t0.drop('cascade')
            t2.drop()
            t3.drop()
            t4.drop()


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

class TestGis(DALtest):

    def testGeometry(self):
        from pydal import geoPoint, geoLine, geoPolygon
        if not IS_POSTGRESQL: return
        db = self.connect()
        t0 = db.define_table('t0', Field('point', 'geometry()'))
        t1 = db.define_table('t1', Field('line', 'geometry(public, 4326, 2)'))
        t2 = db.define_table('t2', Field('polygon', 'geometry(public, 4326, 2)'))
        t0.insert(point=geoPoint(1,1))
        text = db(db.t0.id).select(db.t0.point.st_astext()).first()[db.t0.point.st_astext()]
        self.assertEqual(text, "POINT(1 1)")
        t1.insert(line=geoLine((1,1),(2,2)))
        text = db(db.t1.id).select(db.t1.line.st_astext()).first()[db.t1.line.st_astext()]
        self.assertEqual(text, "LINESTRING(1 1,2 2)")
        t2.insert(polygon=geoPolygon((0,0),(2,0),(2,2),(0,2),(0,0)))
        text = db(db.t2.id).select(db.t2.polygon.st_astext()).first()[db.t2.polygon.st_astext()]
        self.assertEqual(text, "POLYGON((0 0,2 0,2 2,0 2,0 0))")
        query = t0.point.st_intersects(geoLine((0,0),(2,2)))
        output = db(query).select(db.t0.point).first()[db.t0.point]
        self.assertEqual(output, "POINT(1 1)")
        query = t2.polygon.st_contains(geoPoint(1,1))
        n = db(query).count()
        self.assertEqual(n, 1)
        x=t0.point.st_x()
        y=t0.point.st_y()
        point = db(t0.id).select(x, y).first()
        self.assertEqual(point[x], 1)
        self.assertEqual(point[y], 1)

    def testGeometryCase(self):
        from pydal import geoPoint, geoLine, geoPolygon
        if not IS_POSTGRESQL: return
        db = self.connect(ignore_field_case=False)
        t0 = db.define_table('t0', Field('point', 'geometry()'), Field('Point', 'geometry()'))
        t0.insert(point=geoPoint(1,1))
        t0.insert(Point=geoPoint(2,2))

    def testGisMigration(self):
        if not IS_POSTGRESQL: return
        for b in [True, False]:
            db = DAL(DEFAULT_URI, check_reserved=['all'], ignore_field_case=b)
            t0 = db.define_table('t0', Field('Point', 'geometry()'),
                Field('rname_point', 'geometry()', rname='foo'))
            db.commit()
            db.close()
            db = DAL(DEFAULT_URI, check_reserved=['all'], ignore_field_case=b)
            t0 = db.define_table('t0', Field('New_point', 'geometry()'))
            t0.drop()
            db.commit()
            db.close()


class TestSQLCustomType(DALtest):

    def testRun(self):
        db = self.connect()
        from pydal.helpers.classes import SQLCustomType
        native_double = "double"
        native_string = "string"
        if hasattr(db._adapter, 'types'):
            native_double = db._adapter.types['double']
            native_string = db._adapter.types['string'] % {'length': 256}
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
        import zlib
        compressed = SQLCustomType(
             type ='text',
             native='text',
             encoder =(lambda x: zlib.compress(x or '', 1)),
             decoder = (lambda x: zlib.decompress(x))
        )
        t1=db.define_table('t0',Field('cdata', compressed))
        #r_id=t1.insert(cdata="car")
        #row=db(t1.id == r_id).select(t1.ALL).first()
        #self.assertEqual(row['cdata'], "'car'")


class TestLazy(DALtest):

    def testRun(self):
        db = self.connect(lazy_tables=True)
        t0 = db.define_table('t0', Field('name'))
        self.assertTrue(('t0' in db._LAZY_TABLES.keys()))
        db.t0.insert(name='1')
        self.assertFalse(('t0' in db._LAZY_TABLES.keys()))

    def testLazyGetter(self):
        db = self.connect(check_reserved=None, lazy_tables=True)
        db.define_table('tt',  Field('value', 'integer'))
        db.define_table('ttt',
            Field('value', 'integer'),
            Field('tt_id', 'reference tt'),
        )
        # Force table definition
        db.ttt.value.writable=False
        idd=db.tt.insert(value=0)
        db.ttt.insert(tt_id=idd)

    def testRowNone(self):
        db = self.connect(check_reserved=None, lazy_tables=True)
        tt = db.define_table('tt',  Field('value', 'integer'))
        db.tt.insert(value=None)
        row = db(db.tt).select(db.tt.ALL).first()
        self.assertEqual(row.value, None)
        self.assertEqual(row[db.tt.value], None)
        self.assertEqual(row['tt.value'], None)
        self.assertEqual(row.get('tt.value'), None)
        self.assertEqual(row['value'], None)
        self.assertEqual(row.get('value'), None)

    def testRowExtra(self):
        db = self.connect(check_reserved=None, lazy_tables=True)
        tt = db.define_table('tt',  Field('value', 'integer'))
        db.tt.insert(value=1)
        row = db(db.tt).select('value').first()
        self.assertEqual(row.value, 1)

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


class TestUpdateInsert(DALtest):

    def testRun(self):
        db = self.connect()
        t0 = db.define_table('t0', Field('name'))
        i_id = t0.update_or_insert((t0.id == 1), name='web2py')
        u_id = t0.update_or_insert((t0.id == i_id), name='web2py2')
        self.assertTrue(i_id != None)
        self.assertTrue(u_id == None)
        self.assertTrue(db(t0).count() == 1)
        self.assertTrue(db(t0.name == 'web2py').count() == 0)
        self.assertTrue(db(t0.name == 'web2py2').count() == 1)


class TestBulkInsert(DALtest):

    def testRun(self):
        db = self.connect()
        t0 = db.define_table('t0', Field('name'))
        global ctr
        ctr = 0
        def test_after_insert(i, r):
            self.assertIsInstance(i, OpRow)
            global ctr
            ctr += 1
            return True
        t0._after_insert.append(test_after_insert)
        items = [{'name':'web2py_%s' % pos} for pos in range(0, 10, 1)]
        t0.bulk_insert(items)
        self.assertTrue(db(t0).count() == len(items))
        for pos in range(0, 10, 1):
            self.assertTrue(db(t0.name == 'web2py_%s' % pos).count() == 1)
        self.assertTrue(ctr == len(items))


class TestRecordVersioning(DALtest):

    def testRun(self):
        db = self.connect()
        db.define_table('t0', Field('name'), Field('is_active', writable=False,readable=False,default=True))
        db.t0._enable_record_versioning(archive_name='t0_archive')
        self.assertTrue('t0_archive' in db)
        i_id = db.t0.insert(name='web2py1')
        db.t0.insert(name='web2py2')
        db(db.t0.name == 'web2py2').delete()
        self.assertEqual(len(db(db.t0).select()), 1)
        self.assertEqual(db(db.t0).count(), 1)
        db(db.t0.id == i_id).update(name='web2py3')
        self.assertEqual(len(db(db.t0).select()), 1)
        self.assertEqual(db(db.t0).count(), 1)
        self.assertEqual(len(db(db.t0_archive).select()), 2)
        self.assertEqual(db(db.t0_archive).count(), 2)


@unittest.skipIf(IS_SQLITE, "Skip sqlite")
class TestConnection(unittest.TestCase):

    def testRun(self):
        # check connection is no longer active after close
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        connection = db._adapter.connection
        db.close()
        self.assertRaises(Exception, connection.commit)

        # check connection are reused with pool_size
        connections = set()
        for a in range(10):
            db2 = DAL(DEFAULT_URI, check_reserved=['all'], pool_size=5)
            c = db2._adapter.connection
            connections.add(c)
            db2.close()
        self.assertEqual(len(connections), 1)
        c = connections.pop()
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
        # Clean close if a connection is broken (closed explicity)
        for a in range(10):
            db4 = DAL(DEFAULT_URI, check_reserved=['all'], pool_size=5)
            db4._adapter.connection.close()
            db4.close()
        self.assertEqual(len(db4._adapter.POOLS[DEFAULT_URI]), 0)

class TestSerializers(DALtest):

    def testAsJson(self):
        db = self.connect()
        db.define_table('tt', Field('date_field', 'datetime'))
        db.tt.insert(date_field=datetime.datetime.now())
        rows = db().select(db.tt.ALL)
        j=rows.as_json()
        import json #standard library
        json.loads(j)

    def testSelectIterselect(self):
        db = self.connect()
        db.define_table('tt', Field('tt'))
        db.tt.insert(tt='pydal')
        methods = ['as_dict', 'as_csv', 'as_json', 'as_xml', 'as_list']
        for method in methods:
            rows = db(db.tt).select()
            rowsI = db(db.tt).iterselect()
            self.assertEqual(getattr(rows, method)(),
                             getattr(rowsI, method)(),
                             'failed %s' % method)


class TestIterselect(DALtest):

    def testRun(self):
        db = self.connect()
        t0 = db.define_table('t0', Field('name'))
        names = ['web2py', 'pydal', 'Massimo']
        for n in names:
            t0.insert(name=n)

        rows = db(db.t0).select(orderby=db.t0.id)
        for pos, r in enumerate(rows):
            self.assertEqual(r.name, names[pos])
        # Testing basic iteration
        rows = db(db.t0).iterselect(orderby=db.t0.id)
        for pos, r in enumerate(rows):
            self.assertEqual(r.name, names[pos])
        # Testing IterRows.first before basic iteration
        rows = db(db.t0).iterselect(orderby=db.t0.id)
        self.assertEqual(rows.first().name, names[0])
        self.assertEqual(rows.first().name, names[0])

        for pos, r in enumerate(rows):
            self.assertEqual(r.name, names[pos])
        # Testing IterRows.__nonzero__ before basic iteration
        rows = db(db.t0).iterselect(orderby=db.t0.id)
        if rows:
            for pos, r in enumerate(rows):
                self.assertEqual(r.name, names[pos])

        # Empty iterRows
        rows = db(db.t0.name=="IterRows").iterselect(orderby=db.t0.id)
        self.assertEqual(bool(rows), False)
        for pos, r in enumerate(rows):
            self.assertEqual(r.name, names[pos])

        # Testing IterRows.__getitem__
        rows = db(db.t0).iterselect(orderby=db.t0.id)
        self.assertEqual(rows[0].name, names[0])
        self.assertEqual(rows[1].name, names[1])
        # recall the same item
        self.assertEqual(rows[1].name, names[1])
        self.assertEqual(rows[2].name, names[2])
        self.assertRaises(IndexError, rows.__getitem__, 1)

        # Testing IterRows.next()
        rows = db(db.t0).iterselect(orderby=db.t0.id)
        for n in names:
            self.assertEqual(next(rows).name, n)
        self.assertRaises(StopIteration, next, rows)

        # Testing IterRows.compact
        rows = db(db.t0).iterselect(orderby=db.t0.id)
        rows.compact = False
        for n in names:
            self.assertEqual(next(rows).t0.name, n)

    @unittest.skipIf(IS_MSSQL, "Skip mssql")
    def testMultiSelect(self):
        # Iterselect holds the cursors until all elemets have been evaluated
        # inner queries use new cursors
        db = self.connect()
        t0 = db.define_table('t0', Field('name'), Field('name_copy'))
        db(db.t0).delete()
        db.commit()
        names = ['web2py', 'pydal', 'Massimo']
        for n in names:
            t0.insert(name=n)
        c = 0
        for r in db(db.t0).iterselect():
            db.t0.update_or_insert(db.t0.id == r.id, name_copy = r.name)
            c += 1

        self.assertEqual(c, len(names), "The iterator is not looping over all elements")
        self.assertEqual(db(db.t0).count(), len(names))
        c = 0
        for x in db(db.t0).iterselect(orderby=db.t0.id):
            for y in db(db.t0).iterselect(orderby=db.t0.id):
                db.t0.update_or_insert(db.t0.id == x.id, name_copy = x.name)
                c += 1

        self.assertEqual(c, len(names)*len(names))
        self.assertEqual(db(db.t0).count(), len(names))
        db._adapter.test_connection()

    @unittest.skipIf(IS_SQLITE | IS_MSSQL, "Skip sqlite & ms sql")
    def testMultiSelectWithCommit(self):
        db = self.connect()
        t0 = db.define_table('t0', Field('nn', 'integer'))
        for n in xrange(1, 100, 1):
            t0.insert(nn=n)
        db.commit()
        s = db.t0.nn.sum()
        tot = db(db.t0).select(s).first()[s]
        c = 0
        for r in db(db.t0).iterselect(db.t0.ALL):
            db.t0.update_or_insert(db.t0.id == r.id, nn = r.nn * 2)
            db.commit()
            c += 1

        self.assertEqual(c, db(db.t0).count())
        self.assertEqual(tot * 2, db(db.t0).select(s).first()[s])

        db._adapter.test_connection()

if __name__ == '__main__':
    unittest.main()
    tearDownModule()
