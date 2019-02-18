# -*- coding: utf-8 -*-

from ._compat import unittest
from ._adapt import DEFAULT_URI, drop, IS_MSSQL, IS_IMAP, IS_GAE, IS_TERADATA
from pydal import DAL, Field
from pydal._compat import PY2


@unittest.skipIf(IS_IMAP, "Reference not Null unsupported on IMAP")
class TestReferenceNOTNULL(unittest.TestCase):
    #1:N not null

    def testRun(self):
        for ref, bigint in [('reference', False), ('big-reference', True)]:
            db = DAL(DEFAULT_URI, check_reserved=['all'], bigint_id=bigint)
            if bigint and 'big-id' not in db._adapter.types:
                continue
            db.define_table('tt', Field('vv'))
            db.define_table('ttt', Field('vv'), Field('tt_id', '%s tt' % ref,
                                                      notnull=True))
            self.assertRaises(Exception, db.ttt.insert, vv='pydal')
            # The following is mandatory for backends as PG to close the aborted transaction
            db.commit()
            drop(db.ttt)
            drop(db.tt)
            db.close()


@unittest.skipIf(IS_IMAP, "Reference Unique unsupported on IMAP")
@unittest.skipIf(IS_GAE, "Reference Unique unsupported on GAE")
class TestReferenceUNIQUE(unittest.TestCase):
    # 1:1 relation

    def testRun(self):
        for ref, bigint in [('reference', False), ('big-reference', True)]:
            db = DAL(DEFAULT_URI, check_reserved=['all'], bigint_id=bigint)
            if bigint and 'big-id' not in db._adapter.types:
                continue
            db.define_table('tt', Field('vv'))
            db.define_table('ttt', Field('vv'),
                            Field('tt_id', '%s tt' % ref, unique=True),
                            Field('tt_uq', 'integer', unique=True))
            id_1 = db.tt.insert(vv='pydal')
            id_2 = db.tt.insert(vv='pydal')
            # Null tt_id
            db.ttt.insert(vv='pydal', tt_uq=1)
            # first insert is OK
            db.ttt.insert(tt_id=id_1, tt_uq=2)
            self.assertRaises(Exception, db.ttt.insert, tt_id=id_1, tt_uq=3)
            self.assertRaises(Exception, db.ttt.insert, tt_id=id_2, tt_uq=2)
            # The following is mandatory for backends as PG to close the aborted transaction
            db.commit()
            drop(db.ttt)
            drop(db.tt)
            db.close()


@unittest.skipIf(IS_IMAP, "Reference Unique not Null unsupported on IMAP")
@unittest.skipIf(IS_GAE, "Reference Unique not Null unsupported on GAE")
class TestReferenceUNIQUENotNull(unittest.TestCase):
    # 1:1 relation not null

    def testRun(self):
        for ref, bigint in [('reference', False), ('big-reference', True)]:
            db = DAL(DEFAULT_URI, check_reserved=['all'], bigint_id=bigint)
            if bigint and 'big-id' not in db._adapter.types:
                continue
            db.define_table('tt', Field('vv'))
            db.define_table('ttt', Field('vv'), Field('tt_id', '%s tt' % ref,
                                                      unique=True,
                                                      notnull=True))
            self.assertRaises(Exception, db.ttt.insert, vv='pydal')
            db.commit()
            id_i = db.tt.insert(vv='pydal')
            # first insert is OK
            db.ttt.insert(tt_id=id_i)
            self.assertRaises(Exception, db.ttt.insert, tt_id=id_i)
            # The following is mandatory for backends as PG to close the aborted transaction
            db.commit()
            drop(db.ttt)
            drop(db.tt)
            db.close()


@unittest.skipIf(IS_IMAP, "Skip unicode on IMAP")
@unittest.skipIf(IS_MSSQL and not PY2, "Skip unicode on py3 and MSSQL")
class TestUnicode(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('tt', Field('vv'))
        vv = 'ἀγοραζε'
        id_i = db.tt.insert(vv=vv)
        row = db(db.tt.id == id_i).select().first()
        self.assertEqual(row.vv, vv)
        db.commit()
        drop(db.tt)
        db.close()


class TestParseDateTime(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])

        #: skip for adapters that use drivers for datetime parsing
        if db._adapter.parser.registered.get('datetime') is None:
            return

        parse = lambda v: db._adapter.parser.parse(v, 'datetime', 'datetime')

        dt = parse('2015-09-04t12:33:36.223245')
        self.assertEqual(dt.microsecond, 223245)
        self.assertEqual(dt.hour, 12)

        dt = parse('2015-09-04t12:33:36.223245Z')
        self.assertEqual(dt.microsecond, 223245)
        self.assertEqual(dt.hour, 12)

        dt = parse('2015-09-04t12:33:36.223245-2:0')
        self.assertEqual(dt.microsecond, 223245)
        self.assertEqual(dt.hour, 10)

        dt = parse('2015-09-04t12:33:36+1:0')
        self.assertEqual(dt.microsecond, 0)
        self.assertEqual(dt.hour, 13)

        dt = parse('2015-09-04t12:33:36.123')
        self.assertEqual(dt.microsecond, 123000)

        dt = parse('2015-09-04t12:33:36.00123')
        self.assertEqual(dt.microsecond, 1230)

        dt = parse('2015-09-04t12:33:36.1234567890')
        self.assertEqual(dt.microsecond, 123456)
        db.close()

@unittest.skipIf(IS_IMAP, "chained join unsupported on IMAP")
@unittest.skipIf(IS_TERADATA, "chained join unsupported on TERADATA")
class TestChainedJoinUNIQUE(unittest.TestCase):
    # 1:1 relation

    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=['all'])
        db.define_table('aa',Field('name'))
        db.define_table('bb',Field('aa','reference aa'),Field('name'))
        for k in ('x','y','z'):
            i = db.aa.insert(name=k)
            for j in ('u','v','w'):
                db.bb.insert(aa=i,name=k+j)
        db.commit()
        rows = db(db.aa).select()
        rows.join(db.bb.aa, fields=[db.bb.name], orderby=[db.bb.name])
        self.assertEqual(rows[0].bb[0].name, 'xu')
        self.assertEqual(rows[0].bb[1].name, 'xv')
        self.assertEqual(rows[0].bb[2].name, 'xw')
        self.assertEqual(rows[1].bb[0].name, 'yu')
        self.assertEqual(rows[1].bb[1].name, 'yv')
        self.assertEqual(rows[1].bb[2].name, 'yw')
        self.assertEqual(rows[2].bb[0].name, 'zu')
        self.assertEqual(rows[2].bb[1].name, 'zv')
        self.assertEqual(rows[2].bb[2].name, 'zw')

        rows = db(db.bb).select()
        rows.join(db.aa.id, fields=[db.aa.name])

        self.assertEqual(rows[0].aa.name, 'x')
        self.assertEqual(rows[1].aa.name, 'x')
        self.assertEqual(rows[2].aa.name, 'x')
        self.assertEqual(rows[3].aa.name, 'y')
        self.assertEqual(rows[4].aa.name, 'y')
        self.assertEqual(rows[5].aa.name, 'y')
        self.assertEqual(rows[6].aa.name, 'z')
        self.assertEqual(rows[7].aa.name, 'z')
        self.assertEqual(rows[8].aa.name, 'z')

        rows_json = rows.as_json()
        drop(db.bb)
        drop(db.aa)
        db.close()

class TestNullAdapter(unittest.TestCase):
    # Test that NullAdapter can define tables

    def testRun(self):
        db = DAL(None)
        db.define_table('no_table', Field('aa'))
        self.assertIsInstance(db.no_table.aa, Field)
        self.assertIsInstance(db.no_table['aa'], Field)
        db.close()
