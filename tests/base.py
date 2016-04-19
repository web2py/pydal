# -*- coding: utf-8 -*-

from ._compat import unittest
from ._adapt import DEFAULT_URI, drop, IS_MSSQL, IS_IMAP, IS_GAE
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

        parse = lambda v: db._adapter.parser.parse(v, 'datetime')

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
