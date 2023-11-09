from pydal import DAL, Field

from ._adapt import DEFAULT_URI, IS_POSTGRESQL, drop
from ._compat import unittest


class TestIndexesBasic(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=["all"])
        db.define_table("tt", Field("aa"))
        rv = db.tt.create_index("idx_aa", db.tt.aa)
        self.assertTrue(rv)
        rv = db.tt.drop_index("idx_aa")
        self.assertTrue(rv)
        with self.assertRaises(Exception):
            db.tt.drop_index("idx_aa")
        db.rollback()
        drop(db.tt)


@unittest.skipUnless(IS_POSTGRESQL, "Expressions in indexes are not supported")
class TestIndexesExpressions(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=["all"], entity_quoting=True)
        db.define_table("tt", Field("aa"), Field("bb", "datetime"))
        sql = db._adapter.dialect.create_index(
            "idx_aa_and_bb", db.tt, [db.tt.aa, db.tt.bb.coalesce(None)]
        )
        with db._adapter.index_expander():
            coalesce_sql = str(db.tt.bb.coalesce(None))
        expected_sql = "CREATE INDEX %s ON %s (%s,%s);" % (
            db._adapter.dialect.quote("idx_aa_and_bb"),
            db.tt.sql_shortref,
            db.tt.aa.sqlsafe_name,
            coalesce_sql,
        )
        self.assertEqual(sql, expected_sql)
        rv = db.tt.create_index("idx_aa_and_bb", db.tt.aa, db.tt.bb.coalesce(None))
        self.assertTrue(rv)
        rv = db.tt.drop_index("idx_aa_and_bb")
        self.assertTrue(rv)
        drop(db.tt)


@unittest.skipUnless(IS_POSTGRESQL, "Partial indexes are not supported")
class TestIndexesWhere(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=["all"], entity_quoting=True)
        db.define_table("tt", Field("aa"), Field("bb", "boolean"))
        sql = db._adapter.dialect.create_index(
            "idx_aa_f", db.tt, [db.tt.aa], where=str(db.tt.bb == False)
        )
        self.assertEqual(
            sql, 'CREATE INDEX "idx_aa_f" ON "tt" ("aa") WHERE ("tt"."bb" = \'F\');'
        )
        rv = db.tt.create_index("idx_aa_f", db.tt.aa, where=(db.tt.bb == False))
        self.assertTrue(rv)
        rv = db.tt.drop_index("idx_aa_f")
        self.assertTrue(rv)
        drop(db.tt)
