from ._compat import unittest
from ._adapt import DEFAULT_URI, IS_GAE, IS_IMAP, drop
from pydal._compat import integer_types
from pydal import DAL, Field
from pydal.helpers.methods import smart_query


@unittest.skipIf(IS_IMAP, "Skip nosql")
class TestSmartQuery(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=["all"])

        # -----------------------------------------------------------------------------
        # Seems further imports are required for the commented field types below

        # db.define_table('referred_table',
        #                 Field('represent_field', 'string'))
        # NOTE : Don't forget to uncomment the line # drop(db.referred_table) at the very end below
        #        if the above are uncommented

        db.define_table(
            "a_table",
            Field("string_field", "string"),
            Field("text_field", "text"),
            Field("boolean_field", "boolean"),
            Field("integer_field", "integer"),
            Field("double_field", "double"),
            # Field('decimal_field', 'decimal'),
            # Field('date_field', 'date'),
            # Field('time_field', 'time'),
            # Field('datetime_field', 'datetime'),
            # Field('reference_field', 'reference referred_table'),
            # Field('list_string_field', 'list:string'),
            # Field('list_integer_field', 'list:integer'),
            # Field('list_reference_field', 'list:reference referred_table')
        )

        fields = [
            db.a_table.id,
            db.a_table.string_field,
            db.a_table.text_field,
            db.a_table.boolean_field,
            db.a_table.integer_field,
            db.a_table.double_field,
            # db.a_table.decimal_field,
            # db.a_table.date_field,
            # db.a_table.time_field,
            # db.a_table.reference_field,
            # db.a_table.list_string_field,
            # db.a_table.list_integer_field,
            # db.a_table.list_reference_field
        ]
        # -----------------------------------------------------------------------------

        # -----------------------------------------------------------------------------
        # Test with boolean field
        # Operator under test
        # operators = \
        #         [(' starts with ','startswith'),
        #          (' ends with ','endswith'),
        #          ('contains', 'N/A'),
        #          ('like', 'N/A')
        #          ]
        #
        #

        keywords = "a_table.boolean_field = True"
        q = db.a_table.boolean_field == True
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        if not IS_GAE:
            # Test string field query
            # starts with
            keywords = 'a_table.string_field starts with "pydal"'
            q = db.a_table.string_field.startswith("pydal")
            smart_q = smart_query(fields, keywords)
            self.assertEqual(smart_q, q)

            # ends with
            keywords = 'a_table.string_field ends with "Rocks!!"'
            q = db.a_table.string_field.endswith("Rocks!!")
            smart_q = smart_query(fields, keywords)
            self.assertEqual(smart_q, q)

            # contains
            keywords = 'a_table.string_field contains "Rocks"'
            q = db.a_table.string_field.contains("Rocks")
            smart_q = smart_query(fields, keywords)
            self.assertEqual(smart_q, q)

        # Don't work for some reason
        # # like
        # keywords = 'a_table.string_field like "%Rocks%"'
        # q = (db.a_table.string_field.like('%Rocks%'))
        # smart_q = smart_query(fields, keywords)
        # self.assertTrue(smart_q == q)
        # -----------------------------------------------------------------------------

        # -----------------------------------------------------------------------------
        # Tests with integer field
        # For generating these tests
        # def generate_tests():
        #     operators = \
        #         [('=', '='),
        #          ('==', '='),
        #          (' is ','='),
        #          (' equal ', '='),
        #          (' equals ', '='),
        #          (' equal to ', '='),
        #          ('<>', '!='),
        #          (' not equal ', '!='),
        #          (' not equal to ', '!='),
        #          ('<', '<'),
        #          (' less than ', '<'),
        #          ('<=', '<='),
        #          ('=<', '<='),
        #          (' less or equal ', '<='),
        #          (' less or equal than ', '<='),
        #          (' equal or less ', '<='),
        #          (' equal or less than ', '<='),
        #          ('>', '>'),
        #          (' greater than ', '>'),
        #          ('=>', '>='),
        #          ('>=', '>='),
        #          (' greater or equal ', '>='),
        #          (' greater or equal than ', '>='),
        #          (' equal or greater ', '>='),
        #          (' equal or greater than ', '>=')]  # JUST APPEND MORE OPERATORS HERE
        #
        #     for op in operators:
        #         print """
        #         # {op}
        #         keywords = 'a_table.integer_field {test_op} 1'
        #         q = (db.a_table.integer_field {result_op} 1)
        #         smart_q = smart_query(fields, keywords)
        #         self.assertTrue(smart_q == q)""".format(op=op,
        #                                                 test_op=op[0],
        #                                                 result_op='==' if op[1] == '=' else op[1])

        # ('=', '=')
        keywords = "a_table.integer_field = 1"
        q = db.a_table.integer_field == 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # ('==', '=')
        keywords = "a_table.integer_field == 1"
        q = db.a_table.integer_field == 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' is ','=')
        keywords = "a_table.integer_field is 1"
        q = db.a_table.integer_field == 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' equal ', '=')
        keywords = "a_table.integer_field  equal  1"
        q = db.a_table.integer_field == 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' equals ', '=')
        keywords = "a_table.integer_field  equals  1"
        q = db.a_table.integer_field == 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' equal to ', '=')
        keywords = "a_table.integer_field  equal to  1"
        q = db.a_table.integer_field == 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # This one not allow over integer it seems
        # # ('<>', '!=')
        # keywords = 'a_table.integer_field <> 1'
        # q = (db.a_table.integer_field != 1)
        # smart_q = smart_query(fields, keywords)
        # self.assertTrue(smart_q == q)

        # (' not equal ', '!=')
        keywords = "a_table.integer_field  not equal  1"
        q = db.a_table.integer_field != 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' not equal to ', '!=')
        keywords = "a_table.integer_field  not equal to  1"
        q = db.a_table.integer_field != 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # ('<', '<')
        keywords = "a_table.integer_field < 1"
        q = db.a_table.integer_field < 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' less than ', '<')
        keywords = "a_table.integer_field  less than  1"
        q = db.a_table.integer_field < 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # ('<=', '<=')
        keywords = "a_table.integer_field <= 1"
        q = db.a_table.integer_field <= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # This one is invalid, maybe we should remove it from smart_query
        # # ('=<', '<=')
        # keywords = 'a_table.integer_field =< 1'
        # q = (db.a_table.integer_field <= 1)
        # smart_q = smart_query(fields, keywords)
        # self.assertTrue(smart_q == q)

        # (' less or equal ', '<=')
        keywords = "a_table.integer_field  less or equal  1"
        q = db.a_table.integer_field <= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' less or equal than ', '<=')
        keywords = "a_table.integer_field  less or equal than  1"
        q = db.a_table.integer_field <= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' equal or less ', '<=')
        keywords = "a_table.integer_field  equal or less  1"
        q = db.a_table.integer_field <= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' equal or less than ', '<=')
        keywords = "a_table.integer_field  equal or less than  1"
        q = db.a_table.integer_field <= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # ('>', '>')
        keywords = "a_table.integer_field > 1"
        q = db.a_table.integer_field > 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' greater than ', '>')
        keywords = "a_table.integer_field  greater than  1"
        q = db.a_table.integer_field > 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # This one is invalid, maybe we should remove it from smart_query
        # # ('=>', '>=')
        # keywords = 'a_table.integer_field => 1'
        # q = (db.a_table.integer_field >= 1)
        # smart_q = smart_query(fields, keywords)
        # self.assertTrue(smart_q == q)

        # ('>=', '>=')
        keywords = "a_table.integer_field >= 1"
        q = db.a_table.integer_field >= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' greater or equal ', '>=')
        keywords = "a_table.integer_field  greater or equal  1"
        q = db.a_table.integer_field >= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' greater or equal than ', '>=')
        keywords = "a_table.integer_field  greater or equal than  1"
        q = db.a_table.integer_field >= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' equal or greater ', '>=')
        keywords = "a_table.integer_field  equal or greater  1"
        q = db.a_table.integer_field >= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)

        # (' equal or greater than ', '>=')
        keywords = "a_table.integer_field  equal or greater than  1"
        q = db.a_table.integer_field >= 1
        smart_q = smart_query(fields, keywords)
        self.assertEqual(smart_q, q)
        # -----------------------------------------------------------------------------

        # -----------------------------------------------------------------------------
        # Belongs and not belongs

        # NOTE : The below tests don't works
        # Issue : https://github.com/web2py/pydal/issues/161

        # (' in ', 'belongs') -> field.belongs(1, 2, 3)
        # keywords = 'a_table.integer_field in "1, 2, 3"'
        # q = (db.a_table.integer_field.belongs([1, 2, 3]))
        # smart_q = smart_query(fields, keywords)
        # self.assertEqual(smart_q, q)

        # keywords = 'a_table.id in "1, 2, 3"'
        # q = (db.a_table.id.belongs([1, 2, 3]))
        # smart_q = smart_query(fields, keywords)
        # self.assertEqual(smart_q, q)
        #
        # # (' not in ' , 'notbelongs'),
        # keywords = 'a_table.integer_field not in "1, 2, 3"'
        # q = (~db.a_table.id.belongs([1, 2, 3]))
        # smart_q = smart_query(fields, keywords)
        # self.assertTrue(smart_q == q)

        # -----------------------------------------------------------------------------
        # cleanup table
        drop(db.a_table)
        # drop(db.referred_table)
        # -----------------------------------------------------------------------------


if __name__ == "__main__":
    unittest.main()
