import unittest

from pydal import DAL, Field, QueryBuilder


class TestQueryBuilder(unittest.TestCase):
    def test_query_builder(self):
        db = DAL("sqlite:memory")
        db.define_table("thing", Field("name"))
        builder = QueryBuilder(db.think)
        query = builder.parse("name is null")
        self.assertEqual(str(query), '("thing"."name" IS NULL)')
        query = builder.parse("name is not null")
        self.assertEqual(str(query), '("thing"."name" IS NOT NULL)')
        query = builder.parse("name is Max")
        self.assertEqual(str(query), '("thing"."name" = \'Max\')')
        query = builder.parse("name is equal to Max")
        self.assertEqual(str(query), '("thing"."name" = \'Max\')')
        query = builder.parse("name == Max")
        self.assertEqual(str(query), '("thing"."name" = \'Max\')')
        query = builder.parse('name == "Max"')
        self.assertEqual(str(query), '("thing"."name" = \'Max\')')
        query = builder.parse('name == "Ma\\"x"')
        self.assertEqual(str(query), '("thing"."name" = \'Ma\\"x\')')
        query = builder.parse('name != "Max"')
        self.assertEqual(str(query), '("thing"."name" <> \'Max\')')
        query = builder.parse('name < "Max"')
        self.assertEqual(str(query), '("thing"."name" < \'Max\')')
        query = builder.parse('name > "Max"')
        self.assertEqual(str(query), '("thing"."name" > \'Max\')')
        query = builder.parse('name <= "Max"')
        self.assertEqual(str(query), '("thing"."name" <= \'Max\')')
        query = builder.parse('name >= "Max"')
        self.assertEqual(str(query), '("thing"."name" >= \'Max\')')
        query = builder.parse("name in Max, John")
        self.assertEqual(str(query), "(\"thing\".\"name\" IN ('John','Max'))")
        query = builder.parse("name belongs Max, John")
        self.assertEqual(str(query), "(\"thing\".\"name\" IN ('John','Max'))")
        query = builder.parse('name belongs "Max", "John"')
        self.assertEqual(str(query), '("thing"."name" IN (\'Max", "John\'))')
        query = builder.parse('name contains "Max"')
        self.assertEqual(
            str(query), "(LOWER(\"thing\".\"name\") LIKE '%max%' ESCAPE '\\')"
        )
        query = builder.parse('name startswith "Max"')
        self.assertEqual(str(query), "(\"thing\".\"name\" LIKE 'Max%' ESCAPE '\\')")
        query = builder.parse('name starts with "Max"')
        self.assertEqual(str(query), "(\"thing\".\"name\" LIKE 'Max%' ESCAPE '\\')")
        query = builder.parse("name lower == max")
        self.assertEqual(str(query), '(LOWER("thing"."name") = \'max\')')
        query = builder.parse("name lower is equal to max")
        self.assertEqual(str(query), '(LOWER("thing"."name") = \'max\')')
        query = builder.parse("name upper == MAX")
        self.assertEqual(str(query), '(UPPER("thing"."name") = \'MAX\')')
        query = builder.parse("not name == Max")
        self.assertEqual(str(query), '(NOT ("thing"."name" = \'Max\'))')

        query = builder.parse("not (name == Max)")
        self.assertEqual(str(query), '(NOT ("thing"."name" = \'Max\'))')

        query = builder.parse("name == Max or name is John")
        self.assertEqual(
            str(query), '(("thing"."name" = \'Max\') OR ("thing"."name" = \'John\'))'
        )

        query = builder.parse("name == Max and not name is John")
        self.assertEqual(
            str(query),
            '(("thing"."name" = \'Max\') AND (NOT ("thing"."name" = \'John\')))',
        )

        query = builder.parse("not ((name == Max) and not (name == John))")
        self.assertEqual(
            str(query),
            '(NOT (("thing"."name" = \'Max\') AND (NOT ("thing"."name" = \'John\'))))',
        )
