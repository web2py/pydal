import unittest

from pydal import DAL, Field, QueryBuilder


class TestQueryBuilder(unittest.TestCase):
    def test_query_builder(self):
        db = DAL("sqlite:memory")
        db.define_table("thing", Field("name"), Field("solid", "boolean"))
        builder = QueryBuilder(db.thing)
        query = builder.parse("name is null")
        self.assertEqual(str(query), '("thing"."name" IS NULL)')
        query = builder.parse("name is not null")
        self.assertEqual(str(query), '("thing"."name" IS NOT NULL)')
        query = builder.parse("solid is true")
        self.assertEqual(str(query), '("thing"."solid" = \'T\')')
        query = builder.parse("solid is false")
        self.assertEqual(str(query), '("thing"."solid" = \'F\')')
        query = builder.parse("name is Chair")
        self.assertEqual(str(query), '("thing"."name" = \'Chair\')')
        query = builder.parse("name is equal to Chair")
        self.assertEqual(str(query), '("thing"."name" = \'Chair\')')
        query = builder.parse("name == Chair")
        self.assertEqual(str(query), '("thing"."name" = \'Chair\')')
        query = builder.parse('name == "Chair"')
        self.assertEqual(str(query), '("thing"."name" = \'Chair\')')
        query = builder.parse('name == "Cha\\"ir"')
        self.assertEqual(str(query), '("thing"."name" = \'Cha\\"ir\')')
        query = builder.parse('name != "Chair"')
        self.assertEqual(str(query), '("thing"."name" <> \'Chair\')')
        query = builder.parse('name < "Chair"')
        self.assertEqual(str(query), '("thing"."name" < \'Chair\')')
        query = builder.parse('name > "Chair"')
        self.assertEqual(str(query), '("thing"."name" > \'Chair\')')
        query = builder.parse('name <= "Chair"')
        self.assertEqual(str(query), '("thing"."name" <= \'Chair\')')
        query = builder.parse('name >= "Chair"')
        self.assertEqual(str(query), '("thing"."name" >= \'Chair\')')
        query = builder.parse("name in Chair, Table")
        self.assertEqual(str(query), "(\"thing\".\"name\" IN ('Chair','Table'))")
        query = builder.parse("name belongs Chair, Table")
        self.assertEqual(str(query), "(\"thing\".\"name\" IN ('Chair','Table'))")
        query = builder.parse('name belongs "Chair", "Table"')
        self.assertEqual(str(query), '("thing"."name" IN (\'Chair", "Table\'))')
        query = builder.parse('name contains "Chair"')
        self.assertEqual(
            str(query), "(LOWER(\"thing\".\"name\") LIKE '%chair%' ESCAPE '\\')"
        )
        query = builder.parse('name startswith "Chair"')
        self.assertEqual(str(query), "(\"thing\".\"name\" LIKE 'Chair%' ESCAPE '\\')")
        query = builder.parse('name starts with "Chair"')
        self.assertEqual(str(query), "(\"thing\".\"name\" LIKE 'Chair%' ESCAPE '\\')")
        query = builder.parse("name lower == chair")
        self.assertEqual(str(query), '(LOWER("thing"."name") = \'chair\')')
        query = builder.parse("name lower is equal to chair")
        self.assertEqual(str(query), '(LOWER("thing"."name") = \'chair\')')
        query = builder.parse("name upper == CHAIR")
        self.assertEqual(str(query), '(UPPER("thing"."name") = \'CHAIR\')')
        query = builder.parse("not name == Chair")
        self.assertEqual(str(query), '(NOT ("thing"."name" = \'Chair\'))')

        query = builder.parse("not (name == Chair)")
        self.assertEqual(str(query), '(NOT ("thing"."name" = \'Chair\'))')

        query = builder.parse("name == Chair or name is Table")
        self.assertEqual(
            str(query), '(("thing"."name" = \'Chair\') OR ("thing"."name" = \'Table\'))'
        )

        query = builder.parse("name == Chair and not name is Table")
        self.assertEqual(
            str(query),
            '(("thing"."name" = \'Chair\') AND (NOT ("thing"."name" = \'Table\')))',
        )

        query = builder.parse("not ((name == Chair) and not (name == Table))")
        self.assertEqual(
            str(query),
            '(NOT (("thing"."name" = \'Chair\') AND (NOT ("thing"."name" = \'Table\'))))',
        )

    def test_translations(self):
        db = DAL("sqlite:memory")
        db.define_table("thing", Field("name"))
        field_aliases = {"id": "id", "nome": "name"}
        token_aliases = {"non è nullo": "is not null", "è uguale a": "=="}
        builder = QueryBuilder(
            db.thing, field_aliases=field_aliases, token_aliases=token_aliases
        )

        query = builder.parse("nome non è nullo")
        self.assertEqual(str(query), '("thing"."name" IS NOT NULL)')
        query = builder.parse("nome è uguale a Chair")
        self.assertEqual(str(query), '("thing"."name" = \'Chair\')')
