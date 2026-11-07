# -*- coding: utf-8 -*-

"""PostgreSQL JSON AST translation and compiler coverage."""

from pydal import DAL, Field
from pydal.ast_translate import (
    set_to_count,
    set_to_delete,
    set_to_select,
    set_to_update,
    to_ast,
)
from pydal.backends.postgres import PostgresDialectJSON
from pydal.compilers import PostgresCompiler, PostgresPsycoCompiler
from pydal.compilers.sql import ParamSQL

from ._adapt import DEFAULT_URI, IS_NOSQL, IS_POSTGRESQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "AST compiler is SQL-only")
class TestPostgresJSONCompiler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory", migrate=False)
        cls.db._adapter.dialect = PostgresDialectJSON(cls.db._adapter)
        cls.db.define_table(
            "t",
            Field("data", "json"),
            Field("data_b", "jsonb"),
            Field("name"),
            Field("changed", "integer"),
            migrate=False,
        )
        cls.inline = PostgresCompiler(adapter=cls.db._adapter)
        cls.bound = PostgresPsycoCompiler(adapter=cls.db._adapter)

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def test_inline_matches_legacy_json_shapes(self):
        expressions = (
            (self.db.t.data.json_key("a"), '"t"."data"->\'a\''),
            (self.db.t.data.json_key(0), '"t"."data"->0'),
            (self.db.t.data.json_key_value("a"), '"t"."data"->>\'a\''),
            (self.db.t.data.json_path("{a, a1}"), '"t"."data"#>\'{a, a1}\''),
            (
                self.db.t.data.json_path_value("{a, a1}"),
                '"t"."data"#>>\'{a, a1}\'',
            ),
            (
                self.db.t.data.json_path(["a", "a1"]),
                '"t"."data"#>ARRAY[\'a\',\'a1\']::text[]',
            ),
            (
                self.db.t.data.json_contains('{"a": 1}'),
                '"t"."data"::jsonb@>\'{"a": 1}\'::jsonb',
            ),
            (
                self.db.t.data_b.json_key("a"),
                '"t"."data_b"->\'a\'',
            ),
        )
        for expression, expected in expressions:
            with self.subTest(expected=expected):
                self.assertEqual(self.inline.compile_expression(to_ast(expression)), expected)

    def test_translation_types_json_operands_and_scalar_comparisons(self):
        key = to_ast(self.db.t.data.json_key("a"))
        index = to_ast(self.db.t.data.json_key_value(0))
        path = to_ast(self.db.t.data.json_path_value("{a, a1}"))
        comparison = to_ast(self.db.t.data.json_path_value("{a}") == "foo")
        numeric_comparison = to_ast(self.db.t.data.json_key_value("a") == 1)
        lower_comparison = to_ast(
            self.db.t.data.json_path_value("{a}").lower() == "foo"
        )

        self.assertEqual(key.right.type, "string")
        self.assertEqual(index.right.type, "integer")
        self.assertEqual(path.right.type, "string")
        self.assertEqual(comparison.right.type, "string")
        self.assertEqual(numeric_comparison.right.type, "string")
        self.assertEqual(lower_comparison.right.type, "string")

    def test_json_key_rejects_invalid_public_values(self):
        for operation in ("json_key", "json_key_value"):
            for value in (1.5, None, ["a"], {"a": 1}):
                with self.subTest(operation=operation, value=value):
                    expression = getattr(self.db.t.data, operation)(value)
                    with self.assertRaises(TypeError):
                        to_ast(expression)

    def test_json_path_list_is_bound_as_text_array(self):
        expression = self.db.t.data.json_path(["a", "it's"])
        inline = self.inline.compile_expression(to_ast(expression))
        bound = self.bound.compile_expression(to_ast(expression))

        self.assertEqual(inline, '"t"."data"#>ARRAY[\'a\',\'it\'\'s\']::text[]')
        self.assertEqual(bound.params, (["a", "it's"],))
        self.assertIn("#>%s::text[]", bound)

    def test_empty_json_path_list_has_an_explicit_array_type(self):
        expression = self.db.t.data.json_path([])
        inline = self.inline.compile_expression(to_ast(expression))
        bound = self.bound.compile_expression(to_ast(expression))

        self.assertEqual(inline, '"t"."data"#>ARRAY[]::text[]')
        self.assertEqual(bound.params, ([],))

    def test_parameterized_json_values_are_typed_and_ordered(self):
        expression = (
            self.db.t.data.json_key_value("a") == 1
        ) & (self.db.t.data.json_path_value("{a, a1}") == "foo")
        compiled = self.bound.compile_select(
            set_to_select(self.db(expression), (self.db.t.id,), {})
        )

        self.assertIsInstance(compiled, ParamSQL)
        self.assertEqual(compiled.params, ("a", "1", "{a, a1}", "foo"))
        self.assertEqual(compiled.count("%s"), 4)
        self.assertIn("->>%s::text", compiled)
        self.assertIn("#>>%s::text[]", compiled)

    def test_parameterized_containment_keeps_serialized_document(self):
        document = '{"country": "Peru", "percent": "%s"}'
        compiled = self.bound.compile_expression(
            to_ast(self.db.t.data.json_contains(document))
        )

        self.assertIsInstance(compiled, ParamSQL)
        self.assertEqual(compiled.params, (document,))
        self.assertIn("%s::jsonb", compiled)
        self.assertNotIn("jsonb", compiled.params[0])

    def test_special_json_text_is_bound_without_sql_interpolation(self):
        key = "it's 50% \N{SNOWMAN}"
        path = "{it's, 50%, \N{SNOWMAN}}"
        compiled = self.bound.compile_expression(
            to_ast(self.db.t.data.json_key_value(key))
        )
        path_compiled = self.bound.compile_expression(
            to_ast(self.db.t.data.json_path_value(path))
        )

        self.assertEqual(compiled.params, (key,))
        self.assertEqual(path_compiled.params, (path,))
        self.assertNotIn(key, compiled)
        self.assertNotIn(path, path_compiled)

    def test_nested_containment_parenthesizes_extracted_json(self):
        expression = self.db.t.data.json_key("a").json_contains('{"n": 1}')
        inline = self.inline.compile_expression(to_ast(expression))
        bound = self.bound.compile_expression(to_ast(expression))

        self.assertEqual(
            inline,
            '("t"."data"->\'a\')::jsonb@>\'{"n": 1}\'::jsonb',
        )
        self.assertEqual(bound.params, ("a", '{"n": 1}'))
        self.assertIn("->%s::text)::jsonb@>%s::jsonb", bound)

    def test_chained_accessors_aliases_and_null_predicates(self):
        chained = self.db.t.data.json_key("a").json_key_value("a1")
        aliased = chained.with_alias("nested_value")
        missing = self.db.t.data.json_key("missing") == None  # noqa: E711
        json_null = self.db.t.data.json_key("null") == None  # noqa: E711

        self.assertEqual(
            self.inline.compile_expression(to_ast(aliased)),
            '"t"."data"->\'a\'->>\'a1\' AS nested_value',
        )
        self.assertEqual(
            self.inline.compile_expression(to_ast(missing)),
            '("t"."data"->\'missing\' IS NULL)',
        )
        self.assertEqual(
            self.inline.compile_expression(to_ast(json_null)),
            '("t"."data"->\'null\' IS NULL)',
        )

    def test_statement_entry_points_compile_json_expressions(self):
        query = self.db.t.data.json_path_value("{a}") == "foo"
        selected = self.db(query)
        update = self.db.t._fields_and_values_for_update({"changed": 1})

        statements = (
            self.bound.compile_select(set_to_select(selected, (self.db.t.id,), {})),
            self.bound.compile_count(set_to_count(selected)),
            self.bound.compile_update(set_to_update(selected, update.op_values())),
            self.bound.compile_delete(set_to_delete(selected)),
        )
        for statement in statements:
            with self.subTest(statement=str(statement)):
                self.assertIsInstance(statement, ParamSQL)
                self.assertIn("%s::text[]", statement)
                self.assertIn("{a}", statement.params)


@unittest.skipUnless(IS_POSTGRESQL, "requires a PostgreSQL test database")
class TestPostgresJSONIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DAL(DEFAULT_URI)
        cls.table_name = "pydal_json_compiler"
        if cls.table_name in cls.db.tables:
            cls.db[cls.table_name].drop()
        cls.db.define_table(
            cls.table_name,
            Field("data", "json"),
            Field("changed", "integer"),
            migrate=True,
        )
        cls.table = cls.db[cls.table_name]
        cls.table.insert(
            data={"a": {"n": 1, "text": "foo"}, "json_null": None},
            changed=0,
        )
        cls.table.insert(
            data={"a": {}, "json_null": "value"},
            changed=0,
        )
        cls.table.insert(data=None, changed=0)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.table.drop()
        finally:
            cls.db.close()

    def test_select_projection_filter_count_update_delete(self):
        table = self.table
        projection = table.data.json_path_value("{a, text}").with_alias("value")
        select_query = self.db(table.data.json_path_value("{a, text}") == "foo")
        count_query = self.db(table.data.json_path_value("{a, n}") == 1)
        update_query = self.db(table.data.json_path_value("{a, n}") == 1)
        delete_query = self.db(table.data.json_path_value("{a, n}") == 1)
        missing_query = self.db(table.data.json_key("missing") == None)  # noqa: E711
        null_query = self.db(table.data.json_key("json_null") == None)  # noqa: E711
        non_null_query = self.db(table.data.json_key("json_null") != None)  # noqa: E711
        text_null_query = self.db(
            table.data.json_key_value("json_null") == None  # noqa: E711
        )
        text_non_null_query = self.db(
            table.data.json_key_value("json_null") != None  # noqa: E711
        )
        sql_null_query = self.db(table.data == None)  # noqa: E711
        contains_query = self.db(table.data.json_key("a").json_contains('{"n": 1}'))
        list_path_query = self.db(table.data.json_path_value(["a", "n"]) == 1)
        legacy_list_path_sql = self.db._adapter.expand(
            table.data.json_path(["a", "n"])
        )
        self.assertIn("ARRAY['a','n']", legacy_list_path_sql)

        # The column-name prepass still consults the legacy dialect for
        # aliases. Spy on both layers so successful execution proves that the
        # statement renderer itself used the AST compiler.
        dialect = self.db._adapter.dialect
        original_dialect = {
            name: getattr(dialect, name)
            for name in (
                "json_key",
                "json_key_value",
                "json_path",
                "json_path_value",
                "json_contains",
            )
        }
        compiler = self.db._adapter.compiler
        original_compiler = {
            name: getattr(compiler, name)
            for name in (
                "compile_select",
                "compile_count",
                "compile_update",
                "compile_delete",
            )
        }
        compiler_results = {name: [] for name in original_compiler}

        def legacy_json_spy(name, operation):
            def wrapper(*args, **kwargs):
                return operation(*args, **kwargs)

            return wrapper

        def compiler_spy(name, operation):
            def wrapper(*args, **kwargs):
                result = operation(*args, **kwargs)
                compiler_results[name].append(result)
                return result

            return wrapper

        for name, operation in original_dialect.items():
            setattr(dialect, name, legacy_json_spy(name, operation))
        for name, operation in original_compiler.items():
            setattr(compiler, name, compiler_spy(name, operation))
        try:
            row = select_query.select(projection).first()
            self.assertEqual(row.value, "foo")
            self.assertEqual(count_query.count(), 1)
            self.assertEqual(contains_query.count(), 1)
            self.assertEqual(missing_query.count(), 3)
            self.assertEqual(null_query.count(), 1)
            self.assertEqual(non_null_query.count(), 2)
            self.assertEqual(text_null_query.count(), 2)
            self.assertEqual(text_non_null_query.count(), 1)
            self.assertEqual(sql_null_query.count(), 1)
            self.assertEqual(list_path_query.count(), 1)
            self.assertEqual(update_query.update(changed=2), 1)
            self.assertEqual(delete_query.delete(), 1)
        finally:
            for name, operation in original_dialect.items():
                setattr(dialect, name, operation)
            for name, operation in original_compiler.items():
                setattr(compiler, name, operation)
        self.assertTrue(compiler_results["compile_select"])
        self.assertTrue(compiler_results["compile_count"])
        self.assertTrue(compiler_results["compile_update"])
        self.assertTrue(compiler_results["compile_delete"])
        self.assertTrue(any("#>>" in str(result) for result in compiler_results["compile_select"]))
        self.assertTrue(any("#>>" in str(result) for result in compiler_results["compile_count"]))
        self.assertTrue(any("@>" in str(result) for result in compiler_results["compile_count"]))
        self.assertTrue(any("#>>" in str(result) for result in compiler_results["compile_update"]))
        self.assertTrue(any("#>>" in str(result) for result in compiler_results["compile_delete"]))
        if compiler.parameterize:
            self.assertTrue(
                any(
                    ["a", "n"] in result.params
                    for result in compiler_results["compile_count"]
                    if isinstance(result, ParamSQL)
                )
            )
            self.assertTrue(
                any(
                    '{"n": 1}' in result.params
                    for result in compiler_results["compile_count"]
                    if isinstance(result, ParamSQL)
                )
            )
