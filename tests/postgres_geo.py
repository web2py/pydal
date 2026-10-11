# -*- coding: utf-8 -*-

import json
import os

from pydal import DAL, Field, geoPoint
from pydal import ast
from pydal.ast_translate import set_to_select, to_ast
from pydal.backends.postgres import PostgresDialect, PostgresRepresenter
from pydal.compilers import PostgresCompiler, PostgresPsycoCompiler
from pydal.objects import Expression

from ._adapt import DEFAULT_URI, IS_POSTGRESQL, IS_NOSQL
from ._compat import unittest

IS_POSTGIS = IS_POSTGRESQL and os.getenv("PYDAL_TEST_POSTGIS") == "1"


@unittest.skipIf(IS_NOSQL, "PostgreSQL AST compiler is SQL-only")
class TestPostgresGeoCompiler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table("geo", Field("geom"), Field("geog"), Field("n", "integer"))
        # SQLite is used only as a cheap DSL/AST fixture. Use PostgreSQL's
        # dialect and representer so geometry literals exercise the real path.
        cls.db.geo.geom.type = "geometry(POINT,4326)"
        cls.db.geo.geog.type = "geography(POINT,4326)"
        cls.db._adapter.dialect = PostgresDialect(cls.db._adapter)
        cls.represent = PostgresRepresenter(cls.db._adapter)

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def _compiler(self, bound=False):
        compiler = PostgresPsycoCompiler if bound else PostgresCompiler
        return compiler(represent=self.represent.represent, parameterize=bound)

    def _compile(self, expr, bound=False):
        return self._compiler(bound).compile_expression(to_ast(expr))

    def test_all_operations_use_postgres_handlers_inline(self):
        g = self.db.geo.geom
        expressions = [
            (g.st_astext(), 'ST_AsText("geo"."geom")'),
            (g.st_asgeojson(6, 1), 'ST_AsGeoJSON("geo"."geom",6,1)'),
            (g.st_aswkb(), '"geo"."geom"'),
            (g.st_x(), 'ST_X("geo"."geom")'),
            (g.st_y(), 'ST_Y("geo"."geom")'),
            (
                g.st_distance(geoPoint(4, 6)),
                "ST_Distance(\"geo\".\"geom\",ST_GeomFromText('POINT (4.000000 6.000000)',4326))",
            ),
            (
                g.st_simplify(0.5),
                'ST_Simplify("geo"."geom",0.5)',
            ),
            (
                g.st_simplifypreservetopology(0.5),
                'ST_SimplifyPreserveTopology("geo"."geom",0.5)',
            ),
            (g.st_transform(3857), 'ST_Transform("geo"."geom",3857)'),
            (g.st_transform("+proj=longlat"), 'ST_Transform("geo"."geom",\'+proj=longlat\')'),
            (g.st_contains(geoPoint(1, 2)), "ST_Contains(\"geo\".\"geom\",ST_GeomFromText('POINT (1.000000 2.000000)',4326))"),
            (g.st_equals(geoPoint(1, 2)), "ST_Equals(\"geo\".\"geom\",ST_GeomFromText('POINT (1.000000 2.000000)',4326))"),
            (g.st_intersects(geoPoint(1, 2)), "ST_Intersects(\"geo\".\"geom\",ST_GeomFromText('POINT (1.000000 2.000000)',4326))"),
            (g.st_overlaps(geoPoint(1, 2)), "ST_Overlaps(\"geo\".\"geom\",ST_GeomFromText('POINT (1.000000 2.000000)',4326))"),
            (g.st_touches(geoPoint(1, 2)), "ST_Touches(\"geo\".\"geom\",ST_GeomFromText('POINT (1.000000 2.000000)',4326))"),
            (g.st_within(geoPoint(1, 2)), "ST_Within(\"geo\".\"geom\",ST_GeomFromText('POINT (1.000000 2.000000)',4326))"),
            (
                g.st_dwithin(geoPoint(1, 2), 2.5),
                "ST_DWithin(\"geo\".\"geom\",ST_GeomFromText('POINT (1.000000 2.000000)',4326),2.5)",
            ),
        ]
        for expression, expected in expressions:
            self.assertEqual(self._compile(expression), expected)

    def test_bound_scalars_keep_argument_order_and_geo_literals_inline(self):
        g = self.db.geo.geom
        expr = (
            g.st_dwithin(geoPoint(1, 2), 2.5)
            & (g.st_transform(3857).st_asgeojson(7, 1) == "unused")
        )
        sql = self._compile(expr, bound=True)
        self.assertEqual(
            sql.params,
            (2.5, 3857, 7, 1, "unused"),
        )
        self.assertEqual(
            str(sql),
            '(ST_DWithin("geo"."geom",ST_GeomFromText(\'POINT (1.000000 2.000000)\',4326),%s) AND (ST_AsGeoJSON(ST_Transform("geo"."geom",%s),%s,%s) = %s))',
        )

    def test_geography_type_is_preserved_for_second_operand(self):
        expr = self.db.geo.geog.st_dwithin(geoPoint(1, 2), 0.1)
        sql = self._compile(expr)
        self.assertIn("ST_GeogFromText('SRID=4326;POINT (1.000000 2.000000)')", sql)
        self.assertNotIn("ST_GeomFromText", sql)

    def test_gis_ast_keeps_scalar_and_geometry_types_distinct(self):
        g = self.db.geo.geom
        distance = to_ast(g.st_dwithin(geoPoint(1, 2), 2.5))
        self.assertEqual(distance.args[1].type, "geometry(POINT,4326)")
        self.assertEqual(distance.args[2].type, "double")

        simplify = to_ast(g.st_simplify(0.5))
        self.assertEqual(simplify.right.type, "double")

        transform_srid = to_ast(g.st_transform(3857))
        transform_proj4 = to_ast(g.st_transform("+proj=longlat"))
        self.assertEqual(transform_srid.right.type, "integer")
        self.assertEqual(transform_proj4.right.type, "string")

        geojson = to_ast(g.st_asgeojson(6, 1))
        self.assertEqual([arg.type for arg in geojson.args[1:]], ["integer", "integer"])

    def test_st_asgeojson_rejects_malformed_shapes(self):
        g = self.db.geo.geom
        malformed = Expression(
            self.db,
            self.db._adapter.dialect.st_asgeojson,
            g,
            {"precision": 6},
            "string",
        )
        with self.assertRaises(TypeError):
            to_ast(malformed)
        with self.assertRaises(ValueError):
            self._compiler().compile_expression(
                ast.FuncCall("st_asgeojson", (to_ast(g),))
            )

    def test_plain_geo_projection_and_chained_alias_compile(self):
        g = self.db.geo.geom
        select = set_to_select(
            self.db(self.db.geo.n > 0),
            (g.st_simplify(0.5).st_astext().with_alias("shape"),),
            {},
        )
        sql = self._compiler().compile_select(select)
        self.assertIn(
            'ST_AsText(ST_Simplify("geo"."geom",0.5)) AS shape',
            sql,
        )

        projected = set_to_select(
            self.db(self.db.geo.n > 0),
            (g,),
            {},
        )
        self.assertIn('ST_AsText("geo"."geom")', self._compiler().compile_select(projected))


@unittest.skipUnless(
    IS_POSTGIS,
    "requires PostgreSQL plus PYDAL_TEST_POSTGIS=1 for PostGIS integration",
)
class TestPostGISGeoCompilerResults(unittest.TestCase):
    tablename = "pydal_postgis_ast_geo"

    @classmethod
    def setUpClass(cls):
        cls.db = DAL(DEFAULT_URI)
        cls.db.executesql("CREATE EXTENSION IF NOT EXISTS postgis")
        cls.db.executesql('DROP TABLE IF EXISTS "%s"' % cls.tablename)
        cls.db.executesql(
            'CREATE TABLE "%s" ('
            '"id" serial PRIMARY KEY, '
            '"point" geometry(POINT,4326), '
            '"other_point" geometry(POINT,4326), '
            '"polygon" geometry(POLYGON,4326), '
            '"geog" geography(POINT,4326))' % cls.tablename
        )
        cls.db.define_table(
            cls.tablename,
            Field("point", "geometry(POINT,4326)"),
            Field("other_point", "geometry(POINT,4326)"),
            Field("polygon", "geometry(POLYGON,4326)"),
            Field("geog", "geography(POINT,4326)"),
            migrate=False,
        )
        cls.db.executesql(
            'INSERT INTO "%s" ("point","other_point","polygon","geog") VALUES '
            "(ST_GeomFromText('POINT(1 2)',4326),"
            "ST_GeomFromText('POINT(4 6)',4326),"
            "ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))',4326),"
            "ST_GeogFromText('SRID=4326;POINT(1 2)'))" % cls.tablename
        )
        cls.inline = PostgresCompiler(adapter=cls.db._adapter)
        cls.bound = PostgresPsycoCompiler(adapter=cls.db._adapter)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.executesql('DROP TABLE IF EXISTS "%s"' % cls.tablename)
        finally:
            cls.db.close()

    def _value(self, expression, wrapper=""):
        sql = self.inline.compile_expression(to_ast(expression))
        if wrapper:
            query = "SELECT %s%s) FROM %s" % (wrapper, sql, self.tablename)
        else:
            query = "SELECT %s FROM %s" % (sql, self.tablename)
        return self.db.executesql(query)[0][0]

    def _bound_value(self, expression, wrapper=""):
        sql = self.bound.compile_expression(to_ast(expression))
        if wrapper:
            query = "SELECT %s%s) FROM %s" % (wrapper, sql, self.tablename)
        else:
            query = "SELECT %s FROM %s" % (sql, self.tablename)
        return self.db.executesql(query, sql.params)[0][0]

    def test_postgis_executes_all_geo_operations(self):
        t = self.db[self.tablename]
        self.assertEqual(self._value(t.point.st_astext()), "POINT(1 2)")
        self.assertEqual(self._value(t.point.st_aswkb(), "ST_GeometryType("), "ST_Point")
        self.assertEqual(self._value(t.point.st_x()), 1.0)
        self.assertEqual(self._value(t.point.st_y()), 2.0)
        self.assertEqual(self._value(t.point.st_distance(t.other_point)), 5.0)
        self.assertTrue(self._value(t.polygon.st_contains(t.point)))
        self.assertTrue(self._value(t.point.st_equals(t.point)))
        self.assertTrue(self._value(t.polygon.st_intersects(t.point)))
        self.assertTrue(self._value(t.polygon.st_overlaps(t.polygon)) is False)
        self.assertTrue(self._value(t.polygon.st_touches(t.point)) is False)
        self.assertTrue(self._value(t.point.st_within(t.polygon)))
        self.assertTrue(self._value(t.point.st_dwithin(t.other_point, 5.1)))
        self.assertEqual(
            self._value(t.point.st_simplify(0.5).st_astext()),
            "POINT(1 2)",
        )
        self.assertEqual(
            self._value(t.polygon.st_simplifypreservetopology(0.5).st_astext()),
            "POLYGON((0 0,10 0,10 10,0 10,0 0))",
        )
        self.assertEqual(
            self._value(t.point.st_transform(3857), "ST_SRID("),
            3857,
        )
        geojson = self._value(t.point.st_asgeojson(6, 1))
        self.assertEqual(json.loads(geojson)["coordinates"], [1, 2])
        self.assertTrue(self._value(t.geog.st_dwithin(geoPoint(1, 2), 0.01)))

    def test_postgis_executes_bound_scalar_arguments(self):
        t = self.db[self.tablename]
        self.assertTrue(self._bound_value(t.point.st_dwithin(t.other_point, 5.1)))
        self.assertEqual(
            self._bound_value(t.point.st_transform(3857), "ST_SRID("),
            3857,
        )
        geojson = self._bound_value(t.point.st_asgeojson(6, 1))
        self.assertEqual(json.loads(geojson)["coordinates"], [1, 2])

    def test_dal_select_count_and_geo_projection_use_ast_compiler(self):
        self.assertIsInstance(self.db._adapter.compiler, PostgresPsycoCompiler)
        t = self.db[self.tablename]
        filtered = self.db(t.point.st_dwithin(t.other_point, 5.1))
        commands = []
        driver_io = self.db._adapter.driver_io
        execute = driver_io.execute

        def capture(sql, *args, **kwargs):
            commands.append((str(sql), getattr(sql, "params", None)))
            return execute(sql, *args, **kwargs)

        driver_io.execute = capture
        try:
            rows = filtered.select(
                t.point,
                t.point.st_astext().with_alias("shape"),
                orderby=t.id,
            )
            count = filtered.count()
        finally:
            driver_io.execute = execute

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][t._tablename]["point"], "POINT(1 2)")
        self.assertEqual(rows[0]._extra["shape"], "POINT(1 2)")
        self.assertEqual(count, 1)
        self.assertEqual(len(commands), 2)
        for sql, params in commands:
            self.assertIn("ST_DWithin(", sql)
            self.assertIn("%s", sql)
            self.assertEqual(params, (5.1,))
