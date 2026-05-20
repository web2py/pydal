# -*- coding: utf-8 -*-

"""Layer-2d oracle: join/multi-table compile_select vs current path.

Covers implicit comma cross-joins, ``join=`` (INNER) with one or more
``.on()`` expressions, ``left=`` (LEFT OUTER) with one or more ``.on()``
expressions, plus join + WHERE/orderby/limit combinations.
"""

from pydal import DAL, Field
from pydal.ast_translate import set_to_select
from pydal.compilers import SQLiteCompiler

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestAstJoinsMatchesCurrentPath(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table("t1", Field("name"))
        cls.db.define_table("t2", Field("t1_id", "reference t1"), Field("x"))
        cls.db.define_table("t3", Field("t2_id", "reference t2"), Field("y"))
        cls.compiler = SQLiteCompiler(
            represent=cls.db._adapter.represent, parameterize=False
        )
        cls._restore_param = cls.db._adapter.compiler.parameterize
        cls.db._adapter.compiler.parameterize = False

    @classmethod
    def tearDownClass(cls):
        cls.db._adapter.compiler.parameterize = cls._restore_param
        cls.db.close()

    def _check(self, s, fields=(), **attrs):
        current = s._select(*fields, **attrs)
        node = set_to_select(s, fields, attrs)
        compiled = self.compiler.compile_select(node)
        self.assertEqual(compiled, current)

    # ---------- implicit multi-table (comma cross-join) ----------

    def test_implicit_two_table(self):
        self._check(
            self.db(self.db.t1.id == self.db.t2.t1_id),
            (self.db.t1.id, self.db.t2.x),
        )

    def test_implicit_three_table(self):
        self._check(
            self.db(
                (self.db.t1.id == self.db.t2.t1_id)
                & (self.db.t2.id == self.db.t3.t2_id)
            ),
            (self.db.t1.id, self.db.t2.x, self.db.t3.y),
        )

    # ---------- inner joins ----------

    def test_single_inner_join(self):
        self._check(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id, self.db.t2.x),
            join=self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
        )

    def test_two_inner_joins(self):
        self._check(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id, self.db.t2.x, self.db.t3.y),
            join=[
                self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
                self.db.t3.on(self.db.t2.id == self.db.t3.t2_id),
            ],
        )

    def test_inner_join_with_where_and_order(self):
        self._check(
            self.db(self.db.t1.name == "a"),
            (self.db.t1.id, self.db.t2.x),
            join=self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
            orderby=self.db.t2.x,
        )

    def test_inner_join_with_limit_default_orderby(self):
        # limitby + no orderby + tablemap > 1 still triggers default PK
        # ordering — make sure we match.
        self._check(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id,),
            join=self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
            limitby=(0, 5),
        )

    # ---------- left joins ----------

    def test_single_left_join(self):
        self._check(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id, self.db.t2.x),
            left=self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
        )

    def test_two_left_joins(self):
        self._check(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id, self.db.t2.x, self.db.t3.y),
            left=[
                self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
                self.db.t3.on(self.db.t2.id == self.db.t3.t2_id),
            ],
        )

    def test_left_join_with_groupby(self):
        self._check(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id, self.db.t2.id.count()),
            left=self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
            groupby=self.db.t1.id,
        )


@unittest.skipIf(IS_NOSQL, "SQL-only")
class TestAstJoinsUnsupported(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table("t1", Field("name"))
        cls.db.define_table("t2", Field("t1_id", "reference t1"))

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def test_bare_table_in_join_is_cross_join(self):
        # Bare table in join= -> CROSS JOIN (standard interpretation;
        # legacy pydal silently dropped the table here).
        node = set_to_select(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id,),
            {"join": self.db.t2},
        )
        kinds = [j.kind for j in node.joins]
        self.assertIn("cross", kinds)

    def test_bare_table_in_left_is_left_join_on_true(self):
        # Bare table in left= -> LEFT JOIN ON TRUE (unconstrained join;
        # legacy emitted invalid ``LEFT JOIN t1,t2``).
        node = set_to_select(
            self.db(self.db.t1.id > 0),
            (self.db.t1.id,),
            {"left": self.db.t2},
        )
        kinds = [j.kind for j in node.joins]
        self.assertIn("left", kinds)
        # The ON clause is a truthy literal.
        for j in node.joins:
            if j.kind == "left":
                self.assertIsNotNone(j.on)

    def test_simultaneous_join_and_left_raises(self):
        with self.assertRaises(NotImplementedError):
            set_to_select(
                self.db(self.db.t1.id > 0),
                (self.db.t1.id,),
                {
                    "join": self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
                    "left": self.db.t2.on(self.db.t1.id == self.db.t2.t1_id),
                },
            )
