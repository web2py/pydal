# -*- coding: utf-8 -*-

"""Layer-2b oracle: compile(to_ast(e)) == adapter.expand(e), byte-exact.

This is the proof that the AST + compiler combination is lossless against
the current DialectOp-based SQL path. The current path stays the source of
truth; we just verify the new pipeline reproduces it for every expression
we care about.
"""

from pydal import DAL, Field
from pydal.ast_translate import to_ast
from pydal.compilers import SQLiteCompiler
from pydal.objects import Expression

from ._adapt import IS_NOSQL
from ._compat import unittest


@unittest.skipIf(IS_NOSQL, "SQLCompiler is SQL-only")
class TestAstCompileMatchesCurrentPath(unittest.TestCase):
    """For each expression, assert that the AST round-trip produces the
    same SQL string as the existing adapter.expand() path.
    """

    @classmethod
    def setUpClass(cls):
        cls.db = DAL("sqlite:memory")
        cls.db.define_table(
            "t",
            Field("name"),
            Field("age", "integer"),
            Field("data", "json"),
        )
        # Inline mode keeps the byte-for-byte oracle against the legacy
        # ``adapter.expand`` path. Production uses parameterize=True (set
        # as the class default on SQLiteCompiler).
        cls.compiler = SQLiteCompiler(
            represent=cls.db._adapter.represent, parameterize=False
        )
        cls._restore_param = cls.db._adapter.compiler.parameterize
        cls.db._adapter.compiler.parameterize = False

    @classmethod
    def tearDownClass(cls):
        cls.db._adapter.compiler.parameterize = cls._restore_param
        cls.db.close()

    def _check(self, expr):
        current = str(self.db._adapter.expand(expr))
        compiled = self.compiler.compile_expression(to_ast(expr))
        self.assertEqual(
            compiled,
            current,
            "AST roundtrip mismatch:\n  current : %s\n  compiled: %s"
            % (current, compiled),
        )

    # ---------- comparisons ----------

    def test_eq(self):  self._check(self.db.t.name == "x")
    def test_ne(self):  self._check(self.db.t.name != "x")
    def test_lt(self):  self._check(self.db.t.age < 5)
    def test_lte(self): self._check(self.db.t.age <= 5)
    def test_gt(self):  self._check(self.db.t.age > 5)
    def test_gte(self): self._check(self.db.t.age >= 5)
    def test_eq_none(self): self._check(self.db.t.name == None)  # noqa: E711
    def test_ne_none(self): self._check(self.db.t.name != None)  # noqa: E711

    # ---------- logical ----------

    def test_and(self):
        self._check((self.db.t.name == "x") & (self.db.t.age > 5))

    def test_or(self):
        self._check((self.db.t.age < 1) | (self.db.t.age > 99))

    def test_not(self):
        self._check(~(self.db.t.age == 5))

    def test_deep_logic(self):
        self._check(
            ~((self.db.t.name == "x") & (self.db.t.age > 5))
            | (self.db.t.name.startswith("y"))
        )

    # ---------- string match ----------

    def test_like_with_escape(self):
        self._check(self.db.t.name.like("a%", escape="!"))

    def test_like_default_escape(self):
        self._check(self.db.t.name.like("a%"))

    def test_ilike(self):
        self._check(self.db.t.name.ilike("A%"))

    def test_regexp(self):
        self._check(self.db.t.name.regexp("a+"))

    def test_startswith(self):
        self._check(self.db.t.name.startswith("foo"))

    def test_endswith(self):
        self._check(self.db.t.name.endswith("bar"))

    def test_contains_case_sensitive(self):
        self._check(self.db.t.name.contains("bar", case_sensitive=True))

    def test_contains_default(self):
        self._check(self.db.t.name.contains("bar"))

    # ---------- belongs / IN ----------

    def test_belongs_list_ints(self):
        self._check(self.db.t.age.belongs([1, 2, 3]))

    def test_belongs_list_strings(self):
        self._check(self.db.t.name.belongs(["a", "b"]))

    def test_belongs_empty(self):
        self._check(self.db.t.age.belongs([]))

    # ---------- arithmetic / order ----------

    def test_add(self): self._check((self.db.t.age + 1) > 10)
    def test_sub(self): self._check((self.db.t.age - 1) > 10)
    def test_mul(self): self._check((self.db.t.age * 2) > 10)
    def test_div(self): self._check((self.db.t.age / 2) > 10)
    def test_mod(self): self._check((self.db.t.age % 2) == 0)
    def test_invert_desc(self): self._check(~self.db.t.age)
    def test_comma(self):       self._check(self.db.t.age | self.db.t.name)

    # ---------- unary string funcs ----------

    def test_lower_eq(self): self._check(self.db.t.name.lower() == "foo")
    def test_upper_eq(self): self._check(self.db.t.name.upper() == "FOO")
    def test_len_gt(self):   self._check(self.db.t.name.len() > 0)

    # ---------- aggregates ----------

    def test_sum(self): self._check(self.db.t.age.sum())
    def test_max(self): self._check(self.db.t.age.max())
    def test_min(self): self._check(self.db.t.age.min())
    def test_avg(self): self._check(self.db.t.age.avg())
    def test_abs(self): self._check(self.db.t.age.abs())

    # ---------- multi-arg functions ----------

    def test_cast(self):       self._check(self.db.t.age.cast("double"))
    def test_extract_year(self): self._check(self.db.t.age.year())
    def test_extract_month(self): self._check(self.db.t.age.month())
    def test_replace(self):    self._check(self.db.t.name.replace("a", "b"))
    def test_coalesce(self):   self._check(self.db.t.age.coalesce(0, 1))
    def test_coalesce_zero(self): self._check(self.db.t.age.coalesce_zero())
    def test_substring_positive(self): self._check(self.db.t.name[1:3])
    def test_substring_to_end(self):   self._check(self.db.t.name[2:])
    def test_substring_negative(self): self._check(self.db.t.name[-3:])
    def test_case(self):       self._check((self.db.t.age > 0).case(1, 0))

    # ---------- alias / raw ----------

    def test_with_alias(self):
        self._check(self.db.t.age.with_alias("a"))

    def test_raw_expression(self):
        self._check(Expression(self.db, "CUSTOM_SQL"))

    def test_raw_expression_with_semicolon(self):
        self._check(Expression(self.db, "CUSTOM_SQL;"))

    # ---------- nested / sample queries ----------

    def test_realistic_filter(self):
        self._check(
            (self.db.t.name.lower() == "foo")
            & (self.db.t.age.belongs([1, 2, 3]))
            & (self.db.t.age + 1 > 10)
        )

    def test_aggregate_inside_compare(self):
        # SUM(age) > 100
        self._check(self.db.t.age.sum() > 100)
