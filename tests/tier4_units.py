# -*- coding: utf-8 -*-

"""
Tier-4 unit coverage — public-surface helpers I touched / fixed.

* ``pydal.restapi`` (PolicyViolation/NotFound/InvalidFormat, maybe_call,
  trydo, error_wrapper, Policy basics, parse_table_and_fields).
* ``pydal.validators.ValidationError`` (carries message).
* ``pydal.objects.Field`` (class docstring now visible).
"""

from pydal import Field
from pydal.objects import Table
from pydal.restapi import (
    InvalidFormat,
    NotFound,
    Policy,
    PolicyViolation,
    RestAPI,
    error_wrapper,
    maybe_call,
    trydo,
)
from pydal.validators import ValidationError

from ._compat import unittest


class TestRestAPIHelpers(unittest.TestCase):

    def test_maybe_call_passes_value_through(self):
        self.assertEqual(maybe_call(42), 42)

    def test_maybe_call_calls_callable(self):
        self.assertEqual(maybe_call(lambda: "hi"), "hi")

    def test_trydo_returns_result(self):
        self.assertEqual(trydo(lambda: 7, 0), 7)

    def test_trydo_returns_default_on_exception(self):
        self.assertEqual(trydo(lambda: 1 / 0, "fallback"), "fallback")

    def test_parse_table_and_fields_simple(self):
        name, fields = RestAPI.parse_table_and_fields("person")
        self.assertEqual(name, "person")
        self.assertEqual(fields, [])

    def test_parse_table_and_fields_with_fields(self):
        name, fields = RestAPI.parse_table_and_fields("person[a,b]")
        self.assertEqual(name, "person")
        self.assertEqual(fields, ["a", "b"])

    def test_parse_table_and_fields_rejects_three_brackets(self):
        # Fixed in tier 4: was silently returning None.
        with self.assertRaises(ValueError):
            RestAPI.parse_table_and_fields("a[b][c]")


class TestErrorWrapper(unittest.TestCase):

    def test_success_path(self):
        @error_wrapper
        def fn():
            return {"items": [1, 2, 3]}

        rv = fn()
        self.assertEqual(rv["status"], "success")
        self.assertEqual(rv["code"], 200)
        self.assertIn("timestamp", rv)
        self.assertIn("api_version", rv)

    def test_policy_violation_yields_401(self):
        @error_wrapper
        def fn():
            raise PolicyViolation("nope")

        rv = fn()
        self.assertEqual(rv["status"], "error")
        self.assertEqual(rv["code"], 401)
        self.assertEqual(rv["message"], "nope")

    def test_not_found_yields_404(self):
        @error_wrapper
        def fn():
            raise NotFound("gone")

        rv = fn()
        self.assertEqual(rv["code"], 404)

    def test_invalid_format_yields_400(self):
        @error_wrapper
        def fn():
            raise InvalidFormat("bad")

        rv = fn()
        self.assertEqual(rv["code"], 400)


class TestPolicyBasics(unittest.TestCase):

    def test_rejects_unknown_method(self):
        p = Policy()
        with self.assertRaises(InvalidFormat):
            p.set("things", method="PATCH", authorize=True)

    def test_rejects_unknown_attribute(self):
        p = Policy()
        with self.assertRaises(InvalidFormat):
            p.set("things", method="GET", wibble=True)

    def test_no_policy_raises(self):
        p = Policy()
        with self.assertRaises(PolicyViolation):
            p.check_if_allowed("GET", "things")

    def test_no_policy_returns_false_with_exceptions_false(self):
        p = Policy()
        self.assertFalse(p.check_if_allowed("GET", "things", exceptions=False))

    def test_deny_via_authorize_false(self):
        p = Policy()
        p.set("things", method="GET", authorize=False)
        with self.assertRaises(PolicyViolation):
            p.check_if_allowed("GET", "things")

    def test_allow_via_authorize_true(self):
        p = Policy()
        p.set("things", method="GET", authorize=True)
        self.assertTrue(p.check_if_allowed("GET", "things"))


class TestValidationError(unittest.TestCase):

    def test_carries_message(self):
        e = ValidationError("oops")
        self.assertEqual(str(e), "oops")
        self.assertEqual(e.message, "oops")


class TestFieldDocstringNowVisible(unittest.TestCase):
    # Regression test: tier 4 moved Field's docstring above its class
    # attributes so ``Field.__doc__`` is no longer None.
    def test_field_has_docstring(self):
        self.assertIsNotNone(Field.__doc__)
        self.assertIn("Column descriptor", Field.__doc__)

    def test_table_has_docstring(self):
        self.assertIsNotNone(Table.__doc__)
        self.assertIn("database table", Table.__doc__)
