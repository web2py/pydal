# -*- coding: utf-8 -*-

"""
Tier-5 regression tests for backend-specific bug fixes.

Most of these check that the changed classes are still importable and
that the fixed methods produce the expected output. They don't open
real database connections — those tests already exist in the
cross_dialect suite.
"""

import base64
from unittest.mock import MagicMock

from ._compat import unittest


class TestRepresenterBugFixes(unittest.TestCase):

    def test_db2_blob_uses_real_bytes(self):
        # Previously DB2Representer called ``base64.b64encode(str(obj))``
        # which raises TypeError in Py3. Verify it now accepts bytes
        # and produces a valid base64 string.
        from pydal.representers.db2 import DB2Representer

        # Avoid full adapter wiring — bypass __init__ and just exercise
        # the exceptions hook.
        rep = DB2Representer.__new__(DB2Representer)
        rendered = rep.exceptions(b"hello world", "blob")
        self.assertTrue(rendered.startswith("BLOB('"))
        # Decode the base64 portion back and verify it's the original.
        b64 = rendered[len("BLOB('"):-len("')")]
        self.assertEqual(base64.b64decode(b64), b"hello world")

    def test_db2_blob_accepts_string(self):
        # to_bytes() coerces str → bytes, so str input should also work
        # rather than raising as before.
        from pydal.representers.db2 import DB2Representer

        rep = DB2Representer.__new__(DB2Representer)
        rendered = rep.exceptions("hi", "blob")
        self.assertIn("BLOB(", rendered)

    def test_mssql_geography_arg_order(self):
        # Previously _geography used (srid, value) — wrong order. The
        # WKT must end up inside the quotes; the SRID must follow.
        from pydal.representers.mssql import MSSQLRepresenter

        rep = MSSQLRepresenter.__new__(MSSQLRepresenter)
        # The method is wrapped by ``for_type``; reach the underlying
        # function via ``.f`` and call it manually.
        rendered = MSSQLRepresenter._geography.f(rep, "POINT(1 2)", 4326)
        self.assertEqual(
            rendered,
            "geography::STGeomFromText('POINT(1 2)',4326)",
        )


class TestAdapterBugFixes(unittest.TestCase):

    def test_ingres_class_attribute_renamed(self):
        # SEQNAME → INGRES_SEQNAME (was a real AttributeError bug).
        from pydal.dialects.ingres import IngresDialect

        self.assertTrue(hasattr(IngresDialect, "INGRES_SEQNAME"))
        self.assertFalse(hasattr(IngresDialect, "SEQNAME"))

    def test_informix_has_regex_uri(self):
        # Informix used to reference self.REGEX_URI without defining
        # it — connection would AttributeError. Verify it's now a
        # compiled regex.
        from pydal.adapters.informix import Informix

        self.assertTrue(hasattr(Informix, "REGEX_URI"))
        # Should successfully match a sensible URI.
        m = Informix.REGEX_URI.match("user:pw@host/dbname")
        self.assertIsNotNone(m)
        self.assertEqual(m.group("user"), "user")
        self.assertEqual(m.group("password"), "pw")
        self.assertEqual(m.group("host"), "host")
        self.assertEqual(m.group("db"), "dbname")


class TestDialectSelectSignatures(unittest.TestCase):

    def test_ingres_dialect_select_accepts_with_cte(self):
        # Three real bugs got fixed here:
        # - IngresDialect.select missing ``with_cte`` param.
        # - ``%S`` format-string typo (should be ``%s``).
        # - InformixDialect.select / InformixSEDialect.select / SAPDB
        #   were also missing ``with_cte``.
        # Check the signatures expose ``with_cte``.
        import inspect

        from pydal.dialects.informix import (
            InformixDialect,
            InformixSEDialect,
        )
        from pydal.dialects.ingres import IngresDialect
        from pydal.dialects.sap import SAPDBDialect

        for dialect in (
            IngresDialect,
            InformixDialect,
            InformixSEDialect,
            SAPDBDialect,
        ):
            sig = inspect.signature(dialect.select)
            self.assertIn(
                "with_cte", sig.parameters,
                "%s.select missing with_cte" % dialect.__name__,
            )


class TestDocstringCoverage(unittest.TestCase):
    """
    Sanity check: every public adapter / dialect / parser / representer
    module now exposes a module docstring.
    """

    def test_all_backend_modules_have_module_docstring(self):
        import ast
        import os

        skip = {"mongo.py", "google.py", "couchdb.py"}
        roots = [
            "pydal/adapters",
            "pydal/dialects",
            "pydal/parsers",
            "pydal/representers",
        ]
        offenders = []
        for root in roots:
            for fname in sorted(os.listdir(root)):
                if not fname.endswith(".py") or fname in skip:
                    continue
                path = os.path.join(root, fname)
                with open(path) as f:
                    tree = ast.parse(f.read(), path)
                if not ast.get_docstring(tree):
                    offenders.append(path)
        self.assertEqual(offenders, [], "Missing module docstrings: %s" % offenders)
