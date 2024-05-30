import os
import re
import tempfile

from pydal import DAL, Field
from pydal._compat import integer_types

from ._adapt import DEFAULT_URI, IS_IMAP, IS_NOSQL, drop
from ._compat import unittest

long = integer_types[-1]

regex_isint = re.compile(r"^[+-]?\d+$")


def range_error_message(error_message, what_to_enter, minimum, maximum):
    "build the error message for the number range validators"
    if error_message is None:
        error_message = "Enter " + what_to_enter
        if minimum is not None and maximum is not None:
            error_message += " between %(min)g and %(max)g"
        elif minimum is not None:
            error_message += " greater than or equal to %(min)g"
        elif maximum is not None:
            error_message += " less than or equal to %(max)g"
    if type(maximum) in integer_types:
        maximum -= 1
    return error_message % dict(min=minimum, max=maximum)


class IS_INT_IN_RANGE(object):
    def __init__(self, minimum=None, maximum=None, error_message=None):
        self.minimum = int(minimum) if minimum is not None else None
        self.maximum = int(maximum) if maximum is not None else None
        self.error_message = range_error_message(
            error_message, "an integer", self.minimum, self.maximum
        )

    def __call__(self, value, record_id=None):
        if regex_isint.match(str(value)):
            v = int(value)
            if (self.minimum is None or v >= self.minimum) and (
                self.maximum is None or v < self.maximum
            ):
                return (v, None)
        return (value, self.error_message)


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestValidateAndInsert(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=["all"])
        db.define_table(
            "val_and_insert",
            Field("aa"),
            Field("bb", "integer", requires=IS_INT_IN_RANGE(1, 5)),
            Field("cc", writable=False),
        )
        rtn = db.val_and_insert.validate_and_insert(aa="test1", bb=2, cc="not-alloed")
        self.assertTrue("cc" in rtn["errors"])
        rtn = db.val_and_insert.validate_and_insert(aa="test1", bb=2)
        if IS_NOSQL:
            self.assertEqual(isinstance(rtn.get("id"), long), True)
        else:
            self.assertEqual(rtn.get("id"), 1)
        # errors should be empty
        self.assertTrue(not rtn.get("errors"))
        # this insert won't pass
        rtn = db.val_and_insert.validate_and_insert(bb="a")
        # the returned id should be None
        self.assertEqual(rtn.get("id"), None)
        # an error message should be in rtn.errors.bb
        self.assertNotEqual(rtn["errors"]["bb"], None)
        # cleanup table
        drop(db.val_and_insert)

    def testApiUpload(self):
        db = DAL(DEFAULT_URI, check_reserved=["all"])
        with tempfile.TemporaryDirectory() as tempdir:
            db.define_table(
                "thing",
                Field("name", required=True),
                Field("image", "upload", uploadfolder=tempdir),
            )
            b64 = b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
            res = db.thing.validate_and_insert(
                image=dict(filename="image", content=b64)
            )
            self.assertEqual(res, {"id": None, "errors": {"name": "required"}})
            self.assertEqual(os.listdir(tempdir), [])
            res = db.thing.validate_and_insert(
                name="something", image=dict(filename="image", content=b64)
            )
            self.assertEqual(res, {"errors": {}, "id": 1})
            thing = db.thing(res["id"])
            self.assertEqual(thing.name, "something")
            names = os.listdir(tempdir)
            self.assertEqual(len(names), 1)
            self.assertEqual(thing.image, names[0])
            content = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x04\x00\x00\x00\xb5\x1c\x0c\x02\x00\x00\x00\x0bIDATx\xdacd\xf8\x0f\x00\x01\x05\x01\x01'\x18\xe3f\x00\x00\x00\x00IEND\xaeB`\x82"
            with open(os.path.join(tempdir, names[0]), "rb") as stream:
                self.assertEqual(stream.read(), content)


@unittest.skipIf(IS_IMAP, "TODO: IMAP test")
class TestValidateUpdateInsert(unittest.TestCase):
    def testRun(self):
        db = DAL(DEFAULT_URI, check_reserved=["all"])
        t1 = db.define_table(
            "t1", Field("int_level", "integer", requires=IS_INT_IN_RANGE(1, 5))
        )
        i_response = t1.validate_and_update_or_insert((t1.int_level == 1), int_level=1)
        u_response = t1.validate_and_update_or_insert((t1.int_level == 1), int_level=2)
        e_response = t1.validate_and_update_or_insert((t1.int_level == 1), int_level=6)
        self.assertTrue(i_response["id"] != None)
        self.assertTrue(u_response["id"] != None)
        self.assertTrue(e_response.get("id") == None and e_response.get("errors"))
        self.assertEqual(len(db(t1).select()), 1)
        self.assertEqual(db(t1).count(), 1)
        self.assertEqual(db(t1.int_level == 1).count(), 0)
        self.assertEqual(db(t1.int_level == 6).count(), 0)
        self.assertEqual(db(t1.int_level == 2).count(), 1)
        drop(db.t1)
        return
