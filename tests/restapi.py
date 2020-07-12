import unittest

from pydal import DAL, Field
from pydal.validators import IS_NOT_IN_DB
from pydal.restapi import RestAPI, ALLOW_ALL_POLICY, DENY_ALL_POLICY, __version__


class TestRestAPI(unittest.TestCase):
    def setUp(self):
        db = DAL("sqlite:memory")

        db.define_table("color", Field("name", requires=IS_NOT_IN_DB(db, "color.name")))
        db.color.insert(name="red")
        db.color.insert(name="green")
        db.color.insert(name="blue")

        db.define_table("thing", Field("name"), Field("color", "reference color"))
        db.thing.insert(name="Chair", color=1)
        db.thing.insert(name="Chair", color=2)
        db.thing.insert(name="Table", color=1)
        db.thing.insert(name="Table", color=3)
        db.thing.insert(name="Lamp", color=2)

        db.define_table(
            "rel",
            Field("a", "reference thing"),
            Field("desc"),
            Field("b", "reference thing"),
        )
        db.rel.insert(a=1, b=2, desc="is like")
        db.rel.insert(a=3, b=4, desc="is like")
        db.rel.insert(a=1, b=3, desc="is under")
        db.rel.insert(a=2, b=4, desc="is under")
        db.rel.insert(a=5, b=4, desc="is above")

        api = RestAPI(db, ALLOW_ALL_POLICY)

        self.db = db
        self.api = api

    def test_search(self):
        api = self.api
        api.policy = ALLOW_ALL_POLICY

        self.assertEqual(
            api.search("rel", {"@limit": 0, "@model": "true"}),
            {
                "items": [],
                "count": 5,
                "model": [
                    {
                        "name": "id",
                        "label": "Id",
                        "default": None,
                        "type": "id",
                        "regex": "[1-9]\\d*",
                        "required": False,
                        "unique": False,
                        "post_writable": True,
                        "put_writable": True,
                        "options": None,
                        "referenced_by": [],
                    },
                    {
                        "name": "a",
                        "label": "A",
                        "default": None,
                        "type": "reference",
                        "references": "thing",
                        "regex": None,
                        "required": False,
                        "unique": False,
                        "post_writable": True,
                        "put_writable": True,
                        "options": None,
                    },
                    {
                        "name": "desc",
                        "label": "Desc",
                        "default": None,
                        "type": "string",
                        "regex": None,
                        "required": False,
                        "unique": False,
                        "post_writable": True,
                        "put_writable": True,
                        "options": None,
                    },
                    {
                        "name": "b",
                        "label": "B",
                        "default": None,
                        "type": "reference",
                        "references": "thing",
                        "regex": None,
                        "required": False,
                        "unique": False,
                        "post_writable": True,
                        "put_writable": True,
                        "options": None,
                    },
                ],
            },
        )
        self.assertEqual(
            api.search("color", {"name.eq": "red"}),
            {"count": 1, "items": [{"id": 1, "name": "red"}]},
        )
        self.assertEqual(
            api.search("thing", {"name.eq": "Chair"}),
            {
                "count": 2,
                "items": [
                    {"name": "Chair", "color": 1, "id": 1},
                    {"name": "Chair", "color": 2, "id": 2},
                ],
            },
        )
        self.assertEqual(
            api.search("rel[a,b]", {"desc.eq": "is like"}),
            {"count": 2, "items": [{"b": 2, "a": 1}, {"b": 4, "a": 3}]},
        )
        self.assertEqual(
            api.search("thing[name]", {"color.name.eq": "red"}),
            {"count": 2, "items": [{"name": "Chair"}, {"name": "Table"}]},
        )
        self.assertEqual(
            api.search("thing[name]", {"not.color.name.eq": "red"}),
            {
                "count": 3,
                "items": [{"name": "Chair"}, {"name": "Table"}, {"name": "Lamp"}],
            },
        )
        self.assertEqual(
            api.search("thing[name]", {"a.rel.desc": "is above"}),
            {"count": 1, "items": [{"name": "Lamp"}]},
        )
        self.assertEqual(
            api.search("thing[name]", {"a.rel.b.name": "Table"}),
            {
                "count": 4,
                "items": [
                    {"name": "Chair"},
                    {"name": "Chair"},
                    {"name": "Table"},
                    {"name": "Lamp"},
                ],
            },
        )
        self.assertEqual(
            api.search(
                "thing[name]", {"a.rel.b.name": "Table", "a.rel.desc": "is above"}
            ),
            {"count": 1, "items": [{"name": "Lamp"}]},
        )
        self.assertEqual(
            api.search("thing", {"@lookup": "color"}),
            {
                "count": 5,
                "items": [
                    {"name": "Chair", "color": {"name": "red", "id": 1}, "id": 1},
                    {"name": "Chair", "color": {"name": "green", "id": 2}, "id": 2},
                    {"name": "Table", "color": {"name": "red", "id": 1}, "id": 3},
                    {"name": "Table", "color": {"name": "blue", "id": 3}, "id": 4},
                    {"name": "Lamp", "color": {"name": "green", "id": 2}, "id": 5},
                ],
            },
        )
        self.assertEqual(
            api.search("thing", {"@lookup": "color[name]"}),
            {
                "count": 5,
                "items": [
                    {"name": "Chair", "color": {"name": "red"}, "id": 1},
                    {"name": "Chair", "color": {"name": "green"}, "id": 2},
                    {"name": "Table", "color": {"name": "red"}, "id": 3},
                    {"name": "Table", "color": {"name": "blue"}, "id": 4},
                    {"name": "Lamp", "color": {"name": "green"}, "id": 5},
                ],
            },
        )
        self.assertEqual(
            api.search("thing", {"@lookup": "color!:color[name]"}),
            {
                "count": 5,
                "items": [
                    {"name": "Chair", "color.name": "red", "id": 1},
                    {"name": "Chair", "color.name": "green", "id": 2},
                    {"name": "Table", "color.name": "red", "id": 3},
                    {"name": "Table", "color.name": "blue", "id": 4},
                    {"name": "Lamp", "color.name": "green", "id": 5},
                ],
            },
        )
        self.assertEqual(
            api.search("thing", {"@lookup": "related:a.rel[desc]"}),
            {
                "count": 5,
                "items": [
                    {
                        "name": "Chair",
                        "related": [{"desc": "is like"}, {"desc": "is under"}],
                        "color": 1,
                        "id": 1,
                    },
                    {
                        "name": "Chair",
                        "related": [{"desc": "is under"}],
                        "color": 2,
                        "id": 2,
                    },
                    {
                        "name": "Table",
                        "related": [{"desc": "is like"}],
                        "color": 1,
                        "id": 3,
                    },
                    {"name": "Table", "related": [], "color": 3, "id": 4},
                    {
                        "name": "Lamp",
                        "related": [{"desc": "is above"}],
                        "color": 2,
                        "id": 5,
                    },
                ],
            },
        )
        self.assertEqual(
            api.search("thing", {"@lookup": "related:a.rel[desc].b[name]"}),
            {
                "count": 5,
                "items": [
                    {
                        "name": "Chair",
                        "related": [
                            {"b": {"name": "Chair"}, "desc": "is like"},
                            {"b": {"name": "Table"}, "desc": "is under"},
                        ],
                        "color": 1,
                        "id": 1,
                    },
                    {
                        "name": "Chair",
                        "related": [{"b": {"name": "Table"}, "desc": "is under"}],
                        "color": 2,
                        "id": 2,
                    },
                    {
                        "name": "Table",
                        "related": [{"b": {"name": "Table"}, "desc": "is like"}],
                        "color": 1,
                        "id": 3,
                    },
                    {"name": "Table", "related": [], "color": 3, "id": 4},
                    {
                        "name": "Lamp",
                        "related": [{"b": {"name": "Table"}, "desc": "is above"}],
                        "color": 2,
                        "id": 5,
                    },
                ],
            },
        )
        self.assertEqual(
            api.search(
                "thing",
                {
                    "@lookup": "color[name],related:a.rel[desc].b[name]",
                    "@offset": 1,
                    "@limit": 2,
                },
            ),
            {
                "items": [
                    {
                        "name": "Chair",
                        "related": [{"b": {"name": "Table"}, "desc": "is under"}],
                        "color": {"name": "green"},
                        "id": 2,
                    },
                    {
                        "name": "Table",
                        "related": [{"b": {"name": "Table"}, "desc": "is like"}],
                        "color": {"name": "red"},
                        "id": 3,
                    },
                ]
            },
        )
        self.assertEqual(
            api.search(
                "thing",
                {
                    "@lookup": "color[name],related!:a.rel[desc].b[name]",
                    "@offset": 1,
                    "@limit": 2,
                },
            ),
            {
                "items": [
                    {
                        "name": "Chair",
                        "related": [{"name": "Table", "desc": "is under"}],
                        "color": {"name": "green"},
                        "id": 2,
                    },
                    {
                        "name": "Table",
                        "related": [{"name": "Table", "desc": "is like"}],
                        "color": {"name": "red"},
                        "id": 3,
                    },
                ]
            },
        )
        self.assertEqual(
            api.search("color", {"name.contains": "ee"}),
            {"count": 1, "items": [{"id": 2, "name": "green"}]},
        )
        self.assertEqual(
            api.search("color", {"name.in": "blue,green"}),
            {
                "count": 2,
                "items": [{"id": 2, "name": "green"}, {"id": 3, "name": "blue"}],
            },
        )

    def test_REST(self):

        api = self.api
        api.policy = ALLOW_ALL_POLICY

        response = api("GET", "color", None, {"name.eq": "red"})
        del response["timestamp"]
        self.assertEqual(
            response,
            {
                "count": 1,
                "status": "success",
                "code": 200,
                "items": [{"id": 1, "name": "red"}],
                "api_version": __version__,
            },
        )
        response = api("POST", "color", post_vars={"name": "magenta"})
        del response["timestamp"]
        self.assertEqual(
            response,
            {
                "status": "success",
                "errors": {},
                "code": 200,
                "id": 4,
                "api_version": __version__,
            },
        )
        response = api("POST", "color", post_vars={"name": "magenta"})
        del response["timestamp"]
        self.assertEqual(
            response,
            {
                "status": "error",
                "errors": {"name": "Value already in database or empty"},
                "code": 422,
                "message": "Validation Errors",
                "id": None,
                "api_version": __version__,
            },
        )
        response = api("PUT", "color", 4, post_vars={"name": "Magenta"})
        del response["timestamp"]
        self.assertEqual(
            response,
            {
                "status": "success",
                "updated": 1,
                "errors": {},
                "code": 200,
                "api_version": "0.1",
                "id": 4,
            },
        )
        response = api("DELETE", "color", 4)
        del response["timestamp"]
        self.assertEqual(
            response,
            {"deleted": 1, "status": "success", "code": 200, "api_version": "0.1"},
        )

    def test_policies(self):

        api = self.api
        api.policy = DENY_ALL_POLICY

        response = api("GET", "color", None, {"name.eq": "red"})
        del response["timestamp"]
        self.assertEqual(
            response,
            {
                "status": "error",
                "message": "No policy for this object",
                "code": 401,
                "api_version": __version__,
            },
        )
