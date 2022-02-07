import unittest
from pydal import DAL, Field
from pydal.tools.tags import Tags


class TestTags(unittest.TestCase):
    def test_tags(self):
        db = DAL("sqlite:memory")
        db.define_table("thing", Field("name"))
        properties = Tags(db.thing)
        id1 = db.thing.insert(name="chair")
        id2 = db.thing.insert(name="table")
        properties.add(id1, "color/red")
        properties.add(id1, "style/modern")
        properties.add(id2, "color/green")
        properties.add(id2, "material/wood")

        self.assertTrue(properties.get(id1), ["color/red", "style/modern"])
        self.assertTrue(properties.get(id2), ["color/green", "material/wood"])

        rows = db(properties.find(["style/modern"])).select()
        self.assertTrue(rows.first().id, id1)

        rows = db(properties.find(["material/wood"])).select()
        self.assertTrue(rows.first().id, id1)

        rows = db(properties.find(["color"])).select()
        self.assertTrue(len(rows), 2)
