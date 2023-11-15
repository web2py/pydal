import datetime
import functools

from .. import Field
from ..validators import *


class Tags:
    def __init__(self, table, name="default", tag_table=None):
        self.name = name
        self.table = table
        self.tag_table = tag_table or self._make_tag_table()

    def _make_tag_table(self):
        db = self.table._db
        tag_table = db.define_table(
            self.table._tablename + "_tag_" + self.name,
            Field("tagpath"),
            Field("record_id", self.table),
        )
        db.commit()
        return tag_table

    def get(self, record_id):
        tag_table = self.tag_table
        db = tag_table._db
        rows = db(tag_table.record_id == record_id).select(tag_table.tagpath)
        return [row.tagpath.strip("/") for row in rows]

    def add(self, record_id, tags):
        tag_table = self.tag_table
        db = tag_table._db
        if not isinstance(tags, list):
            tags = [tags]
        for tag in tags:
            path = "/%s/" % tag.strip("/")
            if not db(
                (tag_table.record_id == record_id) & (tag_table.tagpath == path)
            ).count():
                tag_table.insert(record_id=record_id, tagpath=path)

    def remove(self, record_id, tags):
        tag_table = self.tag_table
        db = tag_table._db
        if not isinstance(tags, list):
            tags = [tags]
        paths = ["/%s/" % tag.strip("/") for tag in tags]
        db(
            (tag_table.record_id == record_id) & (tag_table.tagpath.belongs(paths))
        ).delete()

    def find(self, tags, mode="and"):
        table = self.table
        tag_table = self.tag_table
        db = tag_table._db
        queries = []
        if not isinstance(tags, list):
            tags = [tags]
        for tag in tags:
            path = "/%s/" % tag.strip("/")
            subquery = db(tag_table.tagpath.startswith(path))._select(
                tag_table.record_id
            )
            queries.append(table.id.belongs(subquery))
        func = lambda a, b: (a & b) if mode == "and" else (a | b)
        return functools.reduce(func, queries)
