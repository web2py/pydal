import datetime
import functools
from .. import Field, Field
from ..validators import *


class Tags:
    def __init__(self, table, name="default"):
        self.table = table
        db = table._db
        self.tag_table = db.define_table(
            table._tablename + "_tag_" + name, Field("path"), Field("record_id", table)
        )
        db.commit()

    def get(self, record_id):
        tag_table = self.tag_table
        db = tag_table._db
        rows = db(tag_table.record_id == record_id).select(tag_table.path)
        return [row.path.strip("/") for row in rows]

    def add(self, record_id, tags):
        tag_table = self.tag_table
        db = tag_table._db
        if not isinstance(tags, list):
            tags = [tags]
        for tag in tags:
            path = "/%s/" % tag.strip("/")
            if not db(tag_table.record_id == record_id)(tag_table.path == path).count():
                tag_table.insert(record_id=record_id, path=path)

    def remove(self, record_id, tags):
        tag_table = self.tag_table
        db = tag_table._db
        if not isinstance(tags, list):
            tags = [tags]
        paths = ["/%s/" % tag.strip("/") for tag in tags]
        db(tag_table.record_id == record_id)(tag_table.path.belongs(paths)).delete()

    def find(self, tags, mode="and"):
        table = self.table
        tag_table = self.tag_table
        db = tag_table._db
        queries = []
        if not isinstance(tags, list):
            tags = [tags]
        for tag in tags:
            path = "/%s/" % tag.strip("/")
            subquery = db(tag_table.path.startswith(path))._select(tag_table.record_id)
            queries.append(table.id.belongs(subquery))
        func = lambda a, b: (a & b) if mode == "and" else (a | b)
        return functools.reduce(func, queries)
