"""
Lightweight tagging extension for any pydal table.

``Tags`` lets you attach hierarchical tag paths (``color/red``,
``style/modern``) to records without altering the target table's
schema — tags live in a sibling ``<tablename>_tag_<name>`` table.

Tag paths support prefix-style hierarchies: a record tagged
``color/red`` matches both ``find("color")`` and ``find("color/red")``,
because ``find`` uses ``startswith`` on the ``/<path>/`` form.

Usage::

    from pydal.tools.tags import Tags

    tags = Tags(db.thing)
    tags.add(thing_id, "color/red")
    tags.add(thing_id, ["color/red", "style/modern"])
    rows = db(tags.find(["color"])).select()
"""

import functools
from typing import List, Union

from .. import Field

# noqa: F401, F403 — validators are re-exported here for back-compat
# with code that does ``from pydal.tools.tags import *``.
from ..validators import *  # noqa: F401, F403


TagsLike = Union[str, List[str]]


class Tags:
    """
    Attach a tag-table to ``table`` and provide add/remove/find ops.

    A single target ``table`` can have multiple Tags namespaces
    distinguished by ``name`` (default ``"default"``) — useful when you
    want separate ``categories`` and ``flags`` taxonomies on the same
    table.
    """

    def __init__(self, table, name: str = "default", tag_table=None):
        """
        Bind a Tags namespace to ``table``.

        ``name`` distinguishes one tag table from another for the same
        target table. ``tag_table`` skips auto-creation; pass it when
        you've already defined the sibling table manually.
        """
        self.name = name
        self.table = table
        self.tag_table = tag_table or self._make_tag_table()

    def _make_tag_table(self):
        """Define the sibling ``<table>_tag_<name>`` table on first use."""
        db = self.table._db
        tag_table = db.define_table(
            self.table._tablename + "_tag_" + self.name,
            Field("tagpath"),
            Field("record_id", self.table),
        )
        db.commit()
        return tag_table

    def get(self, record_id: int) -> List[str]:
        """Return all tag paths attached to ``record_id`` (un-slashed)."""
        tag_table = self.tag_table
        db = tag_table._db
        rows = db(tag_table.record_id == record_id).select(tag_table.tagpath)
        return [row.tagpath.strip("/") for row in rows]

    def add(self, record_id: int, tags: TagsLike) -> None:
        """
        Attach one or more tag paths to ``record_id``.

        Idempotent: re-adding an existing tag is a no-op.
        """
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

    def remove(self, record_id: int, tags: TagsLike) -> None:
        """Detach one or more tag paths from ``record_id``."""
        tag_table = self.tag_table
        db = tag_table._db
        if not isinstance(tags, list):
            tags = [tags]
        paths = ["/%s/" % tag.strip("/") for tag in tags]
        db(
            (tag_table.record_id == record_id) & (tag_table.tagpath.belongs(paths))
        ).delete()

    def find(self, tags: TagsLike, mode: str = "and"):
        """
        Build a Query matching records that carry the given tags.

        Prefix matching: ``find("color")`` matches records with any
        tag whose path starts with ``"color/"``.

        ``mode`` is ``"and"`` (records must carry *all* tags) or
        ``"or"`` (records must carry *any* tag).
        """
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
