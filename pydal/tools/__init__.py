"""
pydal optional tools.

* ``scheduler`` — a minimal cron-style task scheduler with task
  state persistence in a DAL-managed table.
* ``tags`` — a lightweight many-to-one tagging extension that can be
  attached to any pydal table without modifying the schema.

Both are optional convenience layers; nothing in pydal core imports
from here.
"""
