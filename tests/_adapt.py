import os

DEFAULT_URI = os.getenv('DB', 'sqlite:memory')
NOSQL = any([name in DEFAULT_URI for name in ("datastore", "mongodb", "imap")])
IS_IMAP = "imap" in DEFAULT_URI


def drop(table, cascade=None):
    # mongodb implements drop()
    # although it seems it does not work properly
    if NOSQL:
        # GAE drop/cleanup is not implemented
        db = table._db
        db(table).delete()
        del db[table._tablename]
        del db.tables[db.tables.index(table._tablename)]
        db._remove_references_to(table)
    else:
        if cascade:
            table.drop(cascade)
        else:
            table.drop()
