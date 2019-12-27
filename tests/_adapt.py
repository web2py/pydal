import os

DEFAULT_URI = os.getenv("DB", "sqlite:memory")
IS_IMAP = "imap" in DEFAULT_URI
IS_GAE = "datastore" in DEFAULT_URI
IS_MONGODB = "mongodb" in DEFAULT_URI
IS_POSTGRESQL = "postgres" in DEFAULT_URI
IS_SQLITE = "sqlite" in DEFAULT_URI
IS_MSSQL = "mssql" in DEFAULT_URI
IS_MYSQL = "mysql" in DEFAULT_URI
IS_TERADATA = "teradata" in DEFAULT_URI
IS_NOSQL = IS_GAE or IS_MONGODB or IS_IMAP


def drop(table, cascade=None):
    if IS_NOSQL and not IS_MONGODB:
        # GAE drop/cleanup is not implemented
        db = table._db
        db[table]._common_filter = None
        db(table).delete()
        del db[table._tablename]
        del db.tables[db.tables.index(table._tablename)]
        db._remove_references_to(table)
    else:
        if cascade:
            table.drop(cascade)
        else:
            table.drop()


def _quote(db, value):
    return db._adapter.dialect.__class__.quote_template % value
