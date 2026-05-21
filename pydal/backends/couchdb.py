"""Backend module for couchdb."""

# ============================================================
# Adapter
# ============================================================

from ..helpers.classes import SQLALL, FakeCursor
from ..helpers.methods import uuid2int
from ..objects import Field, Query
from ..backend_base import adapters
from ..backend_base import NoSQLAdapter, SQLAdapter


@adapters.register_for("couchdb")
class CouchDB(NoSQLAdapter):
    dbengine = "couchdb"
    drivers = ("couchdb",)

    uploads_in_blob = True

    def _initialize_(self):
        super(CouchDB, self)._initialize_()
        self.ruri = "http://" + self.uri[10:]
        self.db_codec = "UTF-8"

    def connector(self):
        conn = self.driver.Server(self.ruri, **self.driver_args)
        conn.cursor = lambda: FakeCursor()
        conn.close = lambda: None
        conn.commit = lambda: None
        return conn

    def create_table(self, table, migrate=True, fake_migrate=False):
        if migrate:
            try:
                self.connection.create(table._tablename)
            except:
                pass
        super(CouchDB, self).create_table(table, migrate, fake_migrate)

    def _expand(self, expression, field_type=None, query_env={}):
        if isinstance(expression, Field):
            if expression.type == "id":
                return "%s._id" % expression.tablename
        return SQLAdapter._expand(self, expression, field_type, query_env=query_env)

    def insert(self, table, fields):
        rid = uuid2int(self.db.uuid())
        ctable = self.connection[table._tablename]
        values = dict((k.name, self.represent(v, k.type)) for k, v in fields)
        values["_id"] = str(rid)
        ctable.save(values)
        return rid

    @staticmethod
    def _make_id_field(field_name):
        return field_name == "id" and "_id" or field_name

    def _select(
        self,
        query,
        fields,
        left=False,
        join=False,
        distinct=False,
        orderby=False,
        groupby=False,
        having=False,
        limitby=False,
        orderby_on_limitby=True,
        for_update=False,
        outer_scoped=[],
        required=None,
        cache=None,
        cacheable=None,
        processor=None,
    ):
        if not isinstance(query, Query):
            raise SyntaxError("Not Supported")

        new_fields = []
        for item in fields:
            if isinstance(item, SQLALL):
                new_fields += item._table
            else:
                new_fields.append(item)

        fields = new_fields
        tablename = self.get_table(query)._tablename
        fieldnames = [f.name for f in (fields or self.db[tablename])]
        colnames = ["%s.%s" % (tablename, fieldname) for fieldname in fieldnames]
        fields = ",".join(
            ["%s.%s" % (tablename, self._make_id_field(f)) for f in fieldnames]
        )
        fn = "(function(%(t)s){if(%(query)s)emit(%(order)s,[%(fields)s]);})" % dict(
            t=tablename,
            query=self.expand(query),
            order="%s._id" % tablename,
            fields=fields,
        )
        return fn, colnames

    def select(self, query, fields, attributes):
        fn, colnames = self._select(query, fields, attributes)
        tablename = colnames[0].split(".")[0]
        ctable = self.connection[tablename]
        rows = [cols["value"] for cols in ctable.query(fn)]
        processor = attributes.get("processor", self.parse)
        return processor(rows, fields, colnames, False)

    def update(self, table, query, fields):
        from ..drivers import couchdb

        if not isinstance(query, Query):
            raise SyntaxError("Not Supported")
        if query.first.type == "id" and query.op == self.dialect.eq:
            rid = query.second
            tablename = query.first.tablename
            ctable = self.connection[tablename]
            try:
                doc = ctable[str(rid)]
                for key, value in fields:
                    doc[key.name] = self.represent(
                        value, self.db[tablename][key.name].type
                    )
                ctable.save(doc)
                return 1
            except couchdb.http.ResourceNotFound:
                return 0
        tablename = self.get_table(query)._tablename
        rows = self.select(query, [self.db[tablename]._id], {})
        ctable = self.connection[tablename]
        table = self.db[tablename]
        for row in rows:
            doc = ctable[str(row.id)]
            for key, value in fields:
                doc[key.name] = self.represent(value, table[key.name].type)
            ctable.save(doc)
        return len(rows)

    def count(self, query, distinct=None):
        if distinct:
            raise RuntimeError("COUNT DISTINCT not supported")
        if not isinstance(query, Query):
            raise SyntaxError("Not Supported")
        tablename = self.get_table(query)._tablename
        rows = self.select(query, [self.db[tablename]._id], {})
        return len(rows)

    def delete(self, table, query):
        from ..drivers import couchdb

        if not isinstance(query, Query):
            raise SyntaxError("Not Supported")
        if query.first.type == "id" and query.op == self.eq:
            rid = query.second
            tablename = query.first.tablename
            assert tablename == query.first.tablename
            ctable = self.connection[tablename]
            try:
                del ctable[str(rid)]
                return 1
            except couchdb.http.ResourceNotFound:
                return 0
        tablename = self.get_table(query)._tablename
        rows = self.select(query, [self.db[tablename]._id], {})
        ctable = self.connection[tablename]
        for row in rows:
            del ctable[str(row.id)]
        return len(rows)

# ============================================================
# Dialect
# ============================================================

from ..backend_base import dialects
from ..backend_base import NoSQLDialect


@dialects.register_for(CouchDB)
class CouchDBDialect(NoSQLDialect):
    def _and(self, first, second, query_env={}):
        return "(%s && %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def _or(self, first, second, query_env={}):
        return "(%s || %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

    def eq(self, first, second=None, query_env={}):
        if second is None:
            return "(%s == null)" % self.expand(first, query_env=query_env)
        return "(%s == %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def ne(self, first, second=None, query_env={}):
        if second is None:
            return "(%s != null)" % self.expand(first, query_env=query_env)
        return "(%s != %s)" % (
            self.expand(first, query_env=query_env),
            self.expand(second, first.type, query_env=query_env),
        )

    def comma(self, first, second, query_env={}):
        return "%s + %s" % (
            self.expand(first, query_env=query_env),
            self.expand(second, query_env=query_env),
        )

# ============================================================
# Representer
# ============================================================

from ..helpers.classes import Reference
from ..helpers.serializers import serializers
from ..objects import Row
from ..backend_base import repr_for_type, representers
from ..backend_base import NoSQLRepresenter


@representers.register_for(CouchDB)
class CouchDBRepresenter(NoSQLRepresenter):
    def adapt(self, value):
        if isinstance(value, str):
            value = value.encode("utf8")
        return repr(value)

    @repr_for_type("id")
    def _id(self, value):
        return str(int(value))

    @repr_for_type("reference", adapt=False)
    def _reference(self, value):
        if isinstance(value, (Row, Reference)):
            value = value["id"]
        return self.adapter.object_id(value)

    @repr_for_type("date", adapt=False)
    def _date(self, value):
        return serializers.json(value)

    @repr_for_type("time", adapt=False)
    def _time(self, value):
        serializers.json(value)

    @repr_for_type("datetime", adapt=False)
    def _datetime(self, value):
        return serializers.json(value)

    @repr_for_type("boolean", adapt=False)
    def _boolean(self, value):
        return serializers.json(value)

