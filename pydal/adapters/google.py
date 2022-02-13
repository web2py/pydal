import os
import re
import time
import random
from .._compat import pjoin
from .._globals import THREAD_LOCAL
from ..migrator import InDBMigrator
from ..helpers.classes import FakeDriver, SQLCustomType, SQLALL, Reference
from ..helpers.methods import use_common_filters, xorify
from ..objects import Table, Field, Expression, Query
from .base import NoSQLAdapter
from .mysql import MySQL
from .postgres import PostgrePsyco
from . import adapters, with_connection_or_raise

try:
    from google.cloud import datastore
except:
    datastore = None


class GoogleMigratorMixin(object):
    migrator_cls = InDBMigrator


@adapters.register_for("google:sql")
class GoogleSQL(GoogleMigratorMixin, MySQL):
    uploads_in_blob = True
    REGEX_URI = "^(?P<instance>.*)/(?P<db>.+)$"

    def _find_work_folder(self):
        super(GoogleSQL, self)._find_work_folder()
        if os.path.isabs(self.folder) and self.folder.startswith(os.getcwd()):
            self.folder = os.path.relpath(self.folder, os.getcwd())

    def _initialize_(self):
        super(GoogleSQL, self)._initialize_()
        self.folder = self.folder or pjoin(
            "$HOME",
            THREAD_LOCAL._pydal_folder_.split(os.sep + "applications" + os.sep, 1)[1],
        )
        ruri = self.uri.split("://", 1)[1]
        m = re.match(self.REGEX_URI, ruri)
        if not m:
            raise SyntaxError("Invalid URI string in DAL")
        self.driver_args["instance"] = self.credential_decoder(m.group("instance"))
        self.dbstring = self.credential_decoder(m.group("db"))
        self.createdb = self.adapter_args.get("createdb", True)
        if not self.createdb:
            self.driver_args["database"] = self.dbstring

    def find_driver(self):
        self.driver = "google"

    def connector(self):
        return rdbms.connect(**self.driver_args)

    def after_connection(self):
        if self.createdb:
            self.execute("CREATE DATABASE IF NOT EXISTS %s" % self.dbstring)
            self.execute("USE %s" % self.dbstring)
        self.execute("SET FOREIGN_KEY_CHECKS=1;")
        self.execute("SET sql_mode='NO_BACKSLASH_ESCAPES';")

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        command = self.filter_sql_command(args[0]).decode("utf8")
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv


# based on this: https://cloud.google.com/appengine/docs/standard/python/cloud-sql/
@adapters.register_for("google:MySQLdb")
class GoogleMySQL(GoogleMigratorMixin, MySQL):
    uploads_in_blob = True
    drivers = ("MySQLdb",)

    def _find_work_folder(self):
        super(GoogleMySQL, self)._find_work_folder()
        if os.path.isabs(self.folder) and self.folder.startswith(os.getcwd()):
            self.folder = os.path.relpath(self.folder, os.getcwd())

    def after_connection(self):
        self.execute("SET FOREIGN_KEY_CHECKS=1;")
        self.execute("SET sql_mode='NO_BACKSLASH_ESCAPES,TRADITIONAL';")


@adapters.register_for("google:psycopg2")
class GooglePostgres(GoogleMigratorMixin, PostgrePsyco):
    uploads_in_blob = True
    drivers = ("psycopg2",)

    def _find_work_folder(self):
        super(GooglePostgres, self)._find_work_folder()
        if os.path.isabs(self.folder) and self.folder.startswith(os.getcwd()):
            self.folder = os.path.relpath(self.folder, os.getcwd())


@adapters.register_for("google:datastore")
class GoogleDatastore(NoSQLAdapter):
    dbengine = "google:datastore"

    REGEX_NAMESPACE = ".*://(?P<namespace>.+)"

    def _initialize_(self):
        super(GoogleDatastore, self)._initialize_()
        match = re.match(self.REGEX_NAMESPACE, self.uri)
        namespace = match.group("namespace") if match else None
        self.client = datastore.Client(namespace=namespace)

    def find_driver(self):
        pass

    def connector(self):
        return FakeDriver()

    def create_table(self, table, migrate=True, fake_migrate=False, polymodel=None):
        return None

    def expand(self, query):
        return "%s %s %s" % (query.first, query.op, query.seccond)

    def represent(self, obj, field_type, tablename=None):
        if field_type == "id" and tablename:
            if isinstance(obj, list):
                return [self.represent(item, field_type, tablename) for item in obj]
            elif obj is None:
                return None
            else:
                return long(obj)  # FIXME
        if isinstance(obj, (Expression, Field)):
            raise SyntaxError("not supported on GAE")
        else:
            return obj
        return super(GoogleDatastore, self).represent(obj, field_type)

    def truncate(self, table, mode=""):
        self.db(self.id_query(table)).delete()

    def parse_and_query(self, query):
        parsed = []
        queue = [query]
        while queue:
            query = queue.pop()
            if query.op.__name__ == "or":
                raise RuntimeError("Datastore cannot handler OR")
            if query.op.__name__ == "and":
                queue.append(query.first)
                queue.append(query.second)
            elif isinstance(query.first, Field):
                parsed.append(query)
            else:
                raise RuntimeError("Datastore cannot handle expressions like %s" % query.op.__name__)
        return parsed

    def select_raw(self, query, fields=None, attributes=None):
        db = self.db
        fields = fields or []
        attributes = attributes or {}
        attr_get = attributes.get

        # explan all fields
        new_fields = []
        for item in fields:
            if isinstance(item, SQLALL):
                new_fields += item._table
            else:
                new_fields.append(item)
        fields = new_fields

        # make sure we have a query and a table
        if query:
            table = self.get_table(query)
        elif fields:
            table = fields[0].table
            query = db._adapter.id_query(fields[0].table)
        else:
            raise SyntaxError("Unable to determine the table")

        if query:
            if use_common_filters(query):
                query = self.common_filter(query, [table])

        # make the datastore query
        gquery = self.client.query(kind=table._tablename)
        # if we have subset of fields, make a projection query
        if len(fields) < len(table.fields):
            gquery.projection = [item.name for item in fields]
        # add the filters from the query
        for item in self.parse_and_query(query):
            first, op, second = item.op(item.first, item.second)
            if first.type == "id":
                if op == ">" and second == 0:
                    pass
                elif op == "=":
                    key = self.client.key(table._tablename, second)
                    gquery.key_filter(key)
                else:
                    raise NotImplementedError
            else:
                gquery.add_filter(first.name, op, second)
        # deal with orderby
        orderby = attr_get("orderby", False)
        if orderby:
            orderby = [orderby]
            order = []
            while orderby:
                orderby, item = orderby[:-1], orderby[-1]
                if isinstance(item, Field):
                    order.insert(0, item.name)
                    continue
                first, op, second = item.op(item.first, item.second)
                if op == ',':
                    orderby += [first, second]
                elif op == "~":
                    order.insert(0, "-" + second.name)
                else:
                    raise RuntimeError
            gquery.order = order
        # deal with limitby
        limitby = attr_get("limitby", False)
        limit = limitby[1] if limitby else None
        items = list(gquery.fetch(limit=limit))
        return (items, table)

    def select(self, query, fields, attributes):
        """
        calls select_raw and parses results
        """
        items, table = self.select_raw(query, fields, attributes)
        rows = [
            [(t.name == table._id.name and item.id) or item.get(t.name) for t in fields]
            for item in items
        ]
        colnames = [t.longname for t in fields]
        processor = attributes.get("processor", self.parse)
        return processor(rows, fields, colnames, False)

    def count(self, query, distinct=None, limit=None):
        if distinct:
            raise RuntimeError("COUNT DISTINCT not supported")
        items, table = self.select_raw(query, [])
        return len(items)

    def delete(self, table, query):
        """
        This function assumes datastore can only delete 1000 items at the time
        """
        counter = 0
        while True:
            items, table = self.select_raw(query, [])
            if not items:
                break
            counter += len(items)
            while items:
                self.client.delete_multi([item.key for item in items[:1000]])
                items = items[1000:]
        return counter

    def update(self, table, query, update_fields):
        items, table = self.select_raw(query)
        counter = 0
        for item in items:
            for field, value in update_fields:
                item[field.name] = self.represent(value, field.type)
            self.client.put(item)
            counter += 1
        self.db.logger.info(str(counter))
        return counter

    def insert(self, table, fields):
        dfields = dict((f.name, self.represent(v, f.type)) for f, v in fields)
        key_id = int(time.time() * 1e9) + random.randint(0, 999999999)
        key = self.client.key(table._tablename, key_id)
        entity = datastore.Entity(key=key)
        entity.update(dfields)
        self.client.put(entity)
        return key_id
