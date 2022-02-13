import os
import re
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
from ..ndb import have_ndb

if have_ndb:
    from ..ndb import (
        ndb,
        rdbms,
        namespace_manager,
        classobj,
        NDBPolyModel,
        NDBDecimalProperty
    )

@adapters.register_for("google:datastore+ndb")
class GoogleNDB(NoSQLAdapter):
    dbengine = "google:datastore+ndb"

    REGEX_NAMESPACE = ".*://(?P<namespace>.+)"

    def _initialize_(self):
        super(GoogleNDB, self)._initialize_()
        match = re.match(self.REGEX_NAMESPACE, self.uri)
        if match:
            namespace_manager.set_namespace(match.group("namespace"))
        self.ndb_settings = self.adapter_args.get("ndb_settings")

    def find_driver(self):
        pass

    def connector(self):
        return FakeDriver()

    def create_table(self, table, migrate=True, fake_migrate=False, polymodel=None):
        myfields = {}
        for field in table:
            if isinstance(polymodel, Table) and field.name in polymodel.fields():
                continue
            attr = {}
            if isinstance(field.custom_qualifier, dict):
                # this is custom properties to add to the GAE field declartion
                attr = field.custom_qualifier
            field_type = field.type
            if isinstance(field_type, SQLCustomType):
                ftype = self.types[field_type.native or field_type.type](**attr)
            elif isinstance(field_type, ndb.Property):
                ftype = field_type
            elif field_type.startswith("id"):
                continue
            elif field_type.startswith("decimal"):
                precision, scale = field_type[7:].strip("()").split(",")
                precision = int(precision)
                scale = int(scale)
                dec_cls = NDBDecimalProperty
                ftype = dec_cls(precision, scale, **attr)
            elif field_type.startswith("reference"):
                if field.notnull:
                    attr = dict(required=True)
                ftype = self.types[field_type[:9]](**attr)
            elif field_type.startswith("list:reference"):
                if field.notnull:
                    attr["required"] = True
                ftype = self.types[field_type[:14]](**attr)
            elif field_type.startswith("list:"):
                ftype = self.types[field_type](**attr)
            elif field_type not in self.types or not self.types[field_type]:
                raise SyntaxError("Field: unknown field type: %s" % field_type)
            else:
                ftype = self.types[field_type](**attr)
            myfields[field.name] = ftype
        if not polymodel:
            model_cls = ndb.Model
            table._tableobj = classobj(table._tablename, (model_cls,), myfields)
            # Set NDB caching variables
            if self.ndb_settings and (table._tablename in self.ndb_settings):
                for k, v in self.ndb_settings.iteritems():
                    setattr(table._tableobj, k, v)
        elif polymodel == True:
            pm_cls = NDBPolyModel
            table._tableobj = classobj(table._tablename, (pm_cls,), myfields)
        elif isinstance(polymodel, Table):
            table._tableobj = classobj(
                table._tablename, (polymodel._tableobj,), myfields
            )
        else:
            raise SyntaxError("polymodel must be None, True, a table or a tablename")
        return None

    def _expand(self, expression, field_type=None, query_env={}):
        if expression is None:
            return None
        elif isinstance(expression, Field):
            if expression.type in ("text", "blob", "json"):
                raise SyntaxError("AppEngine does not index by: %s" % expression.type)
            return expression.name
        elif isinstance(expression, (Expression, Query)):
            if expression.second is not None:
                return expression.op(
                    expression.first, expression.second, query_env=query_env
                )
            elif expression.first is not None:
                return expression.op(expression.first, query_env=query_env)
            else:
                return expression.op()
        elif field_type:
            return self.represent(expression, field_type)
        elif isinstance(expression, (list, tuple)):
            return ",".join([self.represent(item, field_type) for item in expression])
        elif hasattr(expression, "_FilterNode__name"):
            # check for _FilterNode__name to avoid explicit
            # import of FilterNode
            return expression
        else:
            raise NotImplementedError

    def _add_operators_to_parsed_row(self, rid, table, row):
        row.gae_item = rid
        lid = rid.key.id()
        row.id = lid
        super(GoogleNDB, self)._add_operators_to_parsed_row(lid, table, row)

    def represent(self, obj, field_type, tablename=None):
        if isinstance(obj, ndb.Key):
            return obj
        if field_type == "id" and tablename:
            if isinstance(obj, list):
                return [self.represent(item, field_type, tablename) for item in obj]
            elif obj is None:
                return None
            else:
                return ndb.Key(tablename, long(obj))
        if isinstance(obj, (Expression, Field)):
            raise SyntaxError("not supported on GAE")
        if isinstance(field_type, gae.Property):
            return obj
        return super(GoogleNDB, self).represent(obj, field_type)

    def truncate(self, table, mode=""):
        self.db(self.id_query(table)).delete()

    def select_raw(self, query, fields=None, attributes=None, count_only=False):
        db = self.db
        fields = fields or []
        attributes = attributes or {}
        args_get = attributes.get
        new_fields = []

        for item in fields:
            if isinstance(item, SQLALL):
                new_fields += item._table
            else:
                new_fields.append(item)

        fields = new_fields
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

        # tableobj is a GAE/NDB Model class (or subclass)
        tableobj = table._tableobj
        filters = self.expand(query)

        ## DETERMINE PROJECTION
        projection = None
        if len(table.fields) == len(fields):
            # getting all fields, not a projection query
            projection = None
        elif args_get("projection") == True:
            projection = []
            for f in fields:
                if f.type in ["text", "blob", "json"]:
                    raise SyntaxError(
                        "text and blob field types not allowed in "
                        + "projection queries"
                    )
                else:
                    projection.append(f)

        elif args_get("filterfields") is True:
            projection = []
            for f in fields:
                projection.append(f)

        # real projection's can't include 'id'.
        # it will be added to the result later
        if projection and args_get("projection") == True:
            query_projection = [f.name for f in projection if f.name != table._id.name]
        else:
            query_projection = None
        ## DONE WITH PROJECTION

        cursor = args_get("reusecursor")
        cursor = cursor if isinstance(cursor, str) else None
        qo = ndb.QueryOptions(projection=query_projection, cursor=cursor)

        if filters == None:
            items = tableobj.query(default_options=qo)
        elif getattr(filters, "filter_all", None):
            items = []
        elif (
            getattr(filters, "_FilterNode__value", None)
            and getattr(filters, "_FilterNode__name", None) == "__key__"
            and getattr(filters, "_FilterNode__opsymbol", None) == "="
        ):
            item = ndb.Key.from_old_key(getattr(filters, "_FilterNode__value")).get()
            items = [item] if item else []
        else:
            items = tableobj.query(filters, default_options=qo)

        if count_only:
            items = [len(items) if isinstance(items, list) else items.count()]
        elif not isinstance(items, list):
            if args_get("left", None):
                raise SyntaxError("Set: no left join in appengine")
            if args_get("groupby", None):
                raise SyntaxError("Set: no groupby in appengine")
            orderby = args_get("orderby", False)
            if orderby:
                if isinstance(orderby, (list, tuple)):
                    orderby = xorify(orderby)
                if isinstance(orderby, Expression):
                    orderby = self.expand(orderby)
                orders = orderby.split(", ")
                tbl = tableobj
                for order in orders:
                    order = str(order)
                    desc = order.startswith("-")
                    name = order[1 if desc else 0 :].split(".")[-1]
                    if name == "id":
                        o = -tbl._key if desc else tbl._key
                    else:
                        o = -getattr(tbl, name) if desc else getattr(tbl, name)
                    items = items.order(o)

            if args_get("limitby", None):
                (lmin, lmax) = attributes["limitby"]
                limit = lmax - lmin
                fetch_args = {"offset": lmin, "keys_only": True}

                keys, cursor, more = items.fetch_page(limit, **fetch_args)
                items = ndb.get_multi(keys)
                # cursor is only useful if there was a limit and we
                # didn't return all results
                if args_get("reusecursor"):
                    db["_lastcursor"] = cursor
        return (items, table, projection or [f for f in table])

    def select(self, query, fields, attributes):
        """
        This is the GAE version of select. Some notes to consider:
        - 'nativeRef' is a magical fieldname used for self references
          on GAE
        - optional attribute 'projection' when set to True will trigger
          use of the GAE projection queries.  note that there are rules for
          what is accepted imposed by GAE: each field must be indexed,
          projection queries cannot contain blob or text fields, and you
          cannot use == and also select that same field.
          see https://developers.google.com/appengine/docs/python/datastore/queries#Query_Projection
        - optional attribute 'filterfields' when set to True web2py will
          only parse the explicitly listed fields into the Rows object,
          even though all fields are returned in the query. This can be
          used to reduce memory usage in cases where true projection
          queries are not usable.
        - optional attribute 'reusecursor' allows use of cursor with
          queries that have the limitby attribute. Set the attribute to
          True for the first query, set it to the value of
          db['_lastcursor'] to continue a previous query. The user must
          save the cursor value between requests, and the filters must be
          identical. It is up to the user to follow google's limitations:
          https://developers.google.com/appengine/docs/python/datastore/queries#Query_Cursors
        """

        items, table, fields = self.select_raw(query, fields, attributes)
        rows = [
            [
                (t.name == table._id.name and item)
                or (t.name == "nativeRef" and item)
                or getattr(item, t.name)
                for t in fields
            ]
            for item in items
        ]
        colnames = [t.longname for t in fields]
        processor = attributes.get("processor", self.parse)
        return processor(rows, fields, colnames, False)

    def count(self, query, distinct=None, limit=None):
        if distinct:
            raise RuntimeError("COUNT DISTINCT not supported")
        items, table, fields = self.select_raw(query, count_only=True)
        return items[0]

    def delete(self, table, query):
        """
        This function was changed on 2010-05-04 because according to
        http://code.google.com/p/googleappengine/issues/detail?id=3119
        GAE no longer supports deleting more than 1000 records.
        """
        items, table, fields = self.select_raw(query)
        # items can be one item or a query
        if not isinstance(items, list):
            # use a keys_only query to ensure that this runs as a datastore
            # small operations
            leftitems = items.fetch(1000, keys_only=True)
            counter = 0
            while len(leftitems):
                counter += len(leftitems)
                ndb.delete_multi(leftitems)
                leftitems = items.fetch(1000, keys_only=True)
        else:
            counter = len(items)
            ndb.delete_multi([item.key for item in items])
        return counter

    def update(self, table, query, update_fields):
        items, table, fields = self.select_raw(query)
        counter = 0
        for item in items:
            for field, value in update_fields:
                setattr(item, field.name, self.represent(value, field.type))
            item.put()
            counter += 1
        self.db.logger.info(str(counter))
        return counter

    def insert(self, table, fields):
        dfields = dict((f.name, self.represent(v, f.type)) for f, v in fields)
        tmp = table._tableobj(**dfields)
        tmp.put()
        key = tmp.key
        rid = Reference(key.id())
        rid._table, rid._record, rid._gaekey = table, None, key
        return rid

    def bulk_insert(self, table, items):
        parsed_items = []
        for item in items:
            dfields = dict((f.name, self.represent(v, f.type)) for f, v in item)
            parsed_items.append(table._tableobj(**dfields))
        return ndb.put_multi(parsed_items)
