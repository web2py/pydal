# -*- coding: utf-8 -*-
import datetime
import re
import copy

from .._globals import IDENTITY
from .._compat import integer_types, basestring, PY2
from ..objects import Table, Query, Field, Expression, Row
from ..helpers.classes import SQLCustomType, SQLALL, Reference
from ..helpers.methods import use_common_filters, xorify
from .base import NoSQLAdapter, CALLABLETYPES, SELECT_ARGS

try:
    from bson import Binary
    from bson.binary import USER_DEFINED_SUBTYPE
except:
    class Binary(object):
        pass
    USER_DEFINED_SUBTYPE = 0

long = integer_types[-1]


SUPPORTED_SELECT_ARGS = set(('limitby', 'orderby', 'orderby_on_limitby',
                             'groupby', 'distinct', 'having'))
SQL_ONLY_SELECT_ARGS = set(('join', 'left'))
NON_MONGO_SELECT_ARGS = set(('for_update',))
NOT_IMPLEMENTED_SELECT_ARGS = (SELECT_ARGS - SUPPORTED_SELECT_ARGS
    - SQL_ONLY_SELECT_ARGS - NON_MONGO_SELECT_ARGS)


class MongoDBAdapter(NoSQLAdapter):
    drivers = ('pymongo',)
    driver_auto_json = ['loads', 'dumps']

    uploads_in_blob = False

    types = {
        'boolean': bool,
        'string': str,
        'text': str,
        'json': str,
        'password': str,
        'blob': str,
        'upload': str,
        'integer': long,
        'bigint': long,
        'float': float,
        'double': float,
        'date': datetime.date,
        'time': datetime.time,
        'datetime': datetime.datetime,
        'id': long,
        'reference': long,
        'list:string': list,
        'list:integer': list,
        'list:reference': list,
    }

    GROUP_MARK = "__#GROUP#__"
    AS_MARK = "__#AS#__"
    REGEXP_MARK1 = "__#REGEXP_1#__"
    REGEXP_MARK2 = "__#REGEXP_2#__"

    def __init__(self, db, uri='mongodb://127.0.0.1:5984/db',
                 pool_size=0, folder=None, db_codec='UTF-8',
                 credential_decoder=IDENTITY, driver_args={},
                 adapter_args={}, do_connect=True, after_connection=None):

        super(MongoDBAdapter, self).__init__(
            db=db,
            uri=uri,
            pool_size=pool_size,
            folder=folder,
            db_codec=db_codec,
            credential_decoder=credential_decoder,
            driver_args=driver_args,
            adapter_args=adapter_args,
            do_connect=do_connect,
            after_connection=after_connection)

        if do_connect:
            self.find_driver(adapter_args)

            from pymongo import version
            if 'fake_version' in driver_args:
                version = driver_args['fake_version']
            if int(version.split('.')[0]) < 3:
                raise Exception(
                    "pydal requires pymongo version >= 3.0, found '%s'"
                    % version)

        import random
        from bson.objectid import ObjectId
        from bson.son import SON
        import pymongo.uri_parser
        from pymongo.write_concern import WriteConcern

        m = pymongo.uri_parser.parse_uri(uri)

        self.epoch = datetime.datetime.fromtimestamp(0)
        self.SON = SON
        self.ObjectId = ObjectId
        self.random = random
        self.WriteConcern = WriteConcern

        self.dbengine = 'mongodb'
        db['_lastsql'] = ''
        self.db_codec = 'UTF-8'
        self.find_or_make_work_folder()
        # this is the minimum amount of replicates that it should wait
        # for on insert/update
        self.minimumreplication = adapter_args.get('minimumreplication', 0)
        # by default all inserts and selects are performed asynchronous,
        # but now the default is
        # synchronous, except when overruled by either this default or
        # function parameter
        self.safe = 1 if adapter_args.get('safe', True) else 0

        if isinstance(m, tuple):
            m = {"database": m[1]}
        if m.get('database') is None:
            raise SyntaxError("Database is required!")

        def connector(uri=self.uri, m=m):
            driver = self.driver.MongoClient(uri, w=self.safe)[m.get('database')]
            driver.cursor = lambda: self.fake_cursor
            driver.close = lambda: None
            driver.commit = lambda: None
            return driver
        self.connector = connector
        self.reconnect()

        # _server_version is a string like '3.0.3' or '2.4.12'
        self._server_version = self.connection.command("serverStatus")['version']
        self.server_version = tuple(
            [int(x) for x in self._server_version.split('.')])
        self.server_version_major = (
            self.server_version[0] + self.server_version[1] / 10.0)

    def object_id(self, arg=None):
        """ Convert input to a valid Mongodb ObjectId instance

        self.object_id("<random>") -> ObjectId (not unique) instance """
        if not arg:
            arg = 0
        if isinstance(arg, basestring):
            # we assume an integer as default input
            rawhex = len(arg.replace("0x", "").replace("L", "")) == 24
            if arg.isdigit() and (not rawhex):
                arg = int(arg)
            elif arg == "<random>":
                arg = int("0x%s" %
                    "".join([self.random.choice("0123456789abcdef")
                        for x in range(24)]), 0)
            elif arg.isalnum():
                if not arg.startswith("0x"):
                    arg = "0x%s" % arg
                try:
                    arg = int(arg, 0)
                except ValueError as e:
                    raise ValueError(
                            "invalid objectid argument string: %s" % e)
            else:
                raise ValueError("Invalid objectid argument string. " +
                                 "Requires an integer or base 16 value")
        elif isinstance(arg, self.ObjectId):
            return arg

        elif isinstance(arg, (Row, Reference)):
            return self.object_id(long(arg['id']))

        elif not isinstance(arg, (int, long)):
            raise TypeError("object_id argument must be of type " +
                            "ObjectId or an objectid representable integer" +
                            " (type %s)" % type(arg) )
        hexvalue = hex(arg)[2:].rstrip('L').zfill(24)
        return self.ObjectId(hexvalue)

    def parse_reference(self, value, field_type):
        # here we have to check for ObjectID before base parse
        if isinstance(value, self.ObjectId):
            value = long(str(value), 16)
        return super(MongoDBAdapter,
                     self).parse_reference(value, field_type)

    def parse_id(self, value, field_type):
        if isinstance(value, self.ObjectId):
            value = long(str(value), 16)
        return super(MongoDBAdapter,
                     self).parse_id(value, field_type)

    def represent(self, obj, fieldtype):
        if isinstance(obj, CALLABLETYPES):
            obj = obj()
        if isinstance(fieldtype, SQLCustomType):
            return fieldtype.encoder(obj)
        if isinstance(obj, self.ObjectId):
            value = obj
        elif fieldtype == 'id':
            value = self.object_id(obj)
        elif fieldtype in ['double', 'float']:
            value = None if obj is None else float(obj)
        elif fieldtype == 'date':
            if obj is None:
                return None
            # this piece of data can be stripped off based on the fieldtype
            t = datetime.time(0, 0, 0)
            # mongodb doesn't have a date object and so it must datetime,
            # string or integer
            return datetime.datetime.combine(obj, t)
        elif fieldtype == 'time':
            if obj is None:
                return None
            # this piece of data can be stripped off based on the fieldtype
            d = datetime.date(2000, 1, 1)
            # mongodb doesn't have a time object and so it must datetime,
            # string or integer
            return datetime.datetime.combine(d, obj)
        elif fieldtype == "blob":
            if isinstance(obj, basestring) and obj == '':
                obj = None
            return MongoBlob(obj)

        # reference types must be converted to ObjectID
        elif isinstance(fieldtype, basestring):
            if fieldtype.startswith('list:reference'):
                value = [self.object_id(v) for v in obj]
            elif fieldtype.startswith("reference") or fieldtype == "id":
                value = self.object_id(obj)
            else:
                value = NoSQLAdapter.represent(self, obj, fieldtype)

        elif isinstance(fieldtype, Table):
            raise NotImplementedError("How did you reach this line of code???")
            value = self.object_id(obj)

        else:
            value = NoSQLAdapter.represent(self, obj, fieldtype)

        return value

    def parse_blob(self, value, field_type):
        return MongoBlob.decode(value)

    REGEX_SELECT_AS_PARSER = re.compile("\\'" + AS_MARK + "\\': \\'(\\S+)\\'")

    def _regex_select_as_parser(self, colname):
        return self.REGEX_SELECT_AS_PARSER.search(colname)

    def _get_collection(self, tablename, safe=None):
        ctable = self.connection[tablename]

        if safe is not None and safe != self.safe:
            wc = self.WriteConcern(w=self._get_safe(safe))
            ctable = ctable.with_options(write_concern=wc)

        return ctable

    def _get_safe(self, val=None):
        if val is None:
            return self.safe
        return 1 if val else 0

    def create_table(self, table, migrate=True, fake_migrate=False,
                     polymodel=None):
        table._dbt = None

        table._notnulls = []
        for field_name in table.fields:
            if table[field_name].notnull:
                table._notnulls.append(field_name)

        table._uniques = []
        for field_name in table.fields:
            if table[field_name].unique:
                # this is unnecessary if the fields are indexed and unique
                table._uniques.append(field_name)

    class Expanded (object):
        """
        Class to encapsulate a pydal expression and track the parse 
        expansion and its results.

        Two different MongoDB mechanisms are targeted here.  If the query 
        is sufficiently simple, then simple queries are generated.  The 
        bulk of the complexity here is however to support more complex 
        queries that are targeted to the MongoDB Aggregation Pipeline.

        This class supports four operations: 'count', 'select', 'update' 
        and 'delete'.

        Behavior varies somewhat for each operation type.  However 
        building each pipeline stage is shared where the behavior is the
        same (or similar) for the different operations.

        In general an attempt is made to build the query without using the 
        pipeline, and if that fails then the query is rebuilt with the 
        pipeline.

        QUERY constructed in _build_pipeline_query():
          $project : used to calculate expressions if needed
          $match: filters out records

        FIELDS constructed in _expand_fields():
            FIELDS:COUNT
              $group : filter for distinct if needed
              $group: count the records remaining

            FIELDS:SELECT
              $group : implement aggregations if needed
              $project: implement expressions (etc) for select

            FIELDS:UPDATE
              $project: implement expressions (etc) for update

        HAVING constructed in _add_having():
          $project : used to calculate expressions
          $match: filters out records
          $project : used to filter out previous expression fields

        """

        def __init__ (self, adapter, crud, query, fields=(), tablename=None,
                      groupby=None, distinct=False, having=None):
            self.adapter = adapter
            self._parse_data = {'pipeline': False, 'need_group': 
                                bool(groupby or distinct or having)}
            self.crud = crud
            self.having = having
            self.distinct = distinct
            if not groupby and distinct:
                if distinct is True:
                    # groupby gets all fields
                    self.groupby = fields
                else:
                    self.groupby = distinct
            else:
                self.groupby = groupby

            if crud == 'update':
                self.values = [(f[0], self.annotate_expression(f[1]))
                               for f in (fields or [])]
                self.fields = [f[0] for f in self.values]
            else:
                self.fields = [self.annotate_expression(f)
                               for f in (fields or [])]

            self.tablename = tablename or adapter.get_table(query, *self.fields)
            if use_common_filters(query):
                query = adapter.common_filter(query, [self.tablename])
            self.query = self.annotate_expression(query)

            # expand the query
            self.pipeline = []
            self.query_dict = adapter.expand(self.query)
            self.field_dicts = adapter.SON()
            self.field_groups = adapter.SON()
            self.field_groups['_id'] = adapter.SON()

            if self._parse_data['pipeline']:
                # if the query needs the aggregation engine, set that up
                self._build_pipeline_query()

                # expand the fields for the aggregation engine
                self._expand_fields(None)
            else:
                # expand the fields
                try:
                    if not self._parse_data['need_group']:
                        self._expand_fields(self._fields_loop_abort)
                    else:
                        self._parse_data['pipeline'] = True
                        raise StopIteration

                except StopIteration:
                    # if the fields needs the aggregation engine, set that up
                    self.field_dicts = adapter.SON()
                    if self.query_dict:
                        if self.query_dict != MongoDBAdapter.Expanded.NULL_QUERY:
                            self.pipeline = [{'$match': self.query_dict}]
                        self.query_dict = {}
                    # expand the fields for the aggregation engine
                    self._expand_fields(None)

            if not self._parse_data['pipeline']:
                if crud == 'update':
                    # do not update id fields
                    for fieldname in ("_id", "id"):
                        if fieldname in self.field_dicts:
                            del self.field_dicts[fieldname]
            else:
                if crud == 'update':
                    self._add_all_fields_projection(self.field_dicts)
                    self.field_dicts = adapter.SON()

                elif crud == 'select':
                    if self._parse_data['need_group']:
                        if not self.groupby:
                            # no groupby, aggregate all records
                            self.field_groups['_id'] = None
                        # id has no value after aggregations
                        self.field_dicts['_id'] = False
                        self.pipeline.append({'$group': self.field_groups})
                    if self.field_dicts:
                        self.pipeline.append({'$project': self.field_dicts})
                        self.field_dicts = adapter.SON()
                    self._add_having()

                elif crud == 'count':
                    if self._parse_data['need_group']:
                        self.pipeline.append({'$group': self.field_groups})
                    self.pipeline.append(
                        {'$group': {"_id": None, 'count': {"$sum": 1}}})

                #elif crud == 'delete':
                #    pass

        try:
            from bson.objectid import ObjectId
            NULL_QUERY = {'_id': {'$gt': 
                                  ObjectId('000000000000000000000000')}}
        except:
            pass

        def _build_pipeline_query(self):

            # search for anything needing the $match stage.
            #   currently only '$regex' requires the match stage
            def parse_need_match_stage(items, parent, parent_key):
                need_match = False
                non_matched_indices = []
                if isinstance(items, list):
                    indices = range(len(items))
                elif isinstance(items, dict):
                    indices = items.keys()
                else:
                    return

                for i in indices:
                    if parse_need_match_stage(items[i], items, i):
                        need_match = True

                    elif i not in [MongoDBAdapter.REGEXP_MARK1,
                                   MongoDBAdapter.REGEXP_MARK2]:
                        non_matched_indices.append(i)

                    if i == MongoDBAdapter.REGEXP_MARK1:
                        need_match = True
                        self.query_dict['project'].update(items[i])
                        parent[parent_key] = items[MongoDBAdapter.REGEXP_MARK2]

                if need_match:
                    for i in non_matched_indices:
                        name = str(items[i])
                        self.query_dict['project'][name] = items[i]
                        items[i] = {name: True}

                if parent is None and self.query_dict['project']:
                    self.query_dict['match'] = items
                return need_match

            expanded = self.adapter.expand(self.query)

            if MongoDBAdapter.REGEXP_MARK1 in expanded:
                # the REGEXP_MARK is at the top of the tree, so can just split
                # the regex over a '$project' and a '$match'
                self.query_dict = None
                match = expanded[MongoDBAdapter.REGEXP_MARK2]
                project = expanded[MongoDBAdapter.REGEXP_MARK1]

            else:
                self.query_dict = {'project': {}, 'match': {}}
                if parse_need_match_stage(expanded, None, None):
                    project = self.query_dict['project']
                    match = self.query_dict['match']
                else:
                    project = {'__query__': expanded}
                    match = {'__query__': True}

            if self.crud in ['select', 'update']:
                self._add_all_fields_projection(project)
            else:
                self.pipeline.append({'$project': project})
            self.pipeline.append({'$match': match})
            self.query_dict = None

        def _expand_fields(self, mid_loop):
            if self.crud == 'update':
                mid_loop = mid_loop or self._fields_loop_update_pipeline
                for field, value in self.values:
                    self._expand_field(field, value, mid_loop)
            elif self.crud in ['select', 'count']:
                mid_loop = mid_loop or self._fields_loop_select_pipeline
                for field in self.fields:
                    self._expand_field(field, field, mid_loop)
            elif self.fields:
                raise RuntimeError(self.crud + " not supported with fields")

        def _expand_field(self, field, value, mid_loop):
            expanded = {}
            if isinstance(field, Field):
                expanded = self.adapter.expand(value, field.type)
            elif isinstance(field, (Expression, Query)):
                expanded = self.adapter.expand(field)
                field.name = str(expanded)
            else:
                raise RuntimeError("%s not supported with fields" % type(field))

            if mid_loop:
                expanded = mid_loop(expanded, field, value)
            self.field_dicts[field.name] = expanded

        def _fields_loop_abort(self, expanded, *args):
            # if we need the aggregation engine, then start over
            if self._parse_data['pipeline']:
                raise StopIteration()
            return expanded

        def _fields_loop_update_pipeline(self, expanded, field, value):
            if not isinstance(value, Expression):
                if self.adapter.server_version_major >= 2.6:
                    expanded = {'$literal': expanded}

                # '$literal' not present in server versions < 2.6
                elif field.type in ['string', 'text', 'password']:
                    expanded = {'$concat': [expanded]}
                elif field.type in ['integer', 'bigint', 'float', 'double']:
                    expanded = {'$add': [expanded]}
                elif field.type == 'boolean':
                    expanded = {'$and': [expanded]}
                elif field.type in ['date', 'time', 'datetime']:
                    expanded = {'$add': [expanded]}
                else:
                    raise RuntimeError("updating with expressions not "
                        + "supported for field type '"
                        + "%s' in MongoDB version < 2.6" % field.type)
            return expanded

        def _fields_loop_select_pipeline(self, expanded, field, value):

            # search for anything needing $group
            def parse_groups(items, parent, parent_key):
                for item in items:
                    if isinstance(items[item], list):
                        for list_item in items[item]:
                            if isinstance(list_item, dict):
                                parse_groups(list_item, items[item], 
                                             items[item].index(list_item))

                    elif isinstance(items[item], dict):
                        parse_groups(items[item], items, item)

                    if item == MongoDBAdapter.GROUP_MARK:
                        name = str(items)
                        self.field_groups[name] = items[item]
                        parent[parent_key] = '$' + name
                return items

            if MongoDBAdapter.AS_MARK in field.name:
                # The AS_MARK in the field name is used by base to alias the
                # result, we don't actually need the AS_MARK in the parse tree
                # so we remove it here.
                if isinstance(expanded, list):
                    # AS mark is first element in list, drop it
                    expanded = expanded[1]

                elif MongoDBAdapter.AS_MARK in expanded:
                    # AS mark is element in dict, drop it
                    del expanded[MongoDBAdapter.AS_MARK]

                else:
                    # ::TODO:: should be possible to do this...
                    raise SyntaxError("AS() not at top of parse tree")

            if MongoDBAdapter.GROUP_MARK in expanded:
                # the GROUP_MARK is at the top of the tree, so can just pass
                # the group result straight through the '$project' stage
                self.field_groups[field.name] = expanded[MongoDBAdapter.GROUP_MARK]
                expanded = 1

            elif MongoDBAdapter.GROUP_MARK in field.name:
                # the GROUP_MARK is not at the top of the tree, so we need to
                # pass the group results through to a '$project' stage.
                expanded = parse_groups(expanded, None, None)

            elif self._parse_data['need_group']:
                if field in self.groupby:
                    # this is a 'groupby' field
                    self.field_groups['_id'][field.name] = expanded
                    expanded = '$_id.' + field.name
                else:
                    raise SyntaxError("field '%s' not in groupby" % field)

            return expanded

        def _add_all_fields_projection(self, fields):
            for fieldname in self.adapter.db[self.tablename].fields:
                # add all fields to projection to pass them through
                if fieldname not in fields and fieldname not in ("_id", "id"):
                    fields[fieldname] = 1
            self.pipeline.append({'$project': fields})

        def _add_having(self):
            if not self.having:
                return
            self._expand_field(
                self.having, None, self._fields_loop_select_pipeline)
            fields = {'__having__': self.field_dicts[self.having.name]}
            for fieldname in self.pipeline[-1]['$project']:
                # add all fields to projection to pass them through
                if fieldname not in fields and fieldname not in ("_id", "id"):
                    fields[fieldname] = 1

            self.pipeline.append({'$project': copy.copy(fields)})
            self.pipeline.append({'$match': {'__having__': True}})
            del fields['__having__']
            self.pipeline.append({'$project': fields})

        def annotate_expression(self, expression):
            def mark_has_field(expression):
                if not isinstance(expression, (Expression, Query)):
                    return False
                first_has_field = mark_has_field(expression.first)
                second_has_field = mark_has_field(expression.second)
                expression.has_field = (isinstance(expression, Field)
                    or first_has_field or second_has_field)
                return expression.has_field

            def add_parse_data(child, parent):
                if isinstance(child, (Expression, Query)):
                    child.parse_root = parent.parse_root
                    child.parse_parent = parent
                    child.parse_depth = parent.parse_depth + 1
                    child._parse_data = parent._parse_data
                    add_parse_data(child.first, child)
                    add_parse_data(child.second, child)
                elif isinstance(child, (list, tuple)):
                    for c in child:
                        add_parse_data(c, parent)

            if isinstance(expression, (Expression, Query)):
                expression.parse_root = expression
                expression.parse_depth = -1
                expression._parse_data = self._parse_data
                add_parse_data(expression, expression)

            mark_has_field(expression)
            return expression

        def get_collection(self, safe=None):
            return self.adapter._get_collection(self.tablename, safe)

    @staticmethod
    def parse_data(expression, attribute, value=None):
        if isinstance(expression, (list, tuple)):
            ret = False
            for e in expression:
                ret = MongoDBAdapter.parse_data(e, attribute, value) or ret
            return ret

        if value is not None:
            try:
                expression._parse_data[attribute] = value
            except AttributeError:
                return None
        try:
            return expression._parse_data[attribute]
        except (AttributeError, TypeError):
            return None

    @staticmethod
    def has_field(expression):
        try:
            return expression.has_field
        except AttributeError:
            return False

    def expand(self, expression, field_type=None):

        if isinstance(expression, Field):
            if expression.type == 'id':
                result = "_id"
            else:
                result = expression.name
            if self.parse_data(expression, 'pipeline'):
                # field names as part of expressions need to start with '$'
                result = '$' + result

        elif isinstance(expression, (Expression, Query)):
            first = expression.first
            second = expression.second
            if isinstance(first, Field) and "reference" in first.type:
                # cast to Mongo ObjectId
                if isinstance(second, (tuple, list, set)):
                    second = [self.object_id(item) for
                                         item in expression.second]
                else:
                    second = self.object_id(expression.second)

            op = expression.op
            optional_args = expression.optional_args or {}
            if second is not None:
                result = op(first, second, **optional_args)
            elif first is not None:
                result = op(first, **optional_args)
            elif isinstance(op, str):
                result = op
            else:
                result = op(**optional_args)

        elif isinstance(expression, MongoDBAdapter.Expanded):
            expression.query = (self.expand(expression.query, field_type))
            result = expression

        elif isinstance(expression, (list, tuple)):
            raise NotImplementedError("How did you reach this line of code???")
            result = [self.represent(item, field_type) for item in expression]

        elif field_type:
            result = self.represent(expression, field_type)

        else:
            result = expression
        return result

    def drop(self, table, mode=''):
        ctable = self.connection[table._tablename]
        ctable.drop()
        self._drop_cleanup(table)
        return

    def truncate(self, table, mode, safe=None):
        ctable = self.connection[table._tablename]
        ctable.delete_many({})

    def count(self, query, distinct=None, snapshot=True):
        if not isinstance(query, Query):
            raise SyntaxError("Type '%s' not supported in count" % type(query))

        distinct_fields = []
        if distinct is True:
            distinct_fields = [x for x in query.first.table if x.name != 'id']
        elif distinct:
            if isinstance(distinct, Field):
                distinct_fields = [distinct]
            else:
                while (isinstance(distinct, Expression) and
                        isinstance(distinct.second, Field)):
                    distinct_fields += [distinct.second]
                    distinct = distinct.first
                if isinstance(distinct, Field):
                    distinct_fields += [distinct]
            distinct = True

        expanded = MongoDBAdapter.Expanded(
            self, 'count', query, fields=distinct_fields, distinct=distinct)
        ctable = expanded.get_collection()
        if not expanded.pipeline:
            return ctable.count(filter=expanded.query_dict)
        else:
            for record in ctable.aggregate(expanded.pipeline):
                return record['count']
            return 0

    def select(self, query, fields, attributes, snapshot=False):
        new_fields = []
        for item in fields:
            if isinstance(item, SQLALL):
                new_fields += item._table
            else:
                new_fields.append(item)
        fields = new_fields
        tablename = self.get_table(query, *fields)

        orderby = attributes.get('orderby', False)
        limitby = attributes.get('limitby', False)
        groupby = attributes.get('groupby', None)
        orderby_on_limitby = attributes.get('orderby_on_limitby', True)
        distinct = attributes.get('distinct', False)
        having = attributes.get('having', None)
        for key in attributes.keys():
            if attributes[key] and key not in SUPPORTED_SELECT_ARGS:
                if key in NON_MONGO_SELECT_ARGS:
                    self.db.logger.warning(
                        "Attribute '%s' unsuppored by MongoDB" % key)
                elif key in NOT_IMPLEMENTED_SELECT_ARGS:
                    self.db.logger.warning(
                        "Attribute '%s' is not implemented by MongoDB" % key)
                elif key in SQL_ONLY_SELECT_ARGS:
                    raise MongoDBAdapter.NotOnNoSqlError(
                        "Attribute '%s' not supported on NoSQL databases" % key)
                else:
                    raise SyntaxError(
                        "Attribute '%s' is unknown" % key)
                
        if limitby and orderby_on_limitby and not orderby:
            if groupby:
                orderby = groupby
            else:
                table = self.db[tablename]
                orderby = [table[x] for x in (hasattr(table, '_primarykey')
                    and table._primarykey or ['_id'])]

        if not orderby:
            mongosort_list = []
        else:
            if snapshot:
                raise RuntimeError("snapshot and orderby are mutually exclusive")
            if isinstance(orderby, (list, tuple)):
                orderby = xorify(orderby)

            if str(orderby) == '<random>':
                # !!!! need to add 'random'
                mongosort_list = self.RANDOM()
            else:
                mongosort_list = []
                for f in self.expand(orderby).split(','):
                    include = 1
                    if f.startswith('-'):
                        include = -1
                        f = f[1:]
                    if f.startswith('$'):
                        f = f[1:]
                    mongosort_list.append((f, include))

        expanded = MongoDBAdapter.Expanded(
            self, 'select', query, fields or self.db[tablename],
            groupby=groupby, distinct=distinct, having=having)
        ctable = self.connection[tablename]
        modifiers = {'snapshot':snapshot}

        if not expanded.pipeline:
            if limitby:
                limitby_skip, limitby_limit = limitby[0], int(limitby[1]) - 1
            else:
                limitby_skip = limitby_limit = 0
            mongo_list_dicts = ctable.find(
                expanded.query_dict, expanded.field_dicts, skip=limitby_skip,
                limit=limitby_limit, sort=mongosort_list, modifiers=modifiers)
            null_rows = []
        else:
            if mongosort_list:
                sortby_dict = self.SON()
                for f in mongosort_list:
                    sortby_dict[f[0]] = f[1]
                expanded.pipeline.append({'$sort': sortby_dict})
            if limitby and limitby[1]:
                expanded.pipeline.append({'$limit': limitby[1]})
            if limitby and limitby[0]:
                expanded.pipeline.append({'$skip': limitby[0]})

            mongo_list_dicts = ctable.aggregate(expanded.pipeline)
            null_rows = [(None,)]

        rows = []
        # populate row in proper order
        # Here we replace ._id with .id to follow the standard naming
        colnames = []
        newnames = []
        for field in expanded.fields:
            if hasattr(field, "tablename"):
                if field.name in ('id', '_id'):
                    # Mongodb reserved uuid key
                    colname = (tablename + '.' + 'id', '_id')
                else:
                    colname = (tablename + '.' + field.name, field.name)
            elif not isinstance(query, Expression):
                colname = (field.name, field.name)
            colnames.append(colname[1])
            newnames.append(colname[0])

        for record in mongo_list_dicts:
            row = []
            for colname in colnames:
                try:
                    value = record[colname]
                except:
                    value = None
                if self.server_version_major < 2.6:
                    # '$size' not present in server versions < 2.6
                    if isinstance(value, list) and '$addToSet' in colname:
                        value = len(value)

                row.append(value)
            rows.append(row)
        if not rows:
            rows = null_rows

        processor = attributes.get('processor', self.parse)
        result = processor(rows, fields, newnames, blob_decode=True)
        return result

    def check_notnull(self, table, values):
        for fieldname in table._notnulls:
            if fieldname not in values or values[fieldname] is None:
                raise Exception("NOT NULL constraint failed: %s" % fieldname)

    def check_unique(self, table, values):
        if len(table._uniques) > 0:
            db = table._db
            unique_queries = []
            for fieldname in table._uniques:
                if fieldname in values:
                    value = values[fieldname]
                else:
                    value = table[fieldname].default
                unique_queries.append(
                    Query(db, self.EQ, table[fieldname], value))

            if len(unique_queries) > 0:
                unique_query = unique_queries[0]

                # if more than one field, build a query of ORs
                for query in unique_queries[1:]:
                    unique_query = Query(db, self.OR, unique_query, query)

                if self.count(unique_query, distinct=False) != 0:
                    for query in unique_queries:
                        if self.count(query, distinct=False) != 0:
                            # one of the 'OR' queries failed, see which one
                            raise Exception("NOT UNIQUE constraint failed: %s" %
                                          query.first.name)

    def insert(self, table, fields, safe=None):
        """Safe determines whether a asynchronous request is done or a
        synchronous action is done
        For safety, we use by default synchronous requests"""

        values = {}
        safe = self._get_safe(safe)
        ctable = self._get_collection(table._tablename, safe)

        for k, v in fields:
            if k.name not in ["id", "safe"]:
                fieldname = k.name
                fieldtype = table[k.name].type
                values[fieldname] = self.represent(v, fieldtype)

        # validate notnulls
        try:
            self.check_notnull(table, values)
        except Exception as e:
            if hasattr(table, '_on_insert_error'):
                return table._on_insert_error(table, fields, e)
            raise e

        # validate uniques
        try:
            self.check_unique(table, values)
        except Exception as e:
            if hasattr(table, '_on_insert_error'):
                return table._on_insert_error(table, fields, e)
            raise e

        # perform the insert
        result = ctable.insert_one(values)

        if result.acknowledged:
            Oid = result.inserted_id
            rid = Reference(long(str(Oid), 16))
            (rid._table, rid._record) = (table, None)
            return rid
        else:
            return None

    def update(self, tablename, query, fields, safe=None):
        # return amount of adjusted rows or zero, but no exceptions
        # @ related not finding the result
        if not isinstance(query, Query):
            raise RuntimeError("Not implemented")

        safe = self._get_safe(safe)
        if safe:
            amount = 0
        else:
            amount = self.count(query, distinct=False)
            if amount == 0:
                return amount

        expanded = MongoDBAdapter.Expanded(self, 'update', query, fields)
        ctable = expanded.get_collection(safe)
        if expanded.pipeline:
            try:
                for doc in ctable.aggregate(expanded.pipeline):
                    result = ctable.replace_one({'_id': doc['_id']}, doc)
                    if safe and result.acknowledged:
                        amount += result.matched_count
                return amount
            except Exception as e:
                # TODO Reverse update query to verify that the query succeeded
                raise RuntimeError("uncaught exception when updating rows: %s" % e)

        else:
            try:
                result = ctable.update_many(
                    filter=expanded.query_dict,
                    update={'$set': expanded.field_dicts})
                if safe and result.acknowledged:
                    amount = result.matched_count
                return amount
            except Exception as e:
                # TODO Reverse update query to verify that the query succeeded
                raise RuntimeError("uncaught exception when updating rows: %s" % e)

    def delete(self, tablename, query, safe=None):
        if not isinstance(query, Query):
            raise RuntimeError("query type %s is not supported" % type(query))

        safe = self._get_safe(safe)
        expanded = MongoDBAdapter.Expanded(self, 'delete', query)
        ctable = expanded.get_collection(safe)
        if expanded.pipeline:
            deleted = [x['_id'] for x in ctable.aggregate(expanded.pipeline)]
        else:
            deleted = [x['_id'] for x in ctable.find(expanded.query_dict)]

        # find references to deleted items
        db = self.db
        table = db[tablename]
        cascade = []
        set_null = []
        for field in table._referenced_by:
            if field.type == 'reference '+ tablename:
                if field.ondelete == 'CASCADE':
                    cascade.append(field)
                if field.ondelete == 'SET NULL':
                    set_null.append(field)
        cascade_list = []
        set_null_list = []
        for field in table._referenced_by_list:
            if field.type == 'list:reference '+ tablename:
                if field.ondelete == 'CASCADE':
                    cascade_list.append(field)
                if field.ondelete == 'SET NULL':
                    set_null_list.append(field)

        # perform delete
        result = ctable.delete_many({"_id": { "$in": deleted }})
        if result.acknowledged:
            amount = result.deleted_count
        else:
            amount = len(deleted)

        # clean up any references
        if amount and deleted:
            # ::TODO:: test if deleted references cascade
            def remove_from_list(field, deleted, safe):
                for delete in deleted:
                    modify = {field.name: delete}
                    dtable = self._get_collection(field.tablename, safe)
                    result = dtable.update_many(
                        filter=modify, update={'$pull': modify})

            # for cascaded items, if the reference is the only item in the list,
            # then remove the entire record, else delete reference from the list
            for field in cascade_list:
                for delete in deleted:
                    modify = {field.name: [delete]}
                    dtable = self._get_collection(field.tablename, safe)
                    result = dtable.delete_many(filter=modify)
                remove_from_list(field, deleted, safe)
            for field in set_null_list:
                remove_from_list(field, deleted, safe)
            for field in cascade:
                db(field.belongs(deleted)).delete()
            for field in set_null:
                db(field.belongs(deleted)).update(**{field.name:None})

        return amount

    def bulk_insert(self, table, items):
        return [self.insert(table, item) for item in items]

    ## OPERATORS
    def needs_mongodb_aggregation_pipeline(f):
        def mark_pipeline(self, first, *args, **kwargs):
            self.parse_data(first, 'pipeline', True)
            if len(args) > 0:
                self.parse_data(args[0], 'pipeline', True)
            return f(self, first, *args, **kwargs)
        return mark_pipeline

    def INVERT(self, first):
        #print "in invert first=%s" % first
        return '-%s' % self.expand(first)

    def NOT(self, first):
        op = self.expand(first)
        op_k = list(op)[0]
        op_body = op[op_k]
        r = None
        if type(op_body) is list:
            # apply De Morgan law for and/or
            # not(A and B) -> not(A) or not(B)
            # not(A or B)  -> not(A) and not(B)
            not_op = '$and' if op_k == '$or' else '$or'
            r = {not_op: [self.NOT(first.first), self.NOT(first.second)]}
        else:
            try:
                sub_ops = list(op_body.keys())
                if len(sub_ops) == 1 and sub_ops[0] == '$ne':
                    r = {op_k: op_body['$ne']}
            except AttributeError:
                r = {op_k: {'$ne': op_body}}
            if r is None:
                r = {op_k: {'$not': op_body}}
        return r

    def AND(self, first, second):
        # pymongo expects: .find({'$and': [{'x':'1'}, {'y':'2'}]})
        if isinstance(second, bool):
            if second:
                return self.expand(first)
            return self.NE(first, first)
        return {'$and': [self.expand(first), self.expand(second)]}

    def OR(self, first, second):
        # pymongo expects: .find({'$or': [{'name':'1'}, {'name':'2'}]})
        if isinstance(second, bool):
            if not second:
                return self.expand(first)
            return True
        return {'$or': [self.expand(first), self.expand(second)]}

    def BELONGS(self, first, second):
        if isinstance(second, str):
            # this is broken, the only way second is a string is if it has
            # been converted to SQL.  This no worky.  This might be made to
            # work if _select did not return SQL.
            raise RuntimeError("nested queries not supported")
        items = [self.expand(item, first.type) for item in second]
        return {self.expand(first): {"$in": items}}

    def validate_second(f):
        def check_second(*args, **kwargs):
            if len(args) < 3 or args[2] is None:
                raise RuntimeError("Cannot compare %s with None" % args[1])
            return f(*args, **kwargs)
        return check_second

    def check_fields_for_cmp(f):
        def check_fields(self, first, second=None, *args, **kwargs):
            if (self.parse_data((first, second), 'pipeline')):
                pipeline = True
            elif not isinstance(first, Field) or self.has_field(second):
                pipeline = True
                self.parse_data((first, second), 'pipeline', True)
            else:
                pipeline = False
            return f(self, first, second, *args, pipeline=pipeline, **kwargs)
        return check_fields

    def CMP_OPS_AGGREGATION_PIPELINE(self, op, first, second):
        try:
            type = first.type
        except:
            type = None
        return {op: [self.expand(first), self.expand(second, type)]}

    @check_fields_for_cmp
    def EQ(self, first, second=None, pipeline=False):
        if pipeline:
            return self.CMP_OPS_AGGREGATION_PIPELINE('$eq', first, second)
        return {self.expand(first): self.expand(second, first.type)}

    @check_fields_for_cmp
    def NE(self, first, second=None, pipeline=False):
        if pipeline:
            return self.CMP_OPS_AGGREGATION_PIPELINE('$ne', first, second)
        return {self.expand(first): {'$ne': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def LT(self, first, second=None, pipeline=False):
        if pipeline:
            return self.CMP_OPS_AGGREGATION_PIPELINE('$lt', first, second)
        return {self.expand(first): {'$lt': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def LE(self, first, second=None, pipeline=False):
        if pipeline:
            return self.CMP_OPS_AGGREGATION_PIPELINE('$lte', first, second)
        return {self.expand(first): {'$lte': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def GT(self, first, second=None, pipeline=False):
        if pipeline:
            return self.CMP_OPS_AGGREGATION_PIPELINE('$gt', first, second)
        return {self.expand(first): {'$gt': self.expand(second, first.type)}}

    @validate_second
    @check_fields_for_cmp
    def GE(self, first, second=None, pipeline=False):
        if pipeline:
            return self.CMP_OPS_AGGREGATION_PIPELINE('$gte', first, second)
        return {self.expand(first): {'$gte': self.expand(second, first.type)}}

    @needs_mongodb_aggregation_pipeline
    def ADD(self, first, second):
        op_code = '$add'
        for field in [first, second]:
            try:
                if field.type in ['string', 'text', 'password']:
                    op_code = '$concat'
                    break
            except:
                pass
        return {op_code: [self.expand(first), self.expand(second, first.type)]}

    @needs_mongodb_aggregation_pipeline
    def SUB(self, first, second):
        return {'$subtract': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_mongodb_aggregation_pipeline
    def MUL(self, first, second):
        return {'$multiply': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_mongodb_aggregation_pipeline
    def DIV(self, first, second):
        return {'$divide': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_mongodb_aggregation_pipeline
    def MOD(self, first, second):
        return {'$mod': [
            self.expand(first), self.expand(second, first.type)]}

    _aggregate_map = {
        'SUM': '$sum',
        'MAX': '$max',
        'MIN': '$min',
        'AVG': '$avg',
    }

    @needs_mongodb_aggregation_pipeline
    def AGGREGATE(self, first, what):
        if what == 'ABS':
            return {"$cond": [
                        {"$lt": [self.expand(first), 0]},
                        {"$subtract": [0, self.expand(first)]},
                        self.expand(first)
                        ]}
        try:
            expanded = {self._aggregate_map[what]: self.expand(first)}
        except KeyError:
            raise NotImplementedError("'%s' not implemented" % what)

        self.parse_data(first, 'need_group', True)
        return {MongoDBAdapter.GROUP_MARK: expanded}

    @needs_mongodb_aggregation_pipeline
    def COUNT(self, first, distinct=None):
        self.parse_data(first, 'need_group', True)
        if distinct:
            ret = {MongoDBAdapter.GROUP_MARK:
                {"$addToSet": self.expand(first)}}
            if self.server_version_major >= 2.6:
                # '$size' not present in server versions < 2.6
                ret = {'$size': ret}
            return ret
        return {MongoDBAdapter.GROUP_MARK: {"$sum": 1}}

    _extract_map = {
        'dayofyear':    '$dayOfYear',
        'day':          '$dayOfMonth',
        'dayofweek':    '$dayOfWeek',
        'year':         '$year',
        'month':        '$month',
        'week':         '$week',
        'hour':         '$hour',
        'minute':       '$minute',
        'second':       '$second',
        'millisecond':  '$millisecond',
        'string':       '$dateToString',
    }

    @needs_mongodb_aggregation_pipeline
    def EXTRACT(self, first, what):
        try:
            return {self._extract_map[what]: self.expand(first)}
        except KeyError:
            raise NotImplementedError("EXTRACT(%s) not implemented" % what)

    @needs_mongodb_aggregation_pipeline
    def EPOCH(self, first):
        return {"$divide": [{"$subtract": [self.expand(first), self.epoch]}, 1000]}

    @needs_mongodb_aggregation_pipeline
    def EXPAND_CASE(self, query, true_false):
        return {"$cond": [self.expand(query),
                          self.expand(true_false[0]),
                          self.expand(true_false[1])]}

    @needs_mongodb_aggregation_pipeline
    def AS(self, first, second):
        # put the AS_MARK into the structure.  The 'AS' name will be parsed
        # later from the string of the field name.
        if isinstance(first, Field):
            return [{MongoDBAdapter.AS_MARK: second}, self.expand(first)]
        else:
            result = self.expand(first)
            result[MongoDBAdapter.AS_MARK] = second
        return result

    # We could implement an option that simulates a full featured SQL
    # database. But I think the option should be set explicit or
    # implemented as another library.
    def ON(self, first, second):
        raise MongoDBAdapter.NotOnNoSqlError()

    def COMMA(self, first, second):
        # returns field name lists, to be separated via split(',')
        return '%s,%s' % (self.expand(first), self.expand(second))

    #TODO verify full compatibilty with official SQL Like operator
    def _build_like_regex(self, first, second,
                          case_sensitive=True,
                          escape=None,
                          ends_with=False,
                          starts_with=False,
                          whole_string=True,
                          like_wildcards=False):
        import re
        base = self.expand(second, 'string')
        need_regex = (whole_string or not case_sensitive
                      or starts_with or ends_with
                      or like_wildcards and ('_' in base or '%' in base))
        if not need_regex:
            return base
        else:
            expr = re.escape(base)
            if like_wildcards:
                if escape:
                    # protect % and _ which are escaped
                    expr = expr.replace(escape+'\\%', '%')
                    if PY2:
                        expr = expr.replace(escape+'\\_', '_')
                    elif escape+'_' in expr:
                        set_aside = str(self.object_id('<random>'))
                        while set_aside in expr:
                            set_aside = str(self.object_id('<random>'))
                        expr = expr.replace(escape+'_', set_aside)
                    else:
                        set_aside = None
                expr = expr.replace('\\%', '.*')
                if PY2:
                    expr = expr.replace('\\_', '.')
                else:
                    expr = expr.replace('_', '.')
                if escape:
                    # convert to protected % and _
                    expr = expr.replace('%', '\\%')
                    if PY2:
                        expr = expr.replace('_', '\\_')
                    elif set_aside:
                        expr = expr.replace(set_aside, '_')
            if starts_with:
                pattern = '^%s'
            elif ends_with:
                pattern = '%s$'
            elif whole_string:
                pattern = '^%s$'
            else:
                pattern = '%s'

            return self.REGEXP(first, pattern % expr, case_sensitive)

    def LIKE(self, first, second, case_sensitive=True, escape=None):
        return self._build_like_regex(first, second, 
            case_sensitive=case_sensitive, escape=escape, like_wildcards=True)

    def ILIKE(self, first, second, escape=None):
        return self.LIKE(first, second, case_sensitive=False, escape=escape)

    def STARTSWITH(self, first, second):
        return self._build_like_regex(first, second, starts_with=True)

    def ENDSWITH(self, first, second):
        return self._build_like_regex(first, second, ends_with=True)

    #TODO verify full compatibilty with official oracle contains operator
    def CONTAINS(self, first, second, case_sensitive=True):
        if isinstance(second, self.ObjectId):
            ret = {self.expand(first): second}

        elif isinstance(second, Field):
            if second.type in ['string', 'text']:
                if isinstance(first, Field):
                    if first.type in ['list:string', 'string', 'text']:
                        ret = {'$where': "this.%s.indexOf(this.%s) > -1"
                            % (first.name, second.name)}
                    else:
                        raise NotImplementedError("field.CONTAINS() not "
                            + "implemented for field type of '%s'"
                            % first.type)
                else:
                    raise NotImplementedError(
                        "x.CONTAINS() not implemented for x type of '%s'"
                        % type(first))

            elif second.type in ['integer', 'bigint']:
                ret = {'$where': "this.%s.indexOf(this.%s + '') > -1"
                        % (first.name, second.name)}
            else:
                raise NotImplementedError(
                    "CONTAINS(field) not implemented for field type '%s'"
                    % second.type)

        elif isinstance(second, (basestring, int)):
            whole_string = (isinstance(first, Field)
                            and first.type == 'list:string')
            ret = self._build_like_regex(first, second, 
                case_sensitive=case_sensitive, whole_string=whole_string)

            # first.type in ('string', 'text', 'json', 'upload')
            # or first.type.startswith('list:'):

        else:
            raise NotImplementedError(
                "CONTAINS() not implemented for type '%s'" % type(second))

        return ret

    @needs_mongodb_aggregation_pipeline
    def SUBSTRING(self, field, parameters):
        def parse_parameters(pos0, length):
            """
            The expression object can return these as string based expressions.
            We can't use that so we have to tease it apart.

            These are the possibilities:

              pos0 = '(%s - %d)' % (self.len(), abs(start) - 1)
              pos0 = start + 1

              length = self.len()
              length = '(%s - %d - %s)' % (self.len(), abs(stop) - 1, pos0)
              length = '(%s - %s)' % (stop + 1, pos0)

            Two of these five require the length of the string which is not
            supported by Mongo, so for now these cause an Exception and
            won't reach here.

            If this were to ever be supported it may require a change to
            Expression.__getitem__ so that it either returned the base
            expression to be expanded here, or converted length to a string
            to be parsed back to a call to STRLEN()
            """
            if isinstance(length, basestring):
                return (pos0 - 1, eval(length))
            else:
                # take the rest of the string
                return (pos0 - 1, -1)

        parameters = parse_parameters(*parameters)
        return {'$substr': [self.expand(field), parameters[0], parameters[1]]}

    @needs_mongodb_aggregation_pipeline
    def LOWER(self, first):
        return {'$toLower': self.expand(first)}

    @needs_mongodb_aggregation_pipeline
    def UPPER(self, first):
        return {'$toUpper': self.expand(first)}

    def REGEXP(self, first, second, case_sensitive=True):
        """ MongoDB provides regular expression capabilities for pattern
            matching strings in queries. MongoDB uses Perl compatible
            regular expressions (i.e. 'PCRE') version 8.36 with UTF-8 support.
        """
        if (isinstance(first, Field) and
                first.type in ['integer', 'bigint', 'float', 'double']):
            return {'$where': "RegExp('%s').test(this.%s + '')"
                % (self.expand(second, 'string'), first.name)}

        expanded_first = self.expand(first)
        regex_second = {'$regex': self.expand(second, 'string')}
        if not case_sensitive:
            regex_second['$options'] = 'i'
        if (self.parse_data((first, second), 'pipeline')):
            name = str(expanded_first)
            return {MongoDBAdapter.REGEXP_MARK1: {name: expanded_first},
                    MongoDBAdapter.REGEXP_MARK2: {name: regex_second}}
        try:
            return {expanded_first: regex_second}
        except TypeError:
            # if first is not hashable, then will need the pipeline
            self.parse_data((first, second), 'pipeline', True)
            return {}

    def LENGTH(self, first):
        """ https://jira.mongodb.org/browse/SERVER-5319
            https://github.com/afchin/mongo/commit/f52105977e4d0ccb53bdddfb9c4528a3f3c40bdf
        """
        raise NotImplementedError()

    @needs_mongodb_aggregation_pipeline
    def COALESCE(self, first, second):
        if len(second) > 1:
            second = [self.COALESCE(second[0], second[1:])]
        return {"$ifNull": [self.expand(first), self.expand(second[0])]}

    def RANDOM(self):
        """ ORDER BY RANDOM()

            https://github.com/mongodb/cookbook/blob/master/content/patterns/random-attribute.txt
            https://jira.mongodb.org/browse/SERVER-533
            http://stackoverflow.com/questions/19412/how-to-request-a-random-row-in-sql
        """
        raise NotImplementedError()

    class NotOnNoSqlError(NotImplementedError):
        def __init__(self, message=None):
            if message is None:
                message = "Not Supported on NoSQL databases"
            super(MongoDBAdapter.NotOnNoSqlError, self).__init__(message)


class MongoBlob(Binary):
    MONGO_BLOB_BYTES        = USER_DEFINED_SUBTYPE
    MONGO_BLOB_NON_UTF8_STR = USER_DEFINED_SUBTYPE + 1

    def __new__(cls, value):
        # return None and Binary() unmolested
        if value is None or isinstance(value, Binary):
            return value

        # bytearray is marked as MONGO_BLOB_BYTES
        if isinstance(value, bytearray):
            return Binary.__new__(cls, bytes(value), MongoBlob.MONGO_BLOB_BYTES)

        # return non-strings as Binary(), eg: PY3 bytes()
        if not isinstance(value, basestring):
            return Binary(value)

        # if string is encodable as UTF-8, then return as string
        try:
            value.encode('utf-8')
            return value
        except UnicodeDecodeError:
            # string which can not be UTF-8 encoded, eg: pickle strings
            return Binary.__new__(cls, value, MongoBlob.MONGO_BLOB_NON_UTF8_STR)

    def __repr__(self):
        return repr(MongoBlob.decode(self))

    @staticmethod
    def decode(value):
        if isinstance(value, Binary):
            if value.subtype == MongoBlob.MONGO_BLOB_BYTES:
                return bytearray(value)
            if value.subtype == MongoBlob.MONGO_BLOB_NON_UTF8_STR:
                return str(value)
        return value
