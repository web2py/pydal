# -*- coding: utf-8 -*-
import datetime
import re

from .._globals import IDENTITY
from .._compat import integer_types, basestring
from ..objects import Table, Query, Field, Expression
from ..helpers.classes import SQLALL, Reference
from ..helpers.methods import use_common_filters, xorify
from .base import NoSQLAdapter
try:
    from bson import Binary
    from bson.binary import USER_DEFINED_SUBTYPE
except:
    class Binary(object):
        pass
    USER_DEFINED_SUBTYPE = 0

long = integer_types[-1]


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
            if int(version.split('.')[0]) < 3:
                raise Exception(
                    "pydal requires pymongo version >= 3.0, found '%s'"
                    % pymongo_version)

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
                arg = int("0x%sL" %
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

        if not isinstance(arg, (int, long)):
            raise TypeError("object_id argument must be of type " +
                            "ObjectId or an objectid representable integer")
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
        # the base adapter does not support MongoDB ObjectId
        if isinstance(obj, self.ObjectId):
            value = obj
        else:
            value = NoSQLAdapter.represent(self, obj, fieldtype)
        # reference types must be convert to ObjectID
        if fieldtype == 'date':
            if value is None:
                return value
            # this piece of data can be stripped off based on the fieldtype
            t = datetime.time(0, 0, 0)
            # mongodb doesn't has a date object and so it must datetime,
            # string or integer
            return datetime.datetime.combine(value, t)
        elif fieldtype == 'time':
            if value is None:
                return value
            # this piece of data can be stripped of based on the fieldtype
            d = datetime.date(2000, 1, 1)
            # mongodb doesn't has a  time object and so it must datetime,
            # string or integer
            return datetime.datetime.combine(d, value)
        elif fieldtype == "blob":
            return MongoBlob(value)
        elif isinstance(fieldtype, basestring):
            if fieldtype.startswith('list:'):
                if fieldtype.startswith('list:reference'):
                    value = [self.object_id(v) for v in value]
            elif fieldtype.startswith("reference") or fieldtype == "id":
                value = self.object_id(value)
        elif isinstance(fieldtype, Table):
            value = self.object_id(value)
        return value

    def parse_blob(self, value, field_type):
        return MongoBlob.decode(value)

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
                     polymodel=None, is_capped=False):
        if is_capped:
            raise RuntimeError("Not implemented")
        table._dbt = None

    class Expanded (object):
        """
        Class to encapsulate a pydal expression and track the parse expansion
        and its results.
        """
        def __init__ (self, adapter, crud, query, fields=(), tablename=None):
            self.adapter = adapter
            self.parse_data = {'aggregate': False}
            self.crud = crud
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

            if self.parse_data['aggregate']:
                # if the query needs the aggregation engine, set that up
                if self.query_dict:
                    self.pipeline = [{'$project': 
                                      {'query': adapter.expand(self.query)}}]
                self.query_dict = None
            else:
                # expand the fields
                try:
                    self._expand_fields(self._fields_loop_abort)
                except StopIteration:
                    pass
            if self.parse_data['aggregate']:
                # expand the fields for the aggregation engine
                self._expand_fields(None)

            if crud == 'update':
                if self.parse_data['aggregate']:
                    for fieldname in adapter.db[self.tablename].fields:
                        # add all fields to projection to pass them through
                        if fieldname not in self.field_dicts:
                            if fieldname not in ("_id", "id"):
                                self.field_dicts[fieldname] = 1
                    self.pipeline.append({'$project': self.field_dicts})
                    self.field_dicts = adapter.SON()

                else:
                    # do not update id fields
                    for fieldname in ("_id", "id"):
                        if fieldname in self.field_dicts:
                            self.field_dicts.delete(fieldname)

            elif crud == 'select':
                if self.parse_data['aggregate']:
                    self.field_dicts['_id'] = None
                    self.pipeline.append({'$group': self.field_dicts})
                    self.field_dicts = adapter.SON()

            elif crud == 'count':
                if self.parse_data['aggregate']:
                    self.pipeline.append({'$match': {'query': True}})
                    self.pipeline.append(
                        {'$group': {"_id": None, crud : {"$sum": 1}}})

        def _expand_fields(self, mid_loop):
            if self.crud == 'update':
                mid_loop = mid_loop or self._fields_loop_update_aggregate
                for field, value in self.values:
                    self._expand_field(field, value, mid_loop)
            else:
                for field in self.fields:
                    self._expand_field(field, field, mid_loop)

        def _expand_field(self, field, value, mid_loop):
            expanded = {}
            if isinstance(field, Field):
                expanded = self.adapter.expand(value, field.type)
            elif isinstance(field, Expression):
                expanded = self.adapter.expand(field)
                field.name = str(expanded)

            if mid_loop:
                expanded = mid_loop(expanded, field, value)
            self.field_dicts[field.name] = expanded

        def _fields_loop_abort(self, expanded, *args):
            # if we need the aggregation engine, then start over
            if self.parse_data['aggregate']:
                self.field_dicts = self.adapter.SON()
                if self.query_dict:
                    self.pipeline = [{'$match': self.query_dict}]
                self.query_dict = {}
                raise StopIteration()
            return expanded

        def _fields_loop_update_aggregate(self, expanded, field, value):
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

        def annotate_expression(self, expression):
            import types

            def get_child(self, element):
                child = getattr(self, element)
                if isinstance(child, (Expression, Query)):
                    child.parse_root = self.parse_root
                    child.parse_parent = self
                    child.parse_depth = self.parse_depth + 1
                    child.parse_data = self.parse_data
                    child.get_child = types.MethodType(get_child, child)
                return child

            if isinstance(expression, (Expression, Query)):
                expression.parse_root = expression
                expression.parse_parent = self
                expression.parse_depth = 0
                expression.parse_data = self.parse_data
                expression.parse_data['test'] = 1
                expression.get_child = types.MethodType(get_child, expression)

            return expression

        def get_collection(self, safe=None):
            return self.adapter._get_collection(self.tablename, safe)

    @staticmethod
    def parse_data(expression, attribute, value=None):
        if value is not None:
            try:
                expression.parse_data[attribute] = value
            except AttributeError:
                return None
        try:
            return expression.parse_data[attribute]
        except AttributeError:
            return None

    def expand(self, expression, field_type=None):

        if isinstance(expression, Field):
            if expression.type == 'id':
                result = "_id"
            else:
                result = expression.name
            if self.parse_data(expression, 'aggregate'):
                result = '$' + result

        elif isinstance(expression, (Expression, Query)):
            try:
                first = expression.get_child('first')
                second = expression.get_child('second')
            except AttributeError:
                return self.expand(MongoDBAdapter.Expanded(
                    self, '', expression), field_type).query

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

        elif field_type:
            result = self.represent(expression, field_type)

        elif isinstance(expression, (list, tuple)):
            result = [self.represent(item, field_type) for item in expression]
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
        ctable.remove(None, w=self._get_safe(safe))

    def count(self, query, distinct=None, snapshot=True):
        if distinct:
            raise RuntimeError("COUNT DISTINCT not supported")
        if not isinstance(query, Query):
            raise SyntaxError("Not Supported")

        expanded = MongoDBAdapter.Expanded(self, 'count', query)
        ctable = expanded.get_collection()
        if not expanded.parse_data['aggregate']:
            return ctable.count(filter=expanded.query_dict)
        else:
            for record in ctable.aggregate(expanded.pipeline):
                return record['count']

    def select(self, query, fields, attributes, snapshot=False):
        mongofields_dict = self.SON()
        new_fields, mongosort_list = [], []
        # try an orderby attribute
        orderby = attributes.get('orderby', False)
        limitby = attributes.get('limitby', False)
        # distinct = attributes.get('distinct', False)
        if 'for_update' in attributes:
            self.db.logger.warning('mongodb does not support for_update')
        for key in set(attributes.keys())-set(('limitby', 
                                                'orderby', 'for_update')):
            if attributes[key] is not None:
                self.db.logger.warning(
                    'select attribute not implemented: %s' % key)
        if limitby:
            limitby_skip, limitby_limit = limitby[0], int(limitby[1]) - 1
        else:
            limitby_skip = limitby_limit = 0
        if orderby:
            if isinstance(orderby, (list, tuple)):
                orderby = xorify(orderby)
            # !!!! need to add 'random'
            for f in self.expand(orderby).split(','):
                if f.startswith('-'):
                    mongosort_list.append((f[1:], -1))
                else:
                    mongosort_list.append((f, 1))
        for item in fields:
            if isinstance(item, SQLALL):
                new_fields += item._table
            else:
                new_fields.append(item)
        fields = new_fields
        if isinstance(query, Query):
            tablename = self.get_table(query)
        elif len(fields) != 0:
            if isinstance(fields[0], Expression):
                tablename = self.get_table(fields[0])
            else:
                tablename = fields[0].tablename
        else:
            raise SyntaxError("The table name could not be found in " +
                              "the query nor from the select statement.")

        if query:
            if use_common_filters(query):
                query = self.common_filter(query, [tablename])

        expanded = MongoDBAdapter.Expanded(
            self, 'select', query, fields or self.db[tablename])
        ctable = self.connection[tablename]
        modifiers = {'snapshot':snapshot}

        if not expanded.parse_data['aggregate']:
            mongo_list_dicts = ctable.find(
                expanded.query_dict, expanded.field_dicts, skip=limitby_skip,
                limit=limitby_limit, sort=mongosort_list, modifiers=modifiers)
            null_rows = []
        else:
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
                row.append(value)
            rows.append(row)
        if not rows:
            rows = null_rows

        processor = attributes.get('processor', self.parse)
        result = processor(rows, fields, newnames, blob_decode=True)
        return result

    def insert(self, table, fields, safe=None):
        """Safe determines whether a asynchronous request is done or a
        synchronous action is done
        For safety, we use by default synchronous requests"""

        values = {}
        ctable = self._get_collection(table._tablename, safe)

        for k, v in fields:
            if k.name not in ["id", "safe"]:
                fieldname = k.name
                fieldtype = table[k.name].type
                values[fieldname] = self.represent(v, fieldtype)

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
        if expanded.parse_data['aggregate']:
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

        expanded = MongoDBAdapter.Expanded(self, 'delete', query)
        ctable = expanded.get_collection(safe)
        _filter = expanded.query_dict

        deleted = [x['_id'] for x in ctable.find(_filter)]

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
        result = ctable.delete_many(_filter)
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
    def needs_mongodb_aggregation(f):
        def mark_aggregate(*args, **kwargs):
            if len(args) > 1:
                args[0].parse_data(args[1], 'aggregate', True)
            if len(args) > 2:
                args[0].parse_data(args[2], 'aggregate', True)
            return f(*args, **kwargs)
        return mark_aggregate

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
        return {'$and': [self.expand(first), self.expand(second)]}

    def OR(self, first, second):
        # pymongo expects: .find({'$or': [{'name':'1'}, {'name':'2'}]})
        return {'$or': [self.expand(first), self.expand(second)]}

    def BELONGS(self, first, second):
        if isinstance(second, str):
            # this is broken, the only way second is a string is if it has
            # been converted to SQL.  This no worky.  This might be made to
            # work if _select did not return SQL.
            raise RuntimeError("nested queries not supported")
        items = [self.expand(item, first.type) for item in second]
        return {self.expand(first) : {"$in" : items} }

    def _validate_second(self, first, second):
        if second is None:
            raise RuntimeError("Cannot compare %s with None" % first)

    @needs_mongodb_aggregation
    def CMP_OPS_AGGREGATE(self, op, first, second):
        return {op: [self.expand(first), self.expand(second, first.type)]}

    def EQ(self, first, second=None):
        if not isinstance(first, Field):
            return self.CMP_OPS_AGGREGATE('$eq', first, second)
        return {self.expand(first): self.expand(second, first.type)}

    def NE(self, first, second=None):
        if not isinstance(first, Field):
            return self.CMP_OPS_AGGREGATE('$ne', first, second)
        return {self.expand(first): {'$ne': self.expand(second, first.type)}}

    def LT(self, first, second=None):
        self._validate_second(first, second)
        if not isinstance(first, Field):
            return self.CMP_OPS_AGGREGATE('$lt', first, second)
        return {self.expand(first): {'$lt': self.expand(second, first.type)}}

    def LE(self, first, second=None):
        self._validate_second(first, second)
        if not isinstance(first, Field):
            return self.CMP_OPS_AGGREGATE('$lte', first, second)
        return {self.expand(first): {'$lte': self.expand(second, first.type)}}

    def GT(self, first, second=None):
        self._validate_second(first, second)
        if not isinstance(first, Field):
            return self.CMP_OPS_AGGREGATE('$gt', first, second)
        return {self.expand(first): {'$gt': self.expand(second, first.type)}}

    def GE(self, first, second=None):
        self._validate_second(first, second)
        if not isinstance(first, Field):
            return self.CMP_OPS_AGGREGATE('$gte', first, second)
        return {self.expand(first): {'$gte': self.expand(second, first.type)}}

    @needs_mongodb_aggregation
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

    @needs_mongodb_aggregation
    def SUB(self, first, second):
        return {'$subtract': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_mongodb_aggregation
    def MUL(self, first, second):
        return {'$multiply': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_mongodb_aggregation
    def DIV(self, first, second):
        return {'$divide': [
            self.expand(first), self.expand(second, first.type)]}

    @needs_mongodb_aggregation
    def MOD(self, first, second):
        return {'$mod': [
            self.expand(first), self.expand(second, first.type)]}

    _aggregate_map = {
        'SUM': '$sum',
        'MAX': '$max',
        'MIN': '$min',
        'AVG': '$avg',
    }

    @needs_mongodb_aggregation
    def AGGREGATE(self, first, what):
        try:
            return {self._aggregate_map[what]: self.expand(first)}
        except:
            raise NotImplementedError("'%s' not implemented" % what)

    @needs_mongodb_aggregation
    def COUNT(self, first, distinct=None):
        if distinct:
            raise NotImplementedError("distinct not implmented for count op")
        return {"$sum": 1}

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

    @needs_mongodb_aggregation
    def EXTRACT(self, first, what):
        return {self._extract_map[what]: self.expand(first)}

    @needs_mongodb_aggregation
    def EPOCH(self, first):
        return {"$divide": [{"$subtract": [self.expand(first), self.epoch]}, 1000]}

    def AS(self, first, second):
        raise NotImplementedError("javascript_needed")

    # We could implement an option that simulates a full featured SQL
    # database. But I think the option should be set explicit or
    # implemented as another library.
    def JOIN(self):
        raise MongoDBAdapter.NotOnNoSqlError()

    def LEFT_JOIN(self):
        raise MongoDBAdapter.NotOnNoSqlError()

    def ON(self, first, second):
        raise MongoDBAdapter.NotOnNoSqlError()

    def COMMA(self, first, second):
        #::TODO:: understand (fix) this
        return '%s, %s' % (self.expand(first), self.expand(second))

    #TODO verify full compatibilty with official SQL Like operator
    def _build_like_regex(self, arg,
                          case_sensitive=True,
                          ends_with=False,
                          starts_with=False,
                          whole_string=True,
                          like_wildcards=False):
        import re
        base = self.expand(arg, 'string')
        need_regex = (whole_string or not case_sensitive
                      or starts_with or ends_with
                      or like_wildcards and ('_' in base or '%' in base))
        if not need_regex:
            return base
        else:
            expr = re.escape(base)
            if like_wildcards:
                expr = expr.replace('\\%', '.*')
                expr = expr.replace('\\_', '.').replace('_', '.')
            if starts_with:
                pattern = '^%s'
            elif ends_with:
                pattern = '%s$'
            elif whole_string:
                pattern = '^%s$'
            else:
                pattern = '%s'

            regex = {'$regex': pattern % expr}
            if not case_sensitive:
                regex['$options'] = 'i'
            return regex

    def LIKE(self, first, second, case_sensitive=True, escape=None):
        regex = self._build_like_regex(
            second, case_sensitive=case_sensitive, like_wildcards=True)
        return {self.expand(first): regex}

    def ILIKE(self, first, second, escape=None):
        return self.LIKE(first, second, case_sensitive=False, escape=escape)

    def STARTSWITH(self, first, second):
        regex = self._build_like_regex(second, starts_with=True)
        return {self.expand(first): regex}

    def ENDSWITH(self, first, second):
        regex = self._build_like_regex(second, ends_with=True)
        return {self.expand(first): regex}

    #TODO verify full compatibilty with official oracle contains operator
    def CONTAINS(self, first, second, case_sensitive=True):
        ret = None
        if isinstance(second, self.ObjectId):
            val = second

        elif isinstance(first, Field) and first.type == 'list:string':
            if isinstance(second, Field) and second.type == 'string':
                ret = {
                    '$where':
                    "this.%s.indexOf(this.%s) > -1" % (first.name, second.name)
                }
            else:
                val = self._build_like_regex(
                    second, case_sensitive=case_sensitive, whole_string=True)
        else:
            val = self._build_like_regex(
                second, case_sensitive=case_sensitive, whole_string=False)

        if not ret:
            ret = {self.expand(first): val}

        return ret

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
