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

    error_messages = {"javascript_needed": "This must yet be replaced" +
                      " with javascript in order to work."}

    def __init__(self, db, uri='mongodb://127.0.0.1:5984/db',
                 pool_size=0, folder=None, db_codec='UTF-8',
                 credential_decoder=IDENTITY, driver_args={},
                 adapter_args={}, do_connect=True, after_connection=None):

        self.db = db
        self.uri = uri
        if do_connect: self.find_driver(adapter_args)
        import random
        from bson.objectid import ObjectId
        from bson.son import SON
        import pymongo.uri_parser
        from pymongo.write_concern import WriteConcern

        m = pymongo.uri_parser.parse_uri(uri)

        self.SON = SON
        self.ObjectId = ObjectId
        self.random = random
        self.WriteConcern = WriteConcern

        self.dbengine = 'mongodb'
        self.folder = folder
        db['_lastsql'] = ''
        self.db_codec = 'UTF-8'
        self._after_connection = after_connection
        self.pool_size = pool_size
        self.find_or_make_work_folder()
        #this is the minimum amount of replicates that it should wait
        # for on insert/update
        self.minimumreplication = adapter_args.get('minimumreplication', 0)
        # by default all inserts and selects are performand asynchronous,
        # but now the default is
        # synchronous, except when overruled by either this default or
        # function parameter
        self.safe = 1 if adapter_args.get('safe', True) else 0

        if isinstance(m, tuple):
            m = {"database": m[1]}
        if m.get('database') is None:
            raise SyntaxError("Database is required!")

        def connector(uri=self.uri, m=m):
            return self.driver.MongoClient(uri, w=self.safe)[m.get('database')]

        self.reconnect(connector, cursor=False)

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
                arg = int("0x%sL" % \
                "".join([self.random.choice("0123456789abcdef") \
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
        if fieldtype  =='date':
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
                    newval = []
                    for v in value:
                        newval.append(self.object_id(v))
                    value = newval
            elif fieldtype.startswith("reference") or fieldtype=="id":
                value = self.object_id(value)
            elif fieldtype == "string":
                value = str(value)
        elif isinstance(fieldtype, Table):
            value = self.object_id(value)
        return value

    def parse_blob(self, value, field_type):
        return MongoBlob.decode(value)

    def _expand_query(self, query, tablename=None, safe=None):
        """ Return a tuple containing query and ctable """
        if not tablename:
            tablename = self.get_table(query)
        ctable = self._get_collection(tablename, safe)
        _filter = None
        if query:
            if use_common_filters(query):
                query = self.common_filter(query,[tablename])
            _filter = self.expand(query)
        return (ctable, _filter)

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
                     polymodel=None, isCapped=False):
        if isCapped:
            raise RuntimeError("Not implemented")
        table._dbt = None

    def expand(self, expression, field_type=None):
        if isinstance(expression, Query):
            # any query using 'id':=
            # set name as _id (as per pymongo/mongodb primary key)
            # convert second arg to an objectid field
            # (if its not already)
            # if second arg is 0 convert to objectid
            if isinstance(expression.first,Field) and \
                    ((expression.first.type == 'id') or \
                    ("reference" in expression.first.type)):
                if expression.first.type == 'id':
                    expression.first.name = '_id'
                # cast to Mongo ObjectId
                if isinstance(expression.second, (tuple, list, set)):
                    expression.second = [self.object_id(item) for
                                         item in expression.second]
                else:
                    expression.second = self.object_id(expression.second)

        if isinstance(expression, Field):
            if expression.type=='id':
                result = "_id"
            else:
                result =  expression.name
        elif isinstance(expression, (Expression, Query)):
            first = expression.first
            second = expression.second
            op = expression.op
            optional_args = expression.optional_args or {}
            if not second is None:
                result = op(first, second, **optional_args)
            elif not first is None:
                result = op(first, **optional_args)
            else:
                result = op if isinstance(op, str) else op(**optional_args)
        elif field_type:
            result = self.represent(expression,field_type)
        elif isinstance(expression,(list,tuple)):
            result = [self.represent(item,field_type) for
                      item in expression]
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

        (ctable, _filter) = self._expand_query(query)
        result = ctable.count(filter=_filter)
        return result

    def select(self, query, fields, attributes, snapshot=False):
        mongofields_dict = self.SON()
        new_fields, mongosort_list = [], []
        # try an orderby attribute
        orderby = attributes.get('orderby', False)
        limitby = attributes.get('limitby', False)
        # distinct = attributes.get('distinct', False)
        if 'for_update' in attributes:
            self.db.logger.warning('mongodb does not support for_update')
        for key in set(attributes.keys())-set(('limitby', 'orderby',
                                               'for_update')):
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
            tablename = fields[0].tablename
        else:
            raise SyntaxError("The table name could not be found in " +
                              "the query nor from the select statement.")

        if query:
            if use_common_filters(query):
                query = self.common_filter(query,[tablename])

        mongoqry_dict = self.expand(query)

        fields = fields or self.db[tablename]
        for field in fields:
            mongofields_dict[field.name] = 1
        ctable = self.connection[tablename]
        modifiers={'snapshot':snapshot}

        mongo_list_dicts = ctable.find(
            mongoqry_dict, mongofields_dict, skip=limitby_skip,
            limit=limitby_limit, sort=mongosort_list, modifiers=modifiers)
        rows = []
        # populate row in proper order
        # Here we replace ._id with .id to follow the standard naming
        colnames = []
        newnames = []
        for field in fields:
            colname = str(field)
            colnames.append(colname)
            tablename, fieldname = colname.split(".")
            if fieldname == "_id":
                # Mongodb reserved uuid key
                field.name = "id"
            newnames.append(".".join((tablename, field.name)))

        for record in mongo_list_dicts:
            row = []
            for colname in colnames:
                tablename, fieldname = colname.split(".")
                # switch to Mongo _id uuids for retrieving
                # record id's
                if fieldname == "id": fieldname = "_id"
                if fieldname in record:
                    value = record[fieldname]
                else:
                    value = None
                row.append(value)
            rows.append(row)
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
            if not k.name in ["id", "safe"]:
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
        amount = self.count(query, False)
        if not isinstance(query, Query):
            raise SyntaxError("Not Supported")

        (ctable, _filter) = self._expand_query(query, tablename, safe)

        # do not try to update id fields to avoid backend errors
        modify = {'$set': dict((k.name, self.represent(v, k.type)) for
                  k, v in fields if (not k.name in ("_id", "id")))}
        try:
            result = ctable.update_many(filter=_filter,
                       update=modify)
            if result.acknowledged:
                amount = result.matched_count

            return amount
        except Exception as e:
            # TODO Reverse update query to verifiy that the query succeded
            raise RuntimeError("uncaught exception when updating rows: %s" % e)

    def delete(self, tablename, query, safe=None):
        amount = self.count(query, False)
        if not isinstance(query, Query):
            raise RuntimeError("query type %s is not supported" % \
                               type(query))

        (ctable, _filter) = self._expand_query(query, safe)

        result = ctable.delete_many(_filter)
        if result.acknowledged:
            return result.deleted_count
        else:
            return amount

        return amount

    def bulk_insert(self, table, items):
        return [self.insert(table,item) for item in items]

    ## OPERATORS
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
            except:
                r = {op_k: {'$ne': op_body}}
            if r == None:
                r = {op_k: {'$not': op_body}}
        return r

    def AND(self,first,second):
        # pymongo expects: .find({'$and': [{'x':'1'}, {'y':'2'}]})
        return {'$and': [self.expand(first),self.expand(second)]}

    def OR(self,first,second):
        # pymongo expects: .find({'$or': [{'name':'1'}, {'name':'2'}]})
        return {'$or': [self.expand(first),self.expand(second)]}

    def BELONGS(self, first, second):
        if isinstance(second, str):
            # this is broken, the only way second is a string is if it has
            # been converted to SQL.  This no worky.  This might be made to
            # work if _select did not return SQL.
            raise RuntimeError("nested queries not supported")
        items = [self.expand(item, first.type) for item in second]
        return {self.expand(first) : {"$in" : items} }

    def EQ(self,first,second=None):
        result = {}
        result[self.expand(first)] = self.expand(second, first.type)
        return result

    def NE(self, first, second=None):
        result = {}
        result[self.expand(first)] = {'$ne': self.expand(second, first.type)}
        return result

    def LT(self,first,second=None):
        if second is None:
            raise RuntimeError("Cannot compare %s < None" % first)
        result = {}
        result[self.expand(first)] = {'$lt': self.expand(second, first.type)}
        return result

    def LE(self,first,second=None):
        if second is None:
            raise RuntimeError("Cannot compare %s <= None" % first)
        result = {}
        result[self.expand(first)] = {'$lte': self.expand(second, first.type)}
        return result

    def GT(self,first,second):
        result = {}
        result[self.expand(first)] = {'$gt': self.expand(second, first.type)}
        return result

    def GE(self,first,second=None):
        if second is None:
            raise RuntimeError("Cannot compare %s >= None" % first)
        result = {}
        result[self.expand(first)] = {'$gte': self.expand(second, first.type)}
        return result

    def ADD(self, first, second):
        raise NotImplementedError(self.error_messages["javascript_needed"])
        return '%s + %s' % (self.expand(first),
                            self.expand(second, first.type))

    def SUB(self, first, second):
        raise NotImplementedError(self.error_messages["javascript_needed"])
        return '(%s - %s)' % (self.expand(first),
                              self.expand(second, first.type))

    def MUL(self, first, second):
        raise NotImplementedError(self.error_messages["javascript_needed"])
        return '(%s * %s)' % (self.expand(first),
                              self.expand(second, first.type))

    def DIV(self, first, second):
        raise NotImplementedError(self.error_messages["javascript_needed"])
        return '(%s / %s)' % (self.expand(first),
                              self.expand(second, first.type))

    def MOD(self, first, second):
        raise NotImplementedError(self.error_messages["javascript_needed"])
        return '(%s %% %s)' % (self.expand(first),
                               self.expand(second, first.type))

    def AS(self, first, second):
        raise NotImplementedError(self.error_messages["javascript_needed"])
        return '%s AS %s' % (self.expand(first), second)

    # We could implement an option that simulates a full featured SQL
    # database. But I think the option should be set explicit or
    # implemented as another library.
    def ON(self, first, second):
        raise NotImplementedError("This is not possible in NoSQL" +
                                  " but can be simulated with a wrapper.")
        return '%s ON %s' % (self.expand(first), self.expand(second))

    def COMMA(self, first, second):
        return '%s, %s' % (self.expand(first), self.expand(second))

    #TODO verify full compatibilty with official SQL Like operator
    def _build_like_regex(self, arg,
                          case_sensitive=True,
                          ends_with=False,
                          starts_with=False,
                          whole_string=True,
                          like_wildcards=False):
        import re
        base = self.expand(arg,'string')
        need_regex = (whole_string or not case_sensitive
                      or starts_with or ends_with
                      or like_wildcards and ('_' in base or '%' in base))
        if not need_regex:
            return base
        else:
            expr = re.escape(base)
            if like_wildcards:
                expr = expr.replace('\\%','.*')
                expr = expr.replace('\\_','.').replace('_','.')
            if starts_with:
                pattern = '^%s'
            elif ends_with:
                pattern = '%s$'
            elif whole_string:
                pattern = '^%s$'
            else:
                pattern = '%s'

            regex = { '$regex': pattern % expr }
            if not case_sensitive:
                regex['$options'] = 'i'
            return regex

    def LIKE(self, first, second, case_sensitive=True):
        regex = self._build_like_regex(
            second, case_sensitive=case_sensitive, like_wildcards=True)
        return { self.expand(first): regex }

    def ILIKE(self, first, second):
        return self.LIKE(first, second, case_sensitive=False)

    def STARTSWITH(self, first, second):
        regex = self._build_like_regex(second, starts_with=True)
        return { self.expand(first): regex }

    def ENDSWITH(self, first, second):
        regex = self._build_like_regex(second, ends_with=True)
        return { self.expand(first): regex }

    #TODO verify full compatibilty with official oracle contains operator
    def CONTAINS(self, first, second, case_sensitive=True):
        ret = None
        if isinstance(second, self.ObjectId):
            val = second

        elif isinstance(first, Field) and first.type == 'list:string':
            if isinstance(second, Field) and second.type == 'string':
                ret = {
                    '$where' :
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
        except:
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
