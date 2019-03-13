import re
import sys
import types
from collections import defaultdict
from contextlib import contextmanager
from .._compat import PY2, with_metaclass, iterkeys, iteritems, hashlib_md5, \
    integer_types, basestring
from .._globals import IDENTITY
from ..connection import ConnectionPool
from ..exceptions import NotOnNOSQLError
from ..helpers.classes import Reference, ExecutionHandler, SQLCustomType, \
    SQLALL, NullDriver
from ..helpers.methods import use_common_filters, xorify, merge_tablemaps
from ..helpers.regex import REGEX_SELECT_AS_PARSER, REGEX_TABLE_DOT_FIELD
from ..migrator import Migrator
from ..objects import Table, Field, Expression, Query, Rows, IterRows, \
    LazySet, LazyReferenceGetter, VirtualCommand, Select
from ..utils import deprecated
from . import AdapterMeta, with_connection, with_connection_or_raise


CALLABLETYPES = (
    types.LambdaType, types.FunctionType, types.BuiltinFunctionType,
    types.MethodType, types.BuiltinMethodType)


class BaseAdapter(with_metaclass(AdapterMeta, ConnectionPool)):
    dbengine = "None"
    drivers = ()
    uploads_in_blob = False
    support_distributed_transaction = False

    def __init__(self, db, uri, pool_size=0, folder=None, db_codec='UTF-8',
                 credential_decoder=IDENTITY, driver_args={},
                 adapter_args={}, do_connect=True, after_connection=None,
                 entity_quoting=False):
        super(BaseAdapter, self).__init__()
        self._load_dependencies()
        self.db = db
        self.uri = uri
        self.pool_size = pool_size
        self.folder = folder
        self.db_codec = db_codec
        self.credential_decoder = credential_decoder
        self.driver_args = driver_args
        self.adapter_args = adapter_args
        self.expand = self._expand
        self._after_connection = after_connection
        self.connection = None
        self.find_driver()
        self._initialize_(do_connect)
        if do_connect:
            self.reconnect()

    def _load_dependencies(self):
        from ..dialects import dialects
        from ..parsers import parsers
        from ..representers import representers
        self.dialect = dialects.get_for(self)
        self.parser = parsers.get_for(self)
        self.representer = representers.get_for(self)

    def _initialize_(self, do_connect):
        self._find_work_folder()

    @property
    def types(self):
        return self.dialect.types

    @property
    def _available_drivers(self):
        return [
            driver for driver in self.drivers
            if driver in iterkeys(self.db._drivers_available)]

    def _driver_from_uri(self):
        rv = None
        if self.uri:
            items = self.uri.split('://', 1)[0].split(':')
            rv = items[1] if len(items) > 1 else None
        return rv

    def find_driver(self):
        if getattr(self, 'driver', None) is not None:
            return
        requested_driver = self._driver_from_uri() or \
            self.adapter_args.get('driver')
        if requested_driver:
            if requested_driver in self._available_drivers:
                self.driver_name = requested_driver
                self.driver = self.db._drivers_available[requested_driver]
            else:
                raise RuntimeError(
                    'Driver %s is not available' % requested_driver)
        elif self._available_drivers:
            self.driver_name = self._available_drivers[0]
            self.driver = self.db._drivers_available[self.driver_name]
        else:
            raise RuntimeError(
                "No driver of supported ones %s is available" %
                str(self.drivers))

    def connector(self):
        return self.driver.connect(self.driver_args)

    def test_connection(self):
        pass

    @with_connection
    def close_connection(self):
        rv = self.connection.close()
        self.connection = None
        return rv

    def tables(self, *queries):
        tables = dict()
        for query in queries:
            if isinstance(query, Field):
                key = query.tablename
                if tables.get(key, query.table) is not query.table:
                    raise ValueError('Name conflict in table list: %s' % key)
                tables[key] = query.table
            elif isinstance(query, (Expression, Query)):
                tmp = [x for x in (query.first, query.second) if x is not None]
                tables = merge_tablemaps(tables, self.tables(*tmp))
        return tables

    def get_table(self, *queries):
        tablemap = self.tables(*queries)
        if len(tablemap) == 1:
            return tablemap.popitem()[1]
        elif len(tablemap) < 1:
            raise RuntimeError("No table selected")
        else:
            raise RuntimeError(
                "Too many tables selected (%s)" % str(list(tablemap)))

    def common_filter(self, query, tablist):
        tenant_fieldname = self.db._request_tenant
        for table in tablist:
            if isinstance(table, basestring):
                table = self.db[table]
            # deal with user provided filters
            if table._common_filter is not None:
                query = query & table._common_filter(query)
            # deal with multi_tenant filters
            if tenant_fieldname in table:
                default = table[tenant_fieldname].default
                if default is not None:
                    newquery = table[tenant_fieldname] == default
                    if query is None:
                        query = newquery
                    else:
                        query = query & newquery
        return query

    def _expand(self, expression, field_type=None, colnames=False,
        query_env={}):
        return str(expression)

    def expand_all(self, fields, tabledict):
        new_fields = []
        append = new_fields.append
        for item in fields:
            if isinstance(item, SQLALL):
                new_fields += item._table
            elif isinstance(item, str):
                m = REGEX_TABLE_DOT_FIELD.match(item)
                if m:
                    tablename, fieldname = m.groups()
                    append(self.db[tablename][fieldname])
                else:
                    append(Expression(self.db, lambda item=item: item))
            else:
                append(item)
        # ## if no fields specified take them all from the requested tables
        if not new_fields:
            for table in tabledict.values():
                for field in table:
                    append(field)
        return new_fields

    def parse_value(self, value, field_itype, field_type, blob_decode=True):
        # [Note - gi0baro] I think next if block can be (should be?) avoided
        if field_type != 'blob' and isinstance(value, str):
            try:
                value = value.decode(self.db._db_codec)
            except Exception:
                pass
        if PY2 and isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(field_type, SQLCustomType):
            value = field_type.decoder(value)
        if not isinstance(field_type, str) or value is None:
            return value
        elif field_type == 'blob' and not blob_decode:
            return value
        else:
            return self.parser.parse(value, field_itype, field_type)

    def _add_operators_to_parsed_row(self, rid, table, row):
        for key, record_operator in iteritems(self.db.record_operators):
            setattr(row, key, record_operator(row, table, rid))
        if table._db._lazy_tables:
            row['__get_lazy_reference__'] = LazyReferenceGetter(table, rid)

    def _add_reference_sets_to_parsed_row(self, rid, table, tablename, row):
        for rfield in table._referenced_by:
            referee_link = self.db._referee_name and self.db._referee_name % \
                dict(table=rfield.tablename, field=rfield.name)
            if referee_link and referee_link not in row and \
               referee_link != tablename:
                row[referee_link] = LazySet(rfield, rid)

    def _regex_select_as_parser(self, colname):
        return re.search(REGEX_SELECT_AS_PARSER, colname)

    def _parse(self, row, tmps, fields, colnames, blob_decode,
               cacheable, fields_virtual, fields_lazy):
        new_row = defaultdict(self.db.Row)
        extras = self.db.Row()
        #: let's loop over columns
        for (j, colname) in enumerate(colnames):
            value = row[j]
            tmp = tmps[j]
            tablename = None
            #: do we have a real column?
            if tmp:
                (tablename, fieldname, table, field, ft, fit) = tmp
                colset = new_row[tablename]
                #: parse value
                value = self.parse_value(value, fit, ft, blob_decode)
                if field.filter_out:
                    value = field.filter_out(value)
                colset[fieldname] = value
                #! backward compatibility
                if ft == 'id' and fieldname != 'id' and \
                   'id' not in table.fields:
                    colset['id'] = value
                #: additional parsing for 'id' fields
                if ft == 'id' and not cacheable:
                    self._add_operators_to_parsed_row(value, table, colset)
                    self._add_reference_sets_to_parsed_row(
                        value, table, tablename, colset)
            #: otherwise we set the value in extras
            else:
                value = self.parse_value(
                    value, fields[j]._itype, fields[j].type, blob_decode)
                extras[colname] = value
                new_column_match = self._regex_select_as_parser(colname)
                if new_column_match is not None:
                    new_column_name = new_column_match.group(1)
                    new_row[new_column_name] = value
        #: add extras if needed (eg. operations results)
        if extras:
            new_row['_extra'] = extras
        #: add virtuals
        new_row = self.db.Row(**new_row)
        for tablename in fields_virtual.keys():
            for f, v in fields_virtual[tablename][1]:
                try:
                    new_row[tablename][f] = v.f(new_row)
                except (AttributeError, KeyError):
                    pass  # not enough fields to define virtual field
            for f, v in fields_lazy[tablename][1]:
                try:
                    new_row[tablename][f] = v.handler(v.f, new_row)
                except (AttributeError, KeyError):
                    pass  # not enough fields to define virtual field
        return new_row

    def _parse_expand_colnames(self, fieldlist):
        """
        - Expand a list of colnames into a list of
          (tablename, fieldname, table_obj, field_obj, field_type)
        - Create a list of table for virtual/lazy fields
        """
        fields_virtual = {}
        fields_lazy = {}
        tmps = []
        for field in fieldlist:
            if not isinstance(field, Field):
                tmps.append(None)
                continue
            table = field.table
            tablename, fieldname = table._tablename, field.name
            ft = field.type
            fit = field._itype
            tmps.append((tablename, fieldname, table, field, ft, fit))
            if tablename not in fields_virtual:
                fields_virtual[tablename] = (table, [
                    (f.name, f) for f in table._virtual_fields
                ])
                fields_lazy[tablename] = (table, [
                    (f.name, f) for f in table._virtual_methods
                ])
        return (fields_virtual, fields_lazy, tmps)

    def parse(self, rows, fields, colnames, blob_decode=True, cacheable=False):
        (fields_virtual, fields_lazy, tmps) = \
            self._parse_expand_colnames(fields)
        new_rows = [
            self._parse(
                row, tmps, fields, colnames, blob_decode, cacheable,
                fields_virtual, fields_lazy)
            for row in rows
        ]
        rowsobj = self.db.Rows(self.db, new_rows, colnames, rawrows=rows,
                fields=fields)
        # Old style virtual fields
        for tablename, tmp in fields_virtual.items():
            table = tmp[0]
            # ## old style virtual fields
            for item in table.virtualfields:
                try:
                    rowsobj = rowsobj.setvirtualfields(**{tablename: item})
                except (KeyError, AttributeError):
                    # to avoid breaking virtualfields when partial select
                    pass
        return rowsobj

    def iterparse(self, sql, fields, colnames, blob_decode=True,
                  cacheable=False):
        """
        Iterator to parse one row at a time.
        It doesn't support the old style virtual fields
        """
        return IterRows(self.db, sql, fields, colnames, blob_decode, cacheable)

    def adapt(self, value):
        return value

    def represent(self, obj, field_type):
        if isinstance(obj, CALLABLETYPES):
            obj = obj()
        return self.representer.represent(obj, field_type)

    def _drop_table_cleanup(self, table):
        del self.db[table._tablename]
        del self.db.tables[self.db.tables.index(table._tablename)]
        self.db._remove_references_to(table)

    def drop_table(self, table, mode=''):
        self._drop_table_cleanup(table)

    def rowslice(self, rows, minimum=0, maximum=None):
        return rows

    def sqlsafe_table(self, tablename, original_tablename=None):
        return tablename

    def sqlsafe_field(self, fieldname):
        return fieldname


class DebugHandler(ExecutionHandler):
    def before_execute(self, command):
        self.adapter.db.logger.debug('SQL: %s' % command)


class SQLAdapter(BaseAdapter):
    commit_on_alter_table = False
    # [Note - gi0baro] can_select_for_update should be deprecated and removed
    can_select_for_update = True
    execution_handlers = []
    migrator_cls = Migrator

    def __init__(self, *args, **kwargs):
        super(SQLAdapter, self).__init__(*args, **kwargs)
        migrator_cls = self.adapter_args.get('migrator', self.migrator_cls)
        self.migrator = migrator_cls(self)
        self.execution_handlers = list(self.db.execution_handlers)
        if self.db._debug:
            self.execution_handlers.insert(0, DebugHandler)

    def test_connection(self):
        self.execute('SELECT 1;')

    def represent(self, obj, field_type):
        if isinstance(obj, (Expression, Field)):
            return str(obj)
        return super(SQLAdapter, self).represent(obj, field_type)

    def adapt(self, obj):
        return "'%s'" % obj.replace("'", "''")

    def smart_adapt(self, obj):
        if isinstance(obj, (int, float)):
            return str(obj)
        return self.adapt(str(obj))

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def _build_handlers_for_execution(self):
        rv = []
        for handler_class in self.execution_handlers:
            rv.append(handler_class(self))
        return rv

    def filter_sql_command(self, command):
        return command

    @with_connection_or_raise
    def execute(self, *args, **kwargs):
        command = self.filter_sql_command(args[0])
        handlers = self._build_handlers_for_execution()
        for handler in handlers:
            handler.before_execute(command)
        rv = self.cursor.execute(command, *args[1:], **kwargs)
        for handler in handlers:
            handler.after_execute(command)
        return rv

    def _expand(self, expression, field_type=None, colnames=False,
        query_env={}):
        if isinstance(expression, Field):
            if not colnames:
                rv = expression.sqlsafe
            else:
                rv = expression.longname
            if field_type == 'string' and expression.type not in (
                    'string', 'text', 'json', 'jsonb', 'password'):
                rv = self.dialect.cast(rv, self.types['text'], query_env)
        elif isinstance(expression, (Expression, Query)):
            first = expression.first
            second = expression.second
            op = expression.op
            optional_args = expression.optional_args or {}
            optional_args['query_env'] = query_env
            if second is not None:
                rv = op(first, second, **optional_args)
            elif first is not None:
                rv = op(first, **optional_args)
            elif isinstance(op, str):
                if op.endswith(';'):
                    op = op[:-1]
                rv = '(%s)' % op
            else:
                rv = op()
        elif field_type:
            rv = self.represent(expression, field_type)
        elif isinstance(expression, (list, tuple)):
            rv = ','.join(self.represent(item, field_type)
                          for item in expression)
        elif isinstance(expression, bool):
            rv = self.dialect.true_exp if expression else \
                self.dialect.false_exp
        else:
            rv = expression
        return str(rv)

    def _expand_for_index(self, expression, field_type=None, colnames=False,
        query_env={}):
        if isinstance(expression, Field):
            return expression._rname
        return self._expand(expression, field_type, colnames, query_env)

    @contextmanager
    def index_expander(self):
        self.expand = self._expand_for_index
        yield
        self.expand = self._expand

    def lastrowid(self, table):
        return self.cursor.lastrowid

    def _insert(self, table, fields):
        if fields:
            return self.dialect.insert(
                table._rname,
                ','.join(el[0]._rname for el in fields),
                ','.join(self.expand(v, f.type) for f, v in fields))
        return self.dialect.insert_empty(table._rname)

    def insert(self, table, fields):
        query = self._insert(table, fields)
        try:
            self.execute(query)
        except:
            e = sys.exc_info()[1]
            if hasattr(table, '_on_insert_error'):
                return table._on_insert_error(table, fields, e)
            raise e
        if hasattr(table, '_primarykey'):
            pkdict = dict([
                (k[0].name, k[1]) for k in fields
                if k[0].name in table._primarykey])
            if pkdict:
                return pkdict
        id = self.lastrowid(table)
        if hasattr(table, '_primarykey') and len(table._primarykey) == 1:
            id = {table._primarykey[0]: id}
        if not isinstance(id, integer_types):
            return id
        rid = Reference(id)
        (rid._table, rid._record) = (table, None)
        return rid

    def _update(self, table, query, fields):
        sql_q = ''
        query_env = dict(current_scope=[table._tablename])
        if query:
            if use_common_filters(query):
                query = self.common_filter(query, [table])
            sql_q = self.expand(query, query_env=query_env)
        sql_v = ','.join([
            '%s=%s' % (field._rname,
                self.expand(value, field.type, query_env=query_env))
            for (field, value) in fields])
        return self.dialect.update(table, sql_v, sql_q)

    def update(self, table, query, fields):
        sql = self._update(table, query, fields)
        try:
            self.execute(sql)
        except:
            e = sys.exc_info()[1]
            if hasattr(table, '_on_update_error'):
                return table._on_update_error(table, query, fields, e)
            raise e
        try:
            return self.cursor.rowcount
        except:
            return None

    def _delete(self, table, query):
        sql_q = ''
        query_env = dict(current_scope=[table._tablename])
        if query:
            if use_common_filters(query):
                query = self.common_filter(query, [table])
            sql_q = self.expand(query, query_env=query_env)
        return self.dialect.delete(table, sql_q)

    def delete(self, table, query):
        sql = self._delete(table, query)
        self.execute(sql)
        try:
            return self.cursor.rowcount
        except:
            return None

    def _colexpand(self, field, query_env):
        return self.expand(field, colnames=True, query_env=query_env)

    def _geoexpand(self, field, query_env):
        if isinstance(field.type, str) and field.type.startswith('geo') and \
           isinstance(field, Field):
            field = field.st_astext()
        return self.expand(field, query_env=query_env)

    def _build_joins_for_select(self, tablenames, param):
        if not isinstance(param, (tuple, list)):
            param = [param]
        tablemap = {}
        for item in param:
            if isinstance(item, Expression):
                item = item.first
            key = item._tablename
            if tablemap.get(key, item) is not item:
                raise ValueError('Name conflict in table list: %s' % key)
            tablemap[key] = item
        join_tables = [
            t._tablename for t in param if not isinstance(t, Expression)
        ]
        join_on = [t for t in param if isinstance(t, Expression)]
        tables_to_merge = {}
        for t in join_on:
            tables_to_merge = merge_tablemaps(tables_to_merge, self.tables(t))
        join_on_tables = [t.first._tablename for t in join_on]
        for t in join_on_tables:
            if t in tables_to_merge:
                tables_to_merge.pop(t)
        important_tablenames = join_tables + join_on_tables + \
            list(tables_to_merge)
        excluded = [
            t for t in tablenames if t not in important_tablenames
        ]
        return (
            join_tables, join_on, tables_to_merge, join_on_tables,
            important_tablenames, excluded, tablemap
        )

    def _select_wcols(self, query, fields, left=False, join=False,
                      distinct=False, orderby=False, groupby=False,
                      having=False, limitby=False, orderby_on_limitby=True,
                      for_update=False, outer_scoped=[], required=None,
                      cache=None, cacheable=None, processor=None):
        #: parse tablemap
        tablemap = self.tables(query)
        #: apply common filters if needed
        if use_common_filters(query):
            query = self.common_filter(query, list(tablemap.values()))
        #: auto-adjust tables
        tablemap = merge_tablemaps(tablemap, self.tables(*fields))
        #: remove outer scoped tables if needed
        for item in outer_scoped:
            # FIXME: check for name conflicts
            tablemap.pop(item, None)
        if len(tablemap) < 1:
            raise SyntaxError('Set: no tables selected')
        query_tables = list(tablemap)
        #: check for_update argument
        # [Note - gi0baro] I think this should be removed since useless?
        #                  should affect only NoSQL?
        if self.can_select_for_update is False and for_update is True:
            raise SyntaxError('invalid select attribute: for_update')
        #: build joins (inner, left outer) and table names
        if join:
            (
                # FIXME? ijoin_tables is never used
                ijoin_tables, ijoin_on, itables_to_merge, ijoin_on_tables,
                iimportant_tablenames, iexcluded, itablemap
            ) = self._build_joins_for_select(tablemap, join)
            tablemap = merge_tablemaps(tablemap, itables_to_merge)
            tablemap = merge_tablemaps(tablemap, itablemap)
        if left:
            (
                join_tables, join_on, tables_to_merge, join_on_tables,
                important_tablenames, excluded, jtablemap
            ) = self._build_joins_for_select(tablemap, left)
            tablemap = merge_tablemaps(tablemap, tables_to_merge)
            tablemap = merge_tablemaps(tablemap, jtablemap)
        current_scope = outer_scoped + list(tablemap)
        query_env = dict(current_scope=current_scope,
            parent_scope=outer_scoped)
        #: prepare columns and expand fields
        colnames = [self._colexpand(x, query_env) for x in fields]
        sql_fields = ', '.join(self._geoexpand(x, query_env) for x in fields)
        table_alias = lambda name: tablemap[name].query_name(outer_scoped)[0]
        if join and not left:
            cross_joins = iexcluded + list(itables_to_merge)
            tokens = [table_alias(cross_joins[0])]
            tokens += [self.dialect.cross_join(table_alias(t), query_env)
                    for t in cross_joins[1:]]
            tokens += [self.dialect.join(t, query_env) for t in ijoin_on]
            sql_t = ' '.join(tokens)
        elif not join and left:
            cross_joins = excluded + list(tables_to_merge)
            tokens = [table_alias(cross_joins[0])]
            tokens += [self.dialect.cross_join(table_alias(t), query_env)
                    for t in cross_joins[1:]]
            # FIXME: WTF? This is not correct syntax at least on PostgreSQL
            if join_tables:
                tokens.append(self.dialect.left_join(','.join([table_alias(t)
                        for t in join_tables]), query_env))
            tokens += [self.dialect.left_join(t, query_env) for t in join_on]
            sql_t = ' '.join(tokens)
        elif join and left:
            all_tables_in_query = set(
                important_tablenames + iimportant_tablenames + query_tables)
            tables_in_joinon = set(join_on_tables + ijoin_on_tables)
            tables_not_in_joinon = \
                list(all_tables_in_query.difference(tables_in_joinon))
            tokens = [table_alias(tables_not_in_joinon[0])]
            tokens += [self.dialect.cross_join(table_alias(t), query_env)
                    for t in tables_not_in_joinon[1:]]
            tokens += [self.dialect.join(t, query_env) for t in ijoin_on]
            # FIXME: WTF? This is not correct syntax at least on PostgreSQL
            if join_tables:
                tokens.append(self.dialect.left_join(','.join([table_alias(t)
                        for t in join_tables]), query_env))
            tokens += [self.dialect.left_join(t, query_env) for t in join_on]
            sql_t = ' '.join(tokens)
        else:
            sql_t = ', '.join(table_alias(t) for t in query_tables)
        #: expand query if needed
        if query:
            query = self.expand(query, query_env=query_env)
        if having:
            having = self.expand(having, query_env=query_env)
        #: groupby
        sql_grp = groupby
        if groupby:
            if isinstance(groupby, (list, tuple)):
                groupby = xorify(groupby)
            sql_grp = self.expand(groupby, query_env=query_env)
        #: orderby
        sql_ord = False
        if orderby:
            if isinstance(orderby, (list, tuple)):
                orderby = xorify(orderby)
            if str(orderby) == '<random>':
                sql_ord = self.dialect.random
            else:
                sql_ord = self.expand(orderby, query_env=query_env)
        #: set default orderby if missing
        if (limitby and not groupby and query_tables and orderby_on_limitby and
           not orderby):
            sql_ord = ', '.join([
                tablemap[t][x].sqlsafe
                for t in query_tables if not isinstance(tablemap[t], Select)
                for x in (hasattr(tablemap[t], '_primarykey') and
                          tablemap[t]._primarykey or ['_id'])
            ])
        #: build sql using dialect
        return colnames, self.dialect.select(
            sql_fields, sql_t, query, sql_grp, having, sql_ord, limitby,
            distinct, for_update and self.can_select_for_update
        )

    def _select(self, query, fields, attributes):
        return self._select_wcols(query, fields, **attributes)[1]

    def nested_select(self, query, fields, attributes):
        return Select(self.db, query, fields, attributes)

    def _select_aux_execute(self, sql):
        self.execute(sql)
        return self.cursor.fetchall()

    def _select_aux(self, sql, fields, attributes, colnames):
        cache = attributes.get('cache', None)
        if not cache:
            rows = self._select_aux_execute(sql)
        else:
            if isinstance(cache, dict):
                cache_model = cache['model']
                time_expire = cache['expiration']
                key = cache.get('key')
                if not key:
                    key = self.uri + '/' + sql + '/rows'
                    key = hashlib_md5(key).hexdigest()
            else:
                (cache_model, time_expire) = cache
                key = self.uri + '/' + sql + '/rows'
                key = hashlib_md5(key).hexdigest()
            rows = cache_model(
                key,
                lambda self=self, sql=sql: self._select_aux_execute(sql),
                time_expire)
        if isinstance(rows, tuple):
            rows = list(rows)
        limitby = attributes.get('limitby', None) or (0,)
        rows = self.rowslice(rows, limitby[0], None)
        processor = attributes.get('processor', self.parse)
        cacheable = attributes.get('cacheable', False)
        return processor(rows, fields, colnames, cacheable=cacheable)

    def _cached_select(self, cache, sql, fields, attributes, colnames):
        del attributes['cache']
        (cache_model, time_expire) = cache
        key = self.uri + '/' + sql
        key = hashlib_md5(key).hexdigest()
        args = (sql, fields, attributes, colnames)
        ret = cache_model(
            key,
            lambda self=self, args=args: self._select_aux(*args),
            time_expire)
        ret._restore_fields(fields)
        return ret

    def select(self, query, fields, attributes):
        colnames, sql = self._select_wcols(query, fields, **attributes)
        cache = attributes.get('cache', None)
        if cache and attributes.get('cacheable', False):
            return self._cached_select(
                cache, sql, fields, attributes, colnames)
        return self._select_aux(sql, fields, attributes, colnames)

    def iterselect(self, query, fields, attributes):
        colnames, sql = self._select_wcols(query, fields, **attributes)
        cacheable = attributes.get('cacheable', False)
        return self.iterparse(sql, fields, colnames, cacheable=cacheable)

    def _count(self, query, distinct=None):
        tablemap = self.tables(query)
        tablenames = list(tablemap)
        tables = list(tablemap.values())
        query_env = dict(current_scope=tablenames)
        sql_q = ''
        if query:
            if use_common_filters(query):
                query = self.common_filter(query, tables)
            sql_q = self.expand(query, query_env=query_env)
        sql_t = ','.join(self.table_alias(t, []) for t in tables)
        sql_fields = '*'
        if distinct:
            if isinstance(distinct, (list, tuple)):
                distinct = xorify(distinct)
            sql_fields = self.expand(distinct, query_env=query_env)
        return self.dialect.select(
            self.dialect.count(sql_fields, distinct), sql_t, sql_q
        )

    def count(self, query, distinct=None):
        self.execute(self._count(query, distinct))
        return self.cursor.fetchone()[0]

    def bulk_insert(self, table, items):
        return [self.insert(table, item) for item in items]

    def create_table(self, *args, **kwargs):
        return self.migrator.create_table(*args, **kwargs)

    def _drop_table_cleanup(self, table):
        super(SQLAdapter, self)._drop_table_cleanup(table)
        if table._dbt:
            self.migrator.file_delete(table._dbt)
            self.migrator.log('success!\n', table)

    def drop_table(self, table, mode=''):
        queries = self.dialect.drop_table(table, mode)
        for query in queries:
            if table._dbt:
                self.migrator.log(query + '\n', table)
            self.execute(query)
        self.commit()
        self._drop_table_cleanup(table)

    @deprecated('drop', 'drop_table', 'SQLAdapter')
    def drop(self, table, mode=''):
        return self.drop_table(table, mode='')

    def truncate(self, table, mode=''):
        # Prepare functions "write_to_logfile" and "close_logfile"
        try:
            queries = self.dialect.truncate(table, mode)
            for query in queries:
                self.migrator.log(query + '\n', table)
                self.execute(query)
            self.migrator.log('success!\n', table)
        finally:
            pass

    def create_index(self, table, index_name, *fields, **kwargs):
        expressions = [
            field._rname if isinstance(field, Field) else field
            for field in fields]
        sql = self.dialect.create_index(
            index_name, table, expressions, **kwargs)
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            self.rollback()
            err = 'Error creating index %s\n  Driver error: %s\n' + \
                '  SQL instruction: %s'
            raise RuntimeError(err % (index_name, str(e), sql))
        return True

    def drop_index(self, table, index_name):
        sql = self.dialect.drop_index(index_name, table)
        try:
            self.execute(sql)
            self.commit()
        except Exception as e:
            self.rollback()
            err = 'Error dropping index %s\n  Driver error: %s'
            raise RuntimeError(err % (index_name, str(e)))
        return True

    def distributed_transaction_begin(self, key):
        pass

    @with_connection
    def commit(self):
        return self.connection.commit()

    @with_connection
    def rollback(self):
        return self.connection.rollback()

    @with_connection
    def prepare(self, key):
        self.connection.prepare()

    @with_connection
    def commit_prepared(self, key):
        self.connection.commit()

    @with_connection
    def rollback_prepared(self, key):
        self.connection.rollback()

    def create_sequence_and_triggers(self, query, table, **args):
        self.execute(query)

    def sqlsafe_table(self, tablename, original_tablename=None):
        if original_tablename is not None:
            return self.dialect.alias(original_tablename, tablename)
        return self.dialect.quote(tablename)

    def sqlsafe_field(self, fieldname):
        return self.dialect.quote(fieldname)

    def table_alias(self, tbl, current_scope=[]):
        if isinstance(tbl, basestring):
            tbl = self.db[tbl]
        return tbl.query_name(current_scope)[0]

    def id_query(self, table):
        pkeys = getattr(table, '_primarykey', None)
        if pkeys:
            return table[pkeys[0]] != None
        return table._id != None


class NoSQLAdapter(BaseAdapter):
    can_select_for_update = False

    def commit(self):
        pass

    def rollback(self):
        pass

    def prepare(self):
        pass

    def commit_prepared(self, key):
        pass

    def rollback_prepared(self, key):
        pass

    def id_query(self, table):
        return table._id > 0

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

    def drop_table(self, table, mode=''):
        ctable = self.connection[table._tablename]
        ctable.drop()
        self._drop_table_cleanup(table)

    @deprecated('drop', 'drop_table', 'SQLAdapter')
    def drop(self, table, mode=''):
        return self.drop_table(table, mode='')

    def _select(self, *args, **kwargs):
        raise NotOnNOSQLError(
            "Nested queries are not supported on NoSQL databases")

    def nested_select(self, *args, **kwargs):
        raise NotOnNOSQLError(
            "Nested queries are not supported on NoSQL databases")


class NullAdapter(BaseAdapter):

    def _load_dependencies(self):
        from ..dialects.base import CommonDialect
        self.dialect = CommonDialect(self)

    def find_driver(self):
        pass

    def connector(self):
        return NullDriver()
