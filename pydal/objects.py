# -*- coding: utf-8 -*-

import base64
import cgi
import copy
import csv
import datetime
import decimal
import os
import shutil
import sys
import types

from ._compat import PY2, StringIO, BytesIO, pjoin, exists, hashlib_md5, \
    integer_types, basestring, iteritems, xrange, implements_iterator, \
    implements_bool, copyreg, reduce, string_types, to_bytes, to_native, long
from ._globals import DEFAULT, IDENTITY, AND, OR
from ._gae import Key
from .exceptions import NotFoundException, NotAuthorizedException
from .helpers.regex import REGEX_TABLE_DOT_FIELD, REGEX_ALPHANUMERIC, \
    REGEX_PYTHON_KEYWORDS, REGEX_STORE_PATTERN, REGEX_UPLOAD_PATTERN, \
    REGEX_CLEANUP_FN, REGEX_VALID_TB_FLD, REGEX_TYPE
from .helpers.classes import Reference, MethodAdder, SQLCallableList, SQLALL, \
    Serializable, BasicStorage, SQLCustomType
from .helpers.methods import list_represent, bar_decode_integer, \
    bar_decode_string, bar_encode, archive_record, cleanup, \
    use_common_filters, pluralize
from .helpers.serializers import serializers


DEFAULTLENGTH = {'string': 512, 'password': 512, 'upload': 512, 'text': 2**15,
                 'blob': 2**31}


class Row(BasicStorage):

    """
    A dictionary that lets you do d['a'] as well as d.a
    this is only used to store a `Row`
    """

    def __getitem__(self, k):
        key = str(k)
        _extra = super(Row, self).get('_extra', None)
        if _extra is not None:
            v = _extra.get(key, DEFAULT)
            if v != DEFAULT:
                return v

        try:
            return BasicStorage.__getattribute__(self, key)
        except AttributeError:
            pass

        m = REGEX_TABLE_DOT_FIELD.match(key)
        if m:
            try:
                e = super(Row, self).__getitem__(m.group(1))
                return e[m.group(2)]
            except (KeyError, TypeError):
                pass
            key = m.group(2)
            try:
                return super(Row, self).__getitem__(key)
            except KeyError:
                pass
        try:
            e = super(Row, self).get('__get_lazy_reference__')
            if e is not None and callable(e):
                self[key] = e(key)
                return self[key]
        except Exception as e:
            raise e

        raise KeyError

    __str__ = __repr__ = lambda self: '<Row %s>' % \
        self.as_dict(custom_types=[LazySet])

    __int__ = lambda self: self.get('id')

    __long__ = lambda self: long(self.get('id'))

    __call__ = __getitem__

    def __getattr__(self, k):
        try:
            return self.__getitem__(k)
        except KeyError:
            raise AttributeError

    def __copy__(self):
        return Row(self)

    def __eq__(self, other):
        try:
            return self.as_dict() == other.as_dict()
        except AttributeError:
            return False

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except(KeyError, AttributeError, TypeError):
            return default

    def as_dict(self, datetime_to_str=False, custom_types=None):
        SERIALIZABLE_TYPES = [str, int, float, bool, list, dict]
        DT_INST = (datetime.date, datetime.datetime, datetime.time)
        if PY2:
            SERIALIZABLE_TYPES += [unicode, long]
        if isinstance(custom_types, (list, tuple, set)):
            SERIALIZABLE_TYPES += custom_types
        elif custom_types:
            SERIALIZABLE_TYPES.append(custom_types)
        d = dict(self)
        for k in list(d.keys()):
            v = d[k]
            if d[k] is None:
                continue
            elif isinstance(v, Row):
                d[k] = v.as_dict()
            elif isinstance(v, Reference):
                d[k] = long(v)
            elif isinstance(v, decimal.Decimal):
                d[k] = float(v)
            elif isinstance(v, DT_INST):
                if datetime_to_str:
                    d[k] = v.isoformat().replace('T', ' ')[:19]
            elif not isinstance(v, tuple(SERIALIZABLE_TYPES)):
                del d[k]
        return d

    def as_xml(self, row_name="row", colnames=None, indent='  '):
        def f(row, field, indent='  '):
            if isinstance(row, Row):
                spc = indent+'  \n'
                items = [f(row[x], x, indent+'  ') for x in row]
                return '%s<%s>\n%s\n%s</%s>' % (
                    indent,
                    field,
                    spc.join(item for item in items if item),
                    indent,
                    field)
            elif not callable(row):
                if REGEX_ALPHANUMERIC.match(field):
                    return '%s<%s>%s</%s>' % (indent, field, row, field)
                else:
                    return '%s<extra name="%s">%s</extra>' % \
                        (indent, field, row)
            else:
                return None
        return f(self, row_name, indent=indent)

    def as_json(self, mode="object", default=None, colnames=None,
                serialize=True, **kwargs):
        """
        serializes the row to a JSON object
        kwargs are passed to .as_dict method
        only "object" mode supported

        `serialize = False` used by Rows.as_json

        TODO: return array mode with query column order

        mode and colnames are not implemented
        """

        item = self.as_dict(**kwargs)
        if serialize:
            return serializers.json(item)
        else:
            return item


def pickle_row(s):
    return Row, (dict(s), )

copyreg.pickle(Row, pickle_row)


class Table(Serializable, BasicStorage):

    """
    Represents a database table

    Example::
        You can create a table as::
            db = DAL(...)
            db.define_table('users', Field('name'))

        And then::

            db.users.insert(name='me') # print db.users._insert(...) to see SQL
            db.users.drop()

    """

    def __init__(self, db, tablename, *fields, **args):
        """
        Initializes the table and performs checking on the provided fields.

        Each table will have automatically an 'id'.

        If a field is of type Table, the fields (excluding 'id') from that table
        will be used instead.

        Raises:
            SyntaxError: when a supplied field is of incorrect type.
        """
        # import DAL here to avoid circular imports
        from .base import DAL
        super(Table, self).__init__()
        self._actual = False  # set to True by define_table()
        self._db = db
        self._tablename = tablename
        if not isinstance(tablename, str) or hasattr(DAL, tablename) or not \
           REGEX_VALID_TB_FLD.match(tablename) or \
           REGEX_PYTHON_KEYWORDS.match(tablename):
            raise SyntaxError('Field: invalid table name: %s, '
                              'use rname for "funny" names' % tablename)
        self._ot = None
        self._rname = args.get('rname')
        self._sequence_name = args.get('sequence_name') or \
            db and db._adapter.dialect.sequence_name(self._rname or tablename)
        self._trigger_name = args.get('trigger_name') or \
            db and db._adapter.dialect.trigger_name(tablename)
        self._common_filter = args.get('common_filter')
        self._format = args.get('format')
        self._singular = args.get(
            'singular', tablename.replace('_', ' ').capitalize())
        self._plural = args.get(
            'plural', pluralize(self._singular.lower()).capitalize())
        # horrible but for backard compatibility of appamdin:
        if 'primarykey' in args and args['primarykey'] is not None:
            self._primarykey = args.get('primarykey')

        self._before_insert = []
        self._before_update = [Set.delete_uploaded_files]
        self._before_delete = [Set.delete_uploaded_files]
        self._after_insert = []
        self._after_update = []
        self._after_delete = []

        self._virtual_fields = []
        self._virtual_methods = []

        self.add_method = MethodAdder(self)

        fieldnames, newfields = set(), []
        _primarykey = getattr(self, '_primarykey', None)
        if _primarykey is not None:
            if not isinstance(_primarykey, list):
                raise SyntaxError(
                    "primarykey must be a list of fields from table '%s'"
                    % tablename)
            if len(_primarykey) == 1:
                self._id = [
                    f for f in fields if isinstance(f, Field) and
                    f.name == _primarykey[0]][0]
        elif not [f for f in fields if (isinstance(f, Field) and
                  f.type == 'id') or (isinstance(f, dict) and
                  f.get("type", None) == "id")]:
            field = Field('id', 'id')
            newfields.append(field)
            fieldnames.add('id')
            self._id = field

        virtual_fields = []

        def include_new(field):
            newfields.append(field)
            fieldnames.add(field.name)
            if field.type == 'id':
                self._id = field
        for field in fields:
            if isinstance(field, (FieldVirtual, FieldMethod)):
                virtual_fields.append(field)
            elif isinstance(field, Field) and field.name not in fieldnames:
                if field.db is not None:
                    field = copy.copy(field)
                include_new(field)
            elif isinstance(field, Table):
                table = field
                for field in table:
                    if field.name not in fieldnames and field.type != 'id':
                        t2 = not table._actual and self._tablename
                        include_new(field.clone(point_self_references_to=t2))
            elif isinstance(field, dict) and \
                    field['fieldname'] not in fieldnames:
                include_new(Field(**field))
            elif not isinstance(field, (Field, Table)):
                raise SyntaxError(
                    'define_table argument is not a Field or Table: %s' %
                    field
                )
        fields = newfields
        tablename = tablename
        self._fields = SQLCallableList()
        self.virtualfields = []
        fields = list(fields)

        if db and db._adapter.uploads_in_blob is True:
            uploadfields = [f.name for f in fields if f.type == 'blob']
            for field in fields:
                fn = field.uploadfield
                if isinstance(field, Field) and field.type == 'upload'\
                        and fn is True and not field.uploadfs:
                    fn = field.uploadfield = '%s_blob' % field.name
                if isinstance(fn, str) and fn not in uploadfields and \
                   not field.uploadfs:
                    fields.append(Field(fn, 'blob', default='',
                                        writable=False, readable=False))

        fieldnames_set = set()
        reserved = dir(Table) + ['fields']
        if (db and db.check_reserved):
            check_reserved = db.check_reserved_keyword
        else:
            def check_reserved(field_name):
                if field_name in reserved:
                    raise SyntaxError("field name %s not allowed" % field_name)
        for field in fields:
            field_name = field.name
            check_reserved(field_name)
            if db and db._ignore_field_case:
                fname_item = field_name.lower()
            else:
                fname_item = field_name
            if fname_item in fieldnames_set:
                raise SyntaxError(
                    "duplicate field %s in table %s" % (field_name, tablename))
            else:
                fieldnames_set.add(fname_item)

            self.fields.append(field_name)
            self[field_name] = field
            if field.type == 'id':
                self['id'] = field
            field.tablename = field._tablename = tablename
            field.table = field._table = self
            field.db = field._db = db
        self.ALL = SQLALL(self)

        if _primarykey is not None:
            for k in _primarykey:
                if k not in self.fields:
                    raise SyntaxError(
                        "primarykey must be a list of fields from table '%s " %
                        tablename)
                else:
                    self[k].notnull = True
        for field in virtual_fields:
            self[field.name] = field

    @property
    def fields(self):
        return self._fields

    def update(self, *args, **kwargs):
        raise RuntimeError("Syntax Not Supported")

    def _enable_record_versioning(self,
                                  archive_db=None,
                                  archive_name='%(tablename)s_archive',
                                  is_active='is_active',
                                  current_record='current_record',
                                  current_record_label=None):
        db = self._db
        archive_db = archive_db or db
        archive_name = archive_name % dict(tablename=self._tablename)
        if archive_name in archive_db.tables():
            return  # do not try define the archive if already exists
        fieldnames = self.fields()
        same_db = archive_db is db
        field_type = self if same_db else 'bigint'
        clones = []
        for field in self:
            nfk = same_db or not field.type.startswith('reference')
            clones.append(
                field.clone(unique=False, type=field.type if nfk else 'bigint')
                )
        archive_db.define_table(
            archive_name,
            Field(current_record, field_type, label=current_record_label),
            *clones, **dict(format=self._format))

        self._before_update.append(
            lambda qset, fs, db=archive_db, an=archive_name, cn=current_record:
                archive_record(qset, fs, db[an], cn))
        if is_active and is_active in fieldnames:
            self._before_delete.append(
                lambda qset: qset.update(is_active=False))
            newquery = lambda query, t=self, name=self._tablename: reduce(
                AND, [
                    db[tn].is_active == True
                    for tn in db._adapter.tables(query)
                    if tn == name or getattr(db[tn], '_ot', None) == name]
                )
            query = self._common_filter
            if query:
                self._common_filter = lambda q: reduce(
                    AND, [query(q), newquery(q)])
            else:
                self._common_filter = newquery

    def _validate(self, **vars):
        errors = Row()
        for key, value in iteritems(vars):
            value, error = self[key].validate(value)
            if error:
                errors[key] = error
        return errors

    def _create_references(self):
        db = self._db
        pr = db._pending_references
        self._referenced_by_list = []
        self._referenced_by = []
        self._references = []
        for field in self:
            # fieldname = field.name  #FIXME not used ?
            field_type = field.type
            if isinstance(field_type, str) and (
                    field_type.startswith('reference ') or
                    field_type.startswith('list:reference ')):

                is_list = field_type[:15] == 'list:reference '
                if is_list:
                    ref = field_type[15:].strip()
                else:
                    ref = field_type[10:].strip()

                if not ref:
                    SyntaxError('Table: reference to nothing: %s' % ref)
                if '.' in ref:
                    rtablename, throw_it, rfieldname = ref.partition('.')
                else:
                    rtablename, rfieldname = ref, None
                if rtablename not in db:
                    pr[rtablename] = pr.get(rtablename, []) + [field]
                    continue
                rtable = db[rtablename]
                if rfieldname:
                    if not hasattr(rtable, '_primarykey'):
                        raise SyntaxError(
                            'keyed tables can only reference other keyed tables (for now)')
                    if rfieldname not in rtable.fields:
                        raise SyntaxError(
                            "invalid field '%s' for referenced table '%s'"
                            " in table '%s'" % (rfieldname, rtablename, self._tablename)
                            )
                    rfield = rtable[rfieldname]
                else:
                    rfield = rtable._id
                if is_list:
                    rtable._referenced_by_list.append(field)
                else:
                    rtable._referenced_by.append(field)
                field.referent = rfield
                self._references.append(field)
            else:
                field.referent = None
        if self._tablename in pr:
            referees = pr.pop(self._tablename)
            for referee in referees:
                if referee.type.startswith('list:reference '):
                    self._referenced_by_list.append(referee)
                else:
                    self._referenced_by.append(referee)

    def _filter_fields(self, record, id=False):
        return dict([(k, v) for (k, v) in iteritems(record) if k
                     in self.fields and (self[k].type != 'id' or id)])

    def _build_query(self, key):
        """ for keyed table only """
        query = None
        for k, v in iteritems(key):
            if k in self._primarykey:
                if query:
                    query = query & (self[k] == v)
                else:
                    query = (self[k] == v)
            else:
                raise SyntaxError(
                    'Field %s is not part of the primary key of %s' %
                    (k, self._tablename))
        return query

    def __getitem__(self, key):
        if not key:
            return None
        elif isinstance(key, dict):
            """ for keyed table """
            query = self._build_query(key)
            return self._db(query).select(
                limitby=(0, 1),
                orderby_on_limitby=False
            ).first()
        else:
            try:
                isgoogle = 'google' in self._db._drivers_available and \
                    isinstance(key, Key)
            except:
                isgoogle = False
            if str(key).isdigit() or isgoogle:
                return self._db(self._id == key).select(
                    limitby=(0, 1),
                    orderby_on_limitby=False
                ).first()
            else:
                try:
                    return getattr(self, key)
                except:
                    raise KeyError(key)

    def __call__(self, key=DEFAULT, **kwargs):
        for_update = kwargs.get('_for_update', False)
        if '_for_update' in kwargs:
            del kwargs['_for_update']

        orderby = kwargs.get('_orderby', None)
        if '_orderby' in kwargs:
            del kwargs['_orderby']

        if key is not DEFAULT:
            if isinstance(key, Query):
                record = self._db(key).select(
                    limitby=(0, 1),
                    for_update=for_update,
                    orderby=orderby,
                    orderby_on_limitby=False).first()
            elif not str(key).isdigit():
                record = None
            else:
                record = self._db(self._id == key).select(
                    limitby=(0, 1),
                    for_update=for_update,
                    orderby=orderby,
                    orderby_on_limitby=False).first()
            if record:
                for k, v in iteritems(kwargs):
                    if record[k] != v:
                        return None
            return record
        elif kwargs:
            query = reduce(lambda a, b: a & b, [
                           self[k] == v for k, v in iteritems(kwargs)])
            return self._db(query).select(limitby=(0, 1),
                                          for_update=for_update,
                                          orderby=orderby,
                                          orderby_on_limitby=False).first()
        else:
            return None

    def __setitem__(self, key, value):
        if isinstance(key, dict) and isinstance(value, dict):
            """ option for keyed table """
            if set(key.keys()) == set(self._primarykey):
                value = self._filter_fields(value)
                kv = {}
                kv.update(value)
                kv.update(key)
                if not self.insert(**kv):
                    query = self._build_query(key)
                    self._db(query).update(**self._filter_fields(value))
            else:
                raise SyntaxError(
                    'key must have all fields from primary key: %s' %
                    self._primarykey)
        elif str(key).isdigit():
            if key == 0:
                self.insert(**self._filter_fields(value))
            elif self._db(self._id == key)\
                    .update(**self._filter_fields(value)) is None:
                raise SyntaxError('No such record: %s' % key)
        else:
            if isinstance(key, dict):
                raise SyntaxError(
                    'value must be a dictionary: %s' % value)
            self.__dict__[str(key)] = value
            if isinstance(value, (FieldVirtual, FieldMethod)):
                if value.name == 'unknown':
                    value.name = str(key)
                if isinstance(value, FieldVirtual):
                    self._virtual_fields.append(value)
                else:
                    self._virtual_methods.append(value)

    def __setattr__(self, key, value):
        if key[:1] != '_' and key in self:
            raise SyntaxError(
                'Object exists and cannot be redefined: %s' % key)
        self[key] = value

    def __delitem__(self, key):
        if isinstance(key, dict):
            query = self._build_query(key)
            if not self._db(query).delete():
                raise SyntaxError('No such record: %s' % key)
        elif not str(key).isdigit() or \
                not self._db(self._id == key).delete():
            raise SyntaxError('No such record: %s' % key)

    def __iter__(self):
        for fieldname in self.fields:
            yield self[fieldname]

    def __repr__(self):
        return '<Table %s (%s)>' % (self._tablename, ', '.join(self.fields()))

    def __str__(self):
        if self._ot is not None:
            ot = self._ot
            if 'Oracle' in str(type(self._db._adapter)):
                return '%s %s' % (ot, self._tablename)
            return '%s AS %s' % (ot, self._tablename)

        return self._tablename

    @property
    def sqlsafe(self):
        rname = self._rname
        if rname:
            return rname
        return self._db._adapter.sqlsafe_table(self._tablename)

    @property
    def sqlsafe_alias(self):
        rname = self._rname
        ot = self._ot
        if rname and not ot:
            return rname
        return self._db._adapter.sqlsafe_table(self._tablename, self._ot)

    def _drop(self, mode=''):
        return self._db._adapter.dialect.drop_table(self, mode)

    def drop(self, mode=''):
        return self._db._adapter.drop_table(self, mode)

    def _listify(self, fields, update=False):
        new_fields = {}  # format: new_fields[name] = (field, value)

        # store all fields passed as input in new_fields
        for name in fields:
            if name not in self.fields:
                if name != 'id':
                    raise SyntaxError(
                        'Field %s does not belong to the table' % name)
            else:
                field = self[name]
                value = fields[name]
                if field.filter_in:
                    value = field.filter_in(value)
                new_fields[name] = (field, value)

        # check all fields that should be in the table but are not passed
        to_compute = []
        for ofield in self:
            name = ofield.name
            if name not in new_fields:
                # if field is supposed to be computed, compute it!
                if ofield.compute:  # save those to compute for later
                    to_compute.append((name, ofield))
                # if field is required, check its default value
                elif not update and ofield.default is not None:
                    value = ofield.default
                    fields[name] = value
                    new_fields[name] = (ofield, value)
                # if this is an update, user the update field instead
                elif update and ofield.update is not None:
                    value = ofield.update
                    fields[name] = value
                    new_fields[name] = (ofield, value)
                # if the field is still not there but it should, error
                elif not update and ofield.required:
                    raise RuntimeError(
                        'Table: missing required field: %s' % name)
        # now deal with fields that are supposed to be computed
        if to_compute:
            row = Row(fields)
            for name, ofield in to_compute:
                # try compute it
                try:
                    row[name] = new_value = ofield.compute(row)
                    new_fields[name] = (ofield, new_value)
                except (KeyError, AttributeError):
                    # error silently unless field is required!
                    if ofield.required:
                        raise SyntaxError('unable to compute field: %s' % name)
                    elif ofield.default is not None:
                        row[name] = new_value = ofield.default
                        new_fields[name] = (ofield, new_value)
        return list(new_fields.values())

    def _attempt_upload(self, fields):
        for field in self:
            if field.type == 'upload' and field.name in fields:
                value = fields[field.name]
                if not (value is None or isinstance(value, string_types)):
                    if not PY2 and isinstance(value, bytes):
                        continue
                    if hasattr(value, 'file') and hasattr(value, 'filename'):
                        new_name = field.store(
                            value.file, filename=value.filename)
                    elif isinstance(value, dict):
                        if 'data' in value and 'filename' in value:
                            stream = BytesIO(to_bytes(value['data']))
                            new_name = field.store(
                                stream, filename=value['filename'])
                        else:
                            new_name = None
                    elif hasattr(value, 'read') and hasattr(value, 'name'):
                        new_name = field.store(value, filename=value.name)
                    else:
                        raise RuntimeError("Unable to handle upload")
                    fields[field.name] = new_name

    def _defaults(self, fields):
        """If there are no fields/values specified, return table defaults"""
        fields = copy.copy(fields)
        for field in self:
            if (field.name not in fields and
                    field.type != "id" and
                    field.compute is None and
                    field.default is not None):
                fields[field.name] = field.default
        return fields

    def _insert(self, **fields):
        fields = self._defaults(fields)
        return self._db._adapter._insert(self, self._listify(fields))

    def insert(self, **fields):
        fields = self._defaults(fields)
        self._attempt_upload(fields)
        if any(f(fields) for f in self._before_insert):
            return 0
        ret = self._db._adapter.insert(self, self._listify(fields))
        if ret and self._after_insert:
            fields = Row(fields)
            [f(fields, ret) for f in self._after_insert]
        return ret

    def _validate_fields(self, fields, defattr='default'):
        response = Row()
        response.id, response.errors = None, Row()
        new_fields = copy.copy(fields)
        for fieldname in self.fields:
            default = getattr(self[fieldname], defattr)
            if callable(default):
                default = default()
            raw_value = fields.get(fieldname, default)
            value, error = self[fieldname].validate(raw_value)
            if error:
                response.errors[fieldname] = "%s" % error
            elif value is not None:
                new_fields[fieldname] = value
        return response, new_fields

    def validate_and_insert(self, **fields):
        response, new_fields = self._validate_fields(fields)
        if not response.errors:
            response.id = self.insert(**new_fields)
        return response

    def validate_and_update(self, _key=DEFAULT, **fields):
        response, new_fields = self._validate_fields(fields, 'update')
        #: select record(s) for update
        if _key is DEFAULT:
            record = self(**fields)
        elif isinstance(_key, dict):
            record = self(**_key)
        else:
            record = self(_key)
        #: do the update
        if not response.errors and record:
            if '_id' in self:
                myset = self._db(self._id == record[self._id.name])
            else:
                query = None
                for key, value in iteritems(_key):
                    if query is None:
                        query = getattr(self, key) == value
                    else:
                        query = query & (getattr(self, key) == value)
                myset = self._db(query)
            response.id = myset.update(**new_fields)
        return response

    def update_or_insert(self, _key=DEFAULT, **values):
        if _key is DEFAULT:
            record = self(**values)
        elif isinstance(_key, dict):
            record = self(**_key)
        else:
            record = self(_key)
        if record:
            record.update_record(**values)
            newid = None
        else:
            newid = self.insert(**values)
        return newid

    def validate_and_update_or_insert(self, _key=DEFAULT, **fields):
        if _key is DEFAULT or _key == '':
            primary_keys = {}
            for key, value in iteritems(fields):
                if key in self._primarykey:
                    primary_keys[key] = value
            if primary_keys != {}:
                record = self(**primary_keys)
                _key = primary_keys
            else:
                required_keys = {}
                for key, value in iteritems(fields):
                    if getattr(self, key).required:
                        required_keys[key] = value
                record = self(**required_keys)
                _key = required_keys
        elif isinstance(_key, dict):
            record = self(**_key)
        else:
            record = self(_key)

        if record:
            response = self.validate_and_update(_key, **fields)
            if hasattr(self, '_primarykey'):
                primary_keys = {}
                for key in self._primarykey:
                    primary_keys[key] = getattr(record, key)
                response.id = primary_keys
        else:
            response = self.validate_and_insert(**fields)
        return response

    def bulk_insert(self, items):
        """
        here items is a list of dictionaries
        """
        listify_items = [self._listify(item) for item in items]
        if any(f(item) for item in items for f in self._before_insert):
            return 0
        ret = self._db._adapter.bulk_insert(self, listify_items)
        ret and [
            [f(item, ret[k]) for k, item in enumerate(items)]
            for f in self._after_insert]
        return ret

    def _truncate(self, mode=''):
        return self._db._adapter.dialect.truncate(self, mode)

    def truncate(self, mode=''):
        return self._db._adapter.truncate(self, mode)

    def import_from_csv_file(self,
                             csvfile,
                             id_map = None,
                             null = '<NULL>',
                             unique = 'uuid',
                             id_offset = None,  # id_offset used only when id_map is None
                             transform = None,
                             *args, **kwargs
                             ):
        """
        Import records from csv file.
        Column headers must have same names as table fields.
        Field 'id' is ignored.
        If column names read 'table.file' the 'table.' prefix is ignored.

        - 'unique' argument is a field which must be unique (typically a
          uuid field)
        - 'restore' argument is default False; if set True will remove old values
          in table first.
        - 'id_map' if set to None will not map ids

        The import will keep the id numbers in the restored table.
        This assumes that there is an field of type id that is integer and in
        incrementing order.
        Will keep the id numbers in restored table.
        """

        delimiter = kwargs.get('delimiter', ',')
        quotechar = kwargs.get('quotechar', '"')
        quoting = kwargs.get('quoting', csv.QUOTE_MINIMAL)
        restore = kwargs.get('restore', False)
        if restore:
            self._db[self].truncate()

        reader = csv.reader(csvfile, delimiter=delimiter,
                            quotechar=quotechar, quoting=quoting)
        colnames = None
        if isinstance(id_map, dict):
            if self._tablename not in id_map:
                id_map[self._tablename] = {}
            id_map_self = id_map[self._tablename]

        def fix(field, value, id_map, id_offset):
            list_reference_s = 'list:reference'
            if value == null:
                value = None
            elif field.type == 'blob':
                value = base64.b64decode(value)
            elif field.type == 'double' or field.type == 'float':
                if not value.strip():
                    value = None
                else:
                    value = float(value)
            elif field.type in ('integer', 'bigint'):
                if not value.strip():
                    value = None
                else:
                    value = long(value)
            elif field.type.startswith('list:string'):
                value = bar_decode_string(value)
            elif field.type.startswith(list_reference_s):
                ref_table = field.type[len(list_reference_s):].strip()
                if id_map is not None:
                    value = [id_map[ref_table][long(v)]
                             for v in bar_decode_string(value)]
                else:
                    value = [v for v in bar_decode_string(value)]
            elif field.type.startswith('list:'):
                value = bar_decode_integer(value)
            elif id_map and field.type.startswith('reference'):
                try:
                    value = id_map[field.type[9:].strip()][long(value)]
                except KeyError:
                    pass
            elif id_offset and field.type.startswith('reference'):
                try:
                    value = id_offset[field.type[9:].strip()]+long(value)
                except KeyError:
                    pass
            return value

        def is_id(colname):
            if colname in self:
                return self[colname].type == 'id'
            else:
                return False

        first = True
        unique_idx = None
        for lineno, line in enumerate(reader):
            if not line:
                return
            if not colnames:
                # assume this is the first line of the input, contains colnames
                colnames = [x.split('.', 1)[-1] for x in line][:len(line)]

                cols, cid = {}, None
                for i, colname in enumerate(colnames):
                    if is_id(colname):
                        cid = colname
                    elif colname in self.fields:
                        cols[colname] = self[colname]
                    if colname == unique:
                        unique_idx = i
            elif len(line)==len(colnames):
                # every other line contains instead data
                items = dict(zip(colnames, line))
                if transform:
                    items = transform(items)

                ditems = dict()
                csv_id = None
                for field in self:
                    fieldname = field.name
                    if fieldname in items:
                        try:
                            value = fix(field, items[fieldname], id_map, id_offset)
                            if field.type!='id':
                                ditems[fieldname] = value
                            else:
                                csv_id = long(value)
                        except ValueError:
                            raise RuntimeError("Unable to parse line:%s" % (lineno+1))
                if not (id_map or csv_id is None or id_offset is None or unique_idx):
                    curr_id = self.insert(**ditems)
                    if first:
                        first = False
                        # First curr_id is bigger than csv_id,
                        # then we are not restoring but
                        # extending db table with csv db table
                        id_offset[self._tablename] = (curr_id-csv_id) \
                            if curr_id > csv_id else 0
                    # create new id until we get the same as old_id+offset
                    while curr_id < csv_id+id_offset[self._tablename]:
                        self._db(self[cid] == curr_id).delete()
                        curr_id = self.insert(**ditems)
                # Validation. Check for duplicate of 'unique' &,
                # if present, update instead of insert.
                elif not unique_idx:
                    new_id = self.insert(**ditems)
                else:
                    unique_value = line[unique_idx]
                    query = self[unique] == unique_value
                    record = self._db(query).select().first()
                    if record:
                        record.update_record(**ditems)
                        new_id = record[self._id.name]
                    else:
                        new_id = self.insert(**ditems)
                if id_map and csv_id is not None:
                    id_map_self[csv_id] = new_id
            if lineno % 1000 == 999:
                self._db.commit()

    def as_dict(self, flat=False, sanitize=True):
        table_as_dict = dict(
            tablename=str(self),
            fields=[],
            sequence_name=self._sequence_name,
            trigger_name=self._trigger_name,
            common_filter=self._common_filter,
            format=self._format,
            singular=self._singular,
            plural=self._plural)

        for field in self:
            if (field.readable or field.writable) or (not sanitize):
                table_as_dict["fields"].append(field.as_dict(
                    flat=flat, sanitize=sanitize))
        return table_as_dict

    def with_alias(self, alias):
        return self._db._adapter.alias(self, alias)

    def on(self, query):
        return Expression(self._db, self._db._adapter.dialect.on, self, query)

    def create_index(self, name, *fields, **kwargs):
        return self._db._adapter.create_index(self, name, *fields, **kwargs)

    def drop_index(self, name):
        return self._db._adapter.drop_index(self, name)


def _expression_wrap(wrapper):
    def wrap(self, *args, **kwargs):
        return wrapper(self, *args, **kwargs)
    return wrap


class Expression(object):
    _dialect_expressions_ = {}

    def __new__(cls, *args, **kwargs):
        for name, wrapper in iteritems(cls._dialect_expressions_):
            setattr(cls, name, _expression_wrap(wrapper))
        new_cls = super(Expression, cls).__new__(cls)
        return new_cls

    def __init__(self, db, op, first=None, second=None, type=None,
                 **optional_args):
        self.db = db
        self.op = op
        self.first = first
        self.second = second
        self._table = getattr(first, '_table', None)
        if not type and first and hasattr(first, 'type'):
            self.type = first.type
        else:
            self.type = type
        if isinstance(self.type, str):
            self._itype = REGEX_TYPE.match(self.type).group(0)
        else:
            self._itype = None
        self.optional_args = optional_args

    @property
    def _dialect(self):
        return self.db._adapter.dialect

    def sum(self):
        return Expression(
            self.db, self._dialect.aggregate, self, 'SUM', self.type)

    def max(self):
        return Expression(
            self.db, self._dialect.aggregate, self, 'MAX', self.type)

    def min(self):
        return Expression(
            self.db, self._dialect.aggregate, self, 'MIN', self.type)

    def len(self):
        return Expression(
            self.db, self._dialect.length, self, None, 'integer')

    def avg(self):
        return Expression(
            self.db, self._dialect.aggregate, self, 'AVG', self.type)

    def abs(self):
        return Expression(
            self.db, self._dialect.aggregate, self, 'ABS', self.type)

    def lower(self):
        return Expression(
            self.db, self._dialect.lower, self, None, self.type)

    def upper(self):
        return Expression(
            self.db, self._dialect.upper, self, None, self.type)

    def replace(self, a, b):
        return Expression(
            self.db, self._dialect.replace, self, (a, b), self.type)

    def year(self):
        return Expression(
            self.db, self._dialect.extract, self, 'year', 'integer')

    def month(self):
        return Expression(
            self.db, self._dialect.extract, self, 'month', 'integer')

    def day(self):
        return Expression(
            self.db, self._dialect.extract, self, 'day', 'integer')

    def hour(self):
        return Expression(
            self.db, self._dialect.extract, self, 'hour', 'integer')

    def minutes(self):
        return Expression(
            self.db, self._dialect.extract, self, 'minute', 'integer')

    def coalesce(self, *others):
        return Expression(
            self.db, self._dialect.coalesce, self, others, self.type)

    def coalesce_zero(self):
        return Expression(
            self.db, self._dialect.coalesce_zero, self, None, self.type)

    def seconds(self):
        return Expression(
            self.db, self._dialect.extract, self, 'second', 'integer')

    def epoch(self):
        return Expression(
            self.db, self._dialect.epoch, self, None, 'integer')

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop

            db = self.db
            if start < 0:
                pos0 = '(%s - %d)' % (self.len(), abs(start) - 1)
            else:
                pos0 = start + 1

            maxint = sys.maxint if PY2 else sys.maxsize
            if stop is None or stop == maxint:
                length = self.len()
            elif stop < 0:
                length = '(%s - %d - %s)' % (self.len(), abs(stop) - 1, pos0)
            else:
                length = '(%s - %s)' % (stop + 1, pos0)

            return Expression(db, self._dialect.substring,
                              self, (pos0, length), self.type)
        else:
            return self[i:i + 1]

    def __str__(self):
        return str(self.db._adapter.expand(self, self.type))

    def __or__(self, other):  # for use in sortby
        return Expression(self.db, self._dialect.comma, self, other, self.type)

    def __invert__(self):
        if hasattr(self, '_op') and self.op == self._dialect.invert:
            return self.first
        return Expression(self.db, self._dialect.invert, self, type=self.type)

    def __add__(self, other):
        return Expression(self.db, self._dialect.add, self, other, self.type)

    def __sub__(self, other):
        if self.type in ('integer', 'bigint'):
            result_type = 'integer'
        elif self.type in ['date', 'time', 'datetime', 'double', 'float']:
            result_type = 'double'
        elif self.type.startswith('decimal('):
            result_type = self.type
        else:
            raise SyntaxError("subtraction operation not supported for type")
        return Expression(self.db, self._dialect.sub, self, other, result_type)

    def __mul__(self, other):
        return Expression(self.db, self._dialect.mul, self, other, self.type)

    def __div__(self, other):
        return Expression(self.db, self._dialect.div, self, other, self.type)

    def __truediv__(self, other):
        return self.__div__(other)

    def __mod__(self, other):
        return Expression(self.db, self._dialect.mod, self, other, self.type)

    def __eq__(self, value):
        return Query(self.db, self._dialect.eq, self, value)

    def __ne__(self, value):
        return Query(self.db, self._dialect.ne, self, value)

    def __lt__(self, value):
        return Query(self.db, self._dialect.lt, self, value)

    def __le__(self, value):
        return Query(self.db, self._dialect.lte, self, value)

    def __gt__(self, value):
        return Query(self.db, self._dialect.gt, self, value)

    def __ge__(self, value):
        return Query(self.db, self._dialect.gte, self, value)

    def like(self, value, case_sensitive=True, escape=None):
        op = case_sensitive and self._dialect.like or self._dialect.ilike
        return Query(self.db, op, self, value, escape=escape)

    def ilike(self, value, escape=None):
        return self.like(value, case_sensitive=False, escape=escape)

    def regexp(self, value):
        return Query(self.db, self._dialect.regexp, self, value)

    def belongs(self, *value, **kwattr):
        """
        Accepts the following inputs::

           field.belongs(1, 2)
           field.belongs((1, 2))
           field.belongs(query)

        Does NOT accept:

               field.belongs(1)

        If the set you want back includes `None` values, you can do::

            field.belongs((1, None), null=True)

        """
        db = self.db
        if len(value) == 1:
            value = value[0]
        if isinstance(value, Query):
            value = db(value)._select(value.first._table._id)
        elif not isinstance(value, basestring):
            value = set(value)
            if kwattr.get('null') and None in value:
                value.remove(None)
                return (self == None) | Query(
                    self.db, self._dialect.belongs, self, value)
        return Query(self.db, self._dialect.belongs, self, value)

    def startswith(self, value):
        if self.type not in ('string', 'text', 'json', 'upload'):
            raise SyntaxError("startswith used with incompatible field type")
        return Query(self.db, self._dialect.startswith, self, value)

    def endswith(self, value):
        if self.type not in ('string', 'text', 'json', 'upload'):
            raise SyntaxError("endswith used with incompatible field type")
        return Query(self.db, self._dialect.endswith, self, value)

    def contains(self, value, all=False, case_sensitive=False):
        """
        For GAE contains() is always case sensitive
        """
        if isinstance(value, (list, tuple)):
            subqueries = [self.contains(str(v), case_sensitive=case_sensitive)
                          for v in value if str(v)]
            if not subqueries:
                return self.contains('')
            else:
                return reduce(all and AND or OR, subqueries)
        if self.type not in ('string', 'text', 'json', 'upload') and not \
           self.type.startswith('list:'):
            raise SyntaxError("contains used with incompatible field type")
        return Query(
            self.db, self._dialect.contains, self, value,
            case_sensitive=case_sensitive)

    def with_alias(self, alias):
        return Expression(self.db, self._dialect._as, self, alias, self.type)

    # GIS expressions

    def st_asgeojson(self, precision=15, options=0, version=1):
        return Expression(self.db, self.db._adapter.ST_ASGEOJSON, self,
                          dict(precision=precision, options=options,
                               version=version), 'string')

    def st_astext(self):
        return Expression(
            self.db, self._dialect.st_astext, self, type='string')

    def st_x(self):
        return Expression(self.db, self._dialect.st_x, self, type='string')

    def st_y(self):
        return Expression(self.db, self._dialect.st_y, self, type='string')

    def st_distance(self, other):
        return Expression(
            self.db, self._dialect.st_distance, self, other, 'double')

    def st_simplify(self, value):
        return Expression(
            self.db, self._dialect.st_simplify, self, value, self.type)

    def st_simplifypreservetopology(self, value):
        return Expression(
            self.db, self._dialect.st_simplifypreservetopology, self, value,
            self.type)

    # GIS queries

    def st_contains(self, value):
        return Query(self.db, self._dialect.st_contains, self, value)

    def st_equals(self, value):
        return Query(self.db, self._dialect.st_equals, self, value)

    def st_intersects(self, value):
        return Query(self.db, self._dialect.st_intersects, self, value)

    def st_overlaps(self, value):
        return Query(self.db, self._dialect.st_overlaps, self, value)

    def st_touches(self, value):
        return Query(self.db, self._dialect.st_touches, self, value)

    def st_within(self, value):
        return Query(self.db, self._dialect.st_within, self, value)

    def st_dwithin(self, value, distance):
        return Query(
            self.db, self._dialect.st_dwithin, self, (value, distance))


class FieldVirtual(object):
    def __init__(self, name, f=None, ftype='string', label=None,
                 table_name=None):
        # for backward compatibility
        (self.name, self.f) = (name, f) if f else ('unknown', name)
        self.type = ftype
        self.label = label or self.name.capitalize().replace('_', ' ')
        self.represent = lambda v, r=None: v
        self.formatter = IDENTITY
        self.comment = None
        self.readable = True
        self.writable = False
        self.requires = None
        self.widget = None
        self.tablename = table_name
        self.filter_out = None

    def __str__(self):
        return '%s.%s' % (self.tablename, self.name)


class FieldMethod(object):
    def __init__(self, name, f=None, handler=None):
        # for backward compatibility
        (self.name, self.f) = (name, f) if f else ('unknown', name)
        self.handler = handler or VirtualCommand


@implements_bool
class Field(Expression, Serializable):

    Virtual = FieldVirtual
    Method = FieldMethod
    Lazy = FieldMethod  # for backward compatibility

    """
    Represents a database field

    Example:
        Usage::

            a = Field(name, 'string', length=32, default=None, required=False,
                requires=IS_NOT_EMPTY(), ondelete='CASCADE',
                notnull=False, unique=False,
                uploadfield=True, widget=None, label=None, comment=None,
                uploadfield=True, # True means store on disk,
                                  # 'a_field_name' means store in this field in db
                                  # False means file content will be discarded.
                writable=True, readable=True, update=None, authorize=None,
                autodelete=False, represent=None, uploadfolder=None,
                uploadseparate=False # upload to separate directories by uuid_keys
                                     # first 2 character and tablename.fieldname
                                     # False - old behavior
                                     # True - put uploaded file in
                                     #   <uploaddir>/<tablename>.<fieldname>/uuid_key[:2]
                                     #        directory)
                uploadfs=None        # a pyfilesystem where to store upload
                )

    to be used as argument of `DAL.define_table`

    """

    def __init__(self, fieldname, type='string', length=None, default=DEFAULT,
                 required=False, requires=DEFAULT, ondelete='CASCADE',
                 notnull=False, unique=False, uploadfield=True, widget=None,
                 label=None, comment=None, writable=True, readable=True,
                 update=None, authorize=None, autodelete=False, represent=None,
                 uploadfolder=None, uploadseparate=False, uploadfs=None,
                 compute=None, custom_store=None, custom_retrieve=None,
                 custom_retrieve_file_properties=None, custom_delete=None,
                 filter_in=None, filter_out=None, custom_qualifier=None,
                 map_none=None, rname=None):
        self._db = self.db = None  # both for backward compatibility
        self.op = None
        self.first = None
        self.second = None
        if PY2 and isinstance(fieldname, unicode):
            try:
                fieldname = str(fieldname)
            except UnicodeEncodeError:
                raise SyntaxError('Field: invalid unicode field name')
        self.name = fieldname = cleanup(fieldname)
        if (not isinstance(fieldname, str) or hasattr(Table, fieldname) or
                not REGEX_VALID_TB_FLD.match(fieldname) or
                REGEX_PYTHON_KEYWORDS.match(fieldname)):
            raise SyntaxError('Field: invalid field name: %s, '
                              'use rname for "funny" names' % fieldname)

        if not isinstance(type, (Table, Field)):
            self.type = type
        else:
            self.type = 'reference %s' % type

        self.length = length if length is not None else \
            DEFAULTLENGTH.get(self.type, 512)
        self.default = default if default != DEFAULT else (update or None)
        self.required = required  # is this field required
        self.ondelete = ondelete.upper()  # this is for reference fields only
        self.notnull = notnull
        self.unique = unique
        self.uploadfield = uploadfield
        self.uploadfolder = uploadfolder
        self.uploadseparate = uploadseparate
        self.uploadfs = uploadfs
        self.widget = widget
        self.comment = comment
        self.writable = writable
        self.readable = readable
        self.update = update
        self.authorize = authorize
        self.autodelete = autodelete
        self.represent = list_represent if represent is None and \
            type in ('list:integer', 'list:string') else represent
        self.compute = compute
        self.isattachment = True
        self.custom_store = custom_store
        self.custom_retrieve = custom_retrieve
        self.custom_retrieve_file_properties = custom_retrieve_file_properties
        self.custom_delete = custom_delete
        self.filter_in = filter_in
        self.filter_out = filter_out
        self.custom_qualifier = custom_qualifier
        self.label = (label if label is not None else
                      fieldname.replace('_', ' ').title())
        self.requires = requires if requires is not None else []
        self.map_none = map_none
        self._rname = rname
        stype = self.type
        if isinstance(self.type, SQLCustomType):
            stype = self.type.type
        self._itype = REGEX_TYPE.match(stype).group(0) if stype else None

    def set_attributes(self, *args, **attributes):
        self.__dict__.update(*args, **attributes)

    def clone(self, point_self_references_to=False, **args):
        field = copy.copy(self)
        if point_self_references_to and \
           field.type == 'reference %s'+field._tablename:
            field.type = 'reference %s' % point_self_references_to
        field.__dict__.update(args)
        return field

    def store(self, file, filename=None, path=None):
        # make sure filename is a str sequence
        filename = "{}".format(filename)
        if self.custom_store:
            return self.custom_store(file, filename, path)
        if isinstance(file, cgi.FieldStorage):
            filename = filename or file.filename
            file = file.file
        elif not filename:
            filename = file.name
        filename = os.path.basename(
            filename.replace('/', os.sep).replace('\\', os.sep))
        m = REGEX_STORE_PATTERN.search(filename)
        extension = m and m.group('e') or 'txt'
        uuid_key = self._db.uuid().replace('-', '')[-16:]
        encoded_filename = to_native(
            base64.b16encode(to_bytes(filename)).lower())
        newfilename = '%s.%s.%s.%s' % (
            self._tablename, self.name, uuid_key, encoded_filename)
        newfilename = newfilename[:(self.length - 1 - len(extension))] + \
            '.' + extension
        self_uploadfield = self.uploadfield
        if isinstance(self_uploadfield, Field):
            blob_uploadfield_name = self_uploadfield.uploadfield
            keys = {self_uploadfield.name: newfilename,
                    blob_uploadfield_name: file.read()}
            self_uploadfield.table.insert(**keys)
        elif self_uploadfield is True:
            if self.uploadfs:
                dest_file = self.uploadfs.open(newfilename, 'wb')
            else:
                if path:
                    pass
                elif self.uploadfolder:
                    path = self.uploadfolder
                elif self.db._adapter.folder:
                    path = pjoin(self.db._adapter.folder, '..', 'uploads')
                else:
                    raise RuntimeError(
                        "you must specify a Field(..., uploadfolder=...)")
                if self.uploadseparate:
                    if self.uploadfs:
                        raise RuntimeError("not supported")
                    path = pjoin(path, "%s.%s" % (
                        self._tablename, self.name), uuid_key[:2]
                    )
                if not exists(path):
                    os.makedirs(path)
                pathfilename = pjoin(path, newfilename)
                dest_file = open(pathfilename, 'wb')
            try:
                shutil.copyfileobj(file, dest_file)
            except IOError:
                raise IOError(
                    'Unable to store file "%s" because invalid permissions, '
                    'readonly file system, or filename too long' %
                    pathfilename)
            dest_file.close()
        return newfilename

    def retrieve(self, name, path=None, nameonly=False):
        """
        If `nameonly==True` return (filename, fullfilename) instead of
        (filename, stream)
        """
        self_uploadfield = self.uploadfield
        if self.custom_retrieve:
            return self.custom_retrieve(name, path)
        if self.authorize or isinstance(self_uploadfield, str):
            row = self.db(self == name).select().first()
            if not row:
                raise NotFoundException
        if self.authorize and not self.authorize(row):
            raise NotAuthorizedException
        file_properties = self.retrieve_file_properties(name, path)
        filename = file_properties['filename']
        if isinstance(self_uploadfield, str):  # ## if file is in DB
            stream = BytesIO(to_bytes(row[self_uploadfield] or ''))
        elif isinstance(self_uploadfield, Field):
            blob_uploadfield_name = self_uploadfield.uploadfield
            query = self_uploadfield == name
            data = self_uploadfield.table(query)[blob_uploadfield_name]
            stream = BytesIO(to_bytes(data))
        elif self.uploadfs:
            # ## if file is on pyfilesystem
            stream = self.uploadfs.open(name, 'rb')
        else:
            # ## if file is on regular filesystem
            # this is intentially a sting with filename and not a stream
            # this propagates and allows stream_file_or_304_or_206 to be called
            fullname = pjoin(file_properties['path'], name)
            if nameonly:
                return (filename, fullname)
            stream = open(fullname, 'rb')
        return (filename, stream)

    def retrieve_file_properties(self, name, path=None):
        m = REGEX_UPLOAD_PATTERN.match(name)
        if not m or not self.isattachment:
            raise TypeError('Can\'t retrieve %s file properties' % name)
        self_uploadfield = self.uploadfield
        if self.custom_retrieve_file_properties:
            return self.custom_retrieve_file_properties(name, path)
        if m.group('name'):
            try:
                filename = base64.b16decode(m.group('name'), True).decode('utf-8')
                filename = REGEX_CLEANUP_FN.sub('_', filename)
            except (TypeError, AttributeError):
                filename = name
        else:
            filename = name
        # ## if file is in DB
        if isinstance(self_uploadfield, (str, Field)):
            return dict(path=None, filename=filename)
        # ## if file is on filesystem
        if not path:
            if self.uploadfolder:
                path = self.uploadfolder
            else:
                path = pjoin(self.db._adapter.folder, '..', 'uploads')
        if self.uploadseparate:
            t = m.group('table')
            f = m.group('field')
            u = m.group('uuidkey')
            path = pjoin(path, "%s.%s" % (t, f), u[:2])
        return dict(path=path, filename=filename)

    def formatter(self, value):
        requires = self.requires
        if value is None:
            return self.map_none
        if not requires:
            return value
        if not isinstance(requires, (list, tuple)):
            requires = [requires]
        elif isinstance(requires, tuple):
            requires = list(requires)
        else:
            requires = copy.copy(requires)
        requires.reverse()
        for item in requires:
            if hasattr(item, 'formatter'):
                value = item.formatter(value)
        return value

    def validate(self, value):
        if not self.requires or self.requires == DEFAULT:
            return ((value if value != self.map_none else None), None)
        requires = self.requires
        if not isinstance(requires, (list, tuple)):
            requires = [requires]
        for validator in requires:
            (value, error) = validator(value)
            if error:
                return (value, error)
        return ((value if value != self.map_none else None), None)

    def count(self, distinct=None):
        return Expression(
            self.db, self._dialect.count, self, distinct, 'integer')

    def as_dict(self, flat=False, sanitize=True):
        attrs = (
            'name', 'authorize', 'represent', 'ondelete',
            'custom_store', 'autodelete', 'custom_retrieve',
            'filter_out', 'uploadseparate', 'widget', 'uploadfs',
            'update', 'custom_delete', 'uploadfield', 'uploadfolder',
            'custom_qualifier', 'unique', 'writable', 'compute',
            'map_none', 'default', 'type', 'required', 'readable',
            'requires', 'comment', 'label', 'length', 'notnull',
            'custom_retrieve_file_properties', 'filter_in')
        serializable = (int, long, basestring, float, tuple,
                        bool, type(None))

        def flatten(obj):
            if isinstance(obj, dict):
                return dict((flatten(k), flatten(v)) for k, v in obj.items())
            elif isinstance(obj, (tuple, list, set)):
                return [flatten(v) for v in obj]
            elif isinstance(obj, serializable):
                return obj
            elif isinstance(obj, (datetime.datetime,
                                  datetime.date, datetime.time)):
                return str(obj)
            else:
                return None

        d = dict()
        if not (sanitize and not (self.readable or self.writable)):
            for attr in attrs:
                if flat:
                    d.update({attr: flatten(getattr(self, attr))})
                else:
                    d.update({attr: getattr(self, attr)})
            d["fieldname"] = d.pop("name")
        return d

    def __bool__(self):
        return True

    def __str__(self):
        try:
            return '%s.%s' % (self.tablename, self.name)
        except:
            return '<no table>.%s' % self.name

    def __hash__(self):
        return id(self)

    @property
    def sqlsafe(self):
        if self._table:
            return self._table.sqlsafe + '.' + \
                (self._rname or self._db._adapter.sqlsafe_field(self.name))
        return '<no table>.%s' % self.name

    @property
    def sqlsafe_name(self):
        return self._rname or self._db._adapter.sqlsafe_field(self.name)


class Query(Serializable):

    """
    Necessary to define a set.
    It can be stored or can be passed to `DAL.__call__()` to obtain a `Set`

    Example:
        Use as::

            query = db.users.name=='Max'
            set = db(query)
            records = set.select()

    """

    def __init__(self,
                 db,
                 op,
                 first=None,
                 second=None,
                 ignore_common_filters=False,
                 **optional_args
                 ):
        self.db = self._db = db
        self.op = op
        self.first = first
        self.second = second
        self.ignore_common_filters = ignore_common_filters
        self.optional_args = optional_args

    @property
    def _dialect(self):
        return self.db._adapter.dialect

    def __repr__(self):
        return '<Query %s>' % str(self)

    def __str__(self):
        return str(self.db._adapter.expand(self))

    def __and__(self, other):
        return Query(self.db, self._dialect._and, self, other)

    __rand__ = __and__

    def __or__(self, other):
        return Query(self.db, self._dialect._or, self, other)

    __ror__ = __or__

    def __invert__(self):
        if self.op == self._dialect._not:
            return self.first
        return Query(self.db, self._dialect._not, self)

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return not (self == other)

    def case(self, t=1, f=0):
        return Expression(self.db, self._dialect.case, self, (t, f))

    def as_dict(self, flat=False, sanitize=True):
        """Experimental stuff

        This allows to return a plain dictionary with the basic
        query representation. Can be used with json/xml services
        for client-side db I/O

        Example:
            Usage::

                q = db.auth_user.id != 0
                q.as_dict(flat=True)
                {
                "op": "NE",
                "first":{
                    "tablename": "auth_user",
                    "fieldname": "id"
                    },
                "second":0
                }
        """

        SERIALIZABLE_TYPES = (tuple, dict, set, list, int, long, float,
                              basestring, type(None), bool)

        def loop(d):
            newd = dict()
            for k, v in d.items():
                if k in ("first", "second"):
                    if isinstance(v, self.__class__):
                        newd[k] = loop(v.__dict__)
                    elif isinstance(v, Field):
                        newd[k] = {"tablename": v._tablename,
                                   "fieldname": v.name}
                    elif isinstance(v, Expression):
                        newd[k] = loop(v.__dict__)
                    elif isinstance(v, SERIALIZABLE_TYPES):
                        newd[k] = v
                    elif isinstance(v, (datetime.date,
                                        datetime.time,
                                        datetime.datetime)):
                        newd[k] = unicode(v) if PY2 else str(v)
                elif k == "op":
                    if callable(v):
                        newd[k] = v.__name__
                    elif isinstance(v, basestring):
                        newd[k] = v
                    else:
                        pass  # not callable or string
                elif isinstance(v, SERIALIZABLE_TYPES):
                    if isinstance(v, dict):
                        newd[k] = loop(v)
                    else:
                        newd[k] = v
            return newd

        if flat:
            return loop(self.__dict__)
        else:
            return self.__dict__


class Set(Serializable):

    """
    Represents a set of records in the database.
    Records are identified by the `query=Query(...)` object.
    Normally the Set is generated by `DAL.__call__(Query(...))`

    Given a set, for example::

        myset = db(db.users.name=='Max')

    you can::

        myset.update(db.users.name='Massimo')
        myset.delete() # all elements in the set
        myset.select(orderby=db.users.id, groupby=db.users.name, limitby=(0, 10))

    and take subsets:

       subset = myset(db.users.id<5)

    """

    def __init__(self, db, query, ignore_common_filters=None):
        self.db = db
        self._db = db  # for backward compatibility
        self.dquery = None

        # if query is a dict, parse it
        if isinstance(query, dict):
            query = self.parse(query)

        if ignore_common_filters is not None and \
           use_common_filters(query) == ignore_common_filters:
            query = copy.copy(query)
            query.ignore_common_filters = ignore_common_filters
        self.query = query

    def __repr__(self):
        return '<Set %s>' % str(self.query)

    def __call__(self, query, ignore_common_filters=False):
        return self.where(query, ignore_common_filters)

    def where(self, query, ignore_common_filters=False):
        if query is None:
            return self
        elif isinstance(query, Table):
            query = self.db._adapter.id_query(query)
        elif isinstance(query, str):
            query = Expression(self.db, query)
        elif isinstance(query, Field):
            query = query != None
        if self.query:
            return Set(self.db, self.query & query,
                       ignore_common_filters=ignore_common_filters)
        else:
            return Set(self.db, query,
                       ignore_common_filters=ignore_common_filters)

    def _count(self, distinct=None):
        return self.db._adapter._count(self.query, distinct)

    def _select(self, *fields, **attributes):
        adapter = self.db._adapter
        tablenames = adapter.tables(self.query,
                                    attributes.get('join', None),
                                    attributes.get('left', None),
                                    attributes.get('orderby', None),
                                    attributes.get('groupby', None))
        fields = adapter.expand_all(fields, tablenames)
        return adapter._select(self.query, fields, attributes)

    def _delete(self):
        db = self.db
        tablename = db._adapter.get_table(self.query)
        return db._adapter._delete(tablename, self.query)

    def _update(self, **update_fields):
        db = self.db
        tablename = db._adapter.get_table(self.query)
        fields = db[tablename]._listify(update_fields, update=True)
        return db._adapter._update(tablename, self.query, fields)

    def as_dict(self, flat=False, sanitize=True):
        if flat:
            uid = dbname = uri = None
            codec = self.db._db_codec
            if not sanitize:
                uri, dbname, uid = (self.db._dbname, str(self.db),
                                    self.db._db_uid)
            d = {"query": self.query.as_dict(flat=flat)}
            d["db"] = {"uid": uid, "codec": codec,
                       "name": dbname, "uri": uri}
            return d
        else:
            return self.__dict__

    def parse(self, dquery):
        """Experimental: Turn a dictionary into a Query object"""
        self.dquery = dquery
        return self.build(self.dquery)

    def build(self, d):
        """Experimental: see .parse()"""
        op, first, second = (d["op"], d["first"],
                             d.get("second", None))
        left = right = built = None

        if op in ("AND", "OR"):
            if not (type(first), type(second)) == (dict, dict):
                raise SyntaxError("Invalid AND/OR query")
            if op == "AND":
                built = self.build(first) & self.build(second)
            else:
                built = self.build(first) | self.build(second)
        elif op == "NOT":
            if first is None:
                raise SyntaxError("Invalid NOT query")
            built = ~self.build(first)
        else:
            # normal operation (GT, EQ, LT, ...)
            for k, v in {"left": first, "right": second}.items():
                if isinstance(v, dict) and v.get("op"):
                    v = self.build(v)
                if isinstance(v, dict) and ("tablename" in v):
                    v = self.db[v["tablename"]][v["fieldname"]]
                if k == "left":
                    left = v
                else:
                    right = v

            if hasattr(self.db._adapter, op):
                opm = getattr(self.db._adapter, op)

            if op == "EQ":
                built = left == right
            elif op == "NE":
                built = left != right
            elif op == "GT":
                built = left > right
            elif op == "GE":
                built = left >= right
            elif op == "LT":
                built = left < right
            elif op == "LE":
                built = left <= right
            elif op in ("JOIN", "LEFT_JOIN", "RANDOM", "ALLOW_NULL"):
                built = Expression(self.db, opm)
            elif op in ("LOWER", "UPPER", "EPOCH", "PRIMARY_KEY",
                        "COALESCE_ZERO", "RAW", "INVERT"):
                built = Expression(self.db, opm, left)
            elif op in ("COUNT", "EXTRACT", "AGGREGATE", "SUBSTRING",
                        "REGEXP", "LIKE", "ILIKE", "STARTSWITH",
                        "ENDSWITH", "ADD", "SUB", "MUL", "DIV",
                        "MOD", "AS", "ON", "COMMA", "NOT_NULL",
                        "COALESCE", "CONTAINS", "BELONGS"):
                built = Expression(self.db, opm, left, right)
            # expression as string
            elif not (left or right):
                built = Expression(self.db, op)
            else:
                raise SyntaxError("Operator not supported: %s" % op)

        return built

    def isempty(self):
        return not self.select(limitby=(0, 1), orderby_on_limitby=False)

    def count(self, distinct=None, cache=None):
        db = self.db
        if cache:
            sql = self._count(distinct=distinct)
            if isinstance(cache, dict):
                cache_model = cache['model']
                time_expire = cache['expiration']
                key = cache.get('key')
                if not key:
                    key = db._uri + '/' + sql
                    key = hashlib_md5(key).hexdigest()
            else:
                cache_model, time_expire = cache
                key = db._uri + '/' + sql
                key = hashlib_md5(key).hexdigest()
            return cache_model(
                key,
                lambda self=self, distinct=distinct: db._adapter.count(
                    self.query, distinct),
                time_expire)
        return db._adapter.count(self.query, distinct)

    def select(self, *fields, **attributes):
        adapter = self.db._adapter
        tablenames = adapter.tables(self.query,
                                    attributes.get('join', None),
                                    attributes.get('left', None),
                                    attributes.get('orderby', None),
                                    attributes.get('groupby', None))
        fields = adapter.expand_all(fields, tablenames)
        return adapter.select(self.query, fields, attributes)

    def iterselect(self, *fields, **attributes):
        adapter = self.db._adapter
        tablenames = adapter.tables(self.query,
                                    attributes.get('join', None),
                                    attributes.get('left', None),
                                    attributes.get('orderby', None),
                                    attributes.get('groupby', None))
        fields = adapter.expand_all(fields, tablenames)
        return adapter.iterselect(self.query, fields, attributes)

    def nested_select(self, *fields, **attributes):
        return Expression(self.db, self._select(*fields, **attributes))

    def delete(self):
        db = self.db
        tablename = db._adapter.get_table(self.query)
        table = db[tablename]
        if any(f(self) for f in table._before_delete):
            return 0
        ret = db._adapter.delete(tablename, self.query)
        ret and [f(self) for f in table._after_delete]
        return ret

    def update(self, **update_fields):
        db = self.db
        tablename = db._adapter.get_table(self.query)
        table = db[tablename]
        table._attempt_upload(update_fields)
        if any(f(self, update_fields) for f in table._before_update):
            return 0
        fields = table._listify(update_fields, update=True)
        if not fields:
            raise SyntaxError("No fields to update")
        ret = db._adapter.update("%s" % table._tablename, self.query, fields)
        ret and [f(self, update_fields) for f in table._after_update]
        return ret

    def update_naive(self, **update_fields):
        """
        Same as update but does not call table._before_update and _after_update
        """
        tablename = self.db._adapter.get_table(self.query)
        table = self.db[tablename]
        fields = table._listify(update_fields, update=True)
        if not fields:
            raise SyntaxError("No fields to update")

        ret = self.db._adapter.update("%s" % table, self.query, fields)
        return ret

    def validate_and_update(self, **update_fields):
        tablename = self.db._adapter.get_table(self.query)
        response = Row()
        response.errors = Row()
        new_fields = copy.copy(update_fields)
        for key, value in iteritems(update_fields):
            value, error = self.db[tablename][key].validate(value)
            if error:
                response.errors[key] = '%s' % error
            else:
                new_fields[key] = value
        table = self.db[tablename]
        if response.errors:
            response.updated = None
        else:
            if not any(f(self, new_fields) for f in table._before_update):
                table._attempt_upload(new_fields)
                fields = table._listify(new_fields, update=True)
                if not fields:
                    raise SyntaxError("No fields to update")
                ret = self.db._adapter.update(tablename, self.query, fields)
                ret and [f(self, new_fields) for f in table._after_update]
            else:
                ret = 0
            response.updated = ret
        return response

    def delete_uploaded_files(self, upload_fields=None):
        table = self.db[self.db._adapter.tables(self.query)[0]]
        # ## mind uploadfield==True means file is not in DB
        if upload_fields:
            fields = list(upload_fields)
            # Explicitly add compute upload fields (ex: thumbnail)
            fields += [f for f in table.fields if table[f].compute is not None]
        else:
            fields = table.fields
        fields = [f for f in fields if table[f].type == 'upload' and
                  table[f].uploadfield == True and
                  table[f].autodelete]
        if not fields:
            return False
        for record in self.select(*[table[f] for f in fields]):
            for fieldname in fields:
                field = table[fieldname]
                oldname = record.get(fieldname, None)
                if not oldname:
                    continue
                if upload_fields and fieldname in upload_fields and \
                   oldname == upload_fields[fieldname]:
                    continue
                if field.custom_delete:
                    field.custom_delete(oldname)
                else:
                    uploadfolder = field.uploadfolder
                    if not uploadfolder:
                        uploadfolder = pjoin(
                            self.db._adapter.folder, '..', 'uploads')
                    if field.uploadseparate:
                        items = oldname.split('.')
                        uploadfolder = pjoin(
                            uploadfolder, "%s.%s" %
                            (items[0], items[1]), items[2][:2])
                    oldpath = pjoin(uploadfolder, oldname)
                    if exists(oldpath):
                        os.unlink(oldpath)
        return False


class LazyReferenceGetter(object):
    def __init__(self, table, id):
        self.db, self.tablename, self.id = table._db, table._tablename, id

    def __call__(self, other_tablename):
        if self.db._lazy_tables is False:
            raise AttributeError()
        table = self.db[self.tablename]
        other_table = self.db[other_tablename]
        for rfield in table._referenced_by:
            if rfield.table == other_table:
                return LazySet(rfield, self.id)
        raise AttributeError()


class LazySet(object):
    def __init__(self, field, id):
        self.db, self.tablename, self.fieldname, self.id = \
            field.db, field._tablename, field.name, id

    def _getset(self):
        query = self.db[self.tablename][self.fieldname] == self.id
        return Set(self.db, query)

    def __repr__(self):
        return repr(self._getset())

    def __call__(self, query, ignore_common_filters=False):
        return self.where(query, ignore_common_filters)

    def where(self, query, ignore_common_filters=False):
        return self._getset()(query, ignore_common_filters)

    def _count(self, distinct=None):
        return self._getset()._count(distinct)

    def _select(self, *fields, **attributes):
        return self._getset()._select(*fields, **attributes)

    def _delete(self):
        return self._getset()._delete()

    def _update(self, **update_fields):
        return self._getset()._update(**update_fields)

    def isempty(self):
        return self._getset().isempty()

    def count(self, distinct=None, cache=None):
        return self._getset().count(distinct, cache)

    def select(self, *fields, **attributes):
        return self._getset().select(*fields, **attributes)

    def nested_select(self, *fields, **attributes):
        return self._getset().nested_select(*fields, **attributes)

    def delete(self):
        return self._getset().delete()

    def update(self, **update_fields):
        return self._getset().update(**update_fields)

    def update_naive(self, **update_fields):
        return self._getset().update_naive(**update_fields)

    def validate_and_update(self, **update_fields):
        return self._getset().validate_and_update(**update_fields)

    def delete_uploaded_files(self, upload_fields=None):
        return self._getset().delete_uploaded_files(upload_fields)


class VirtualCommand(object):
    def __init__(self, method, row):
        self.method = method
        self.row = row

    def __call__(self, *args, **kwargs):
        return self.method(self.row, *args, **kwargs)


@implements_bool
class BasicRows(object):
    """
    Abstract class for Rows and IterRows
    """
    def __bool__(self):
        return True if self.first() is not None else False

    def __str__(self):
        """
        Serializes the table into a csv file
        """

        s = StringIO()
        self.export_to_csv_file(s)
        return s.getvalue()

    def as_trees(self, parent_name='parent_id', children_name='children',
                 render=False):
        """
        returns the data as list of trees.

        :param parent_name: the name of the field holding the reference to the
                            parent (default parent_id).
        :param children_name: the name where the children of each row will be
                              stored as a list (default children).
        :param render: whether we will render the fields using their represent
                       (default False) can be a list of fields to render or
                       True to render all.
        """
        roots = []
        drows = {}
        rows = list(self.render(fields=None if render is True else render)) \
            if render else self
        for row in rows:
            drows[row.id] = row
            row[children_name] = []
        for row in rows:
            parent = row[parent_name]
            if parent is None:
                roots.append(row)
            else:
                drows[parent][children_name].append(row)
        return roots

    def as_list(self,
                compact=True,
                storage_to_dict=True,
                datetime_to_str=False,
                custom_types=None):
        """
        Returns the data as a list or dictionary.

        Args:
            storage_to_dict: when True returns a dict, otherwise a list
            datetime_to_str: convert datetime fields as strings
        """
        (oc, self.compact) = (self.compact, compact)
        if storage_to_dict:
            items = [item.as_dict(datetime_to_str, custom_types)
                     for item in self]
        else:
            items = [item for item in self]
        self.compact = oc
        return items

    def as_dict(self,
                key='id',
                compact=True,
                storage_to_dict=True,
                datetime_to_str=False,
                custom_types=None):
        """
        Returns the data as a dictionary of dictionaries (storage_to_dict=True)
        or records (False)

        Args:
            key: the name of the field to be used as dict key, normally the id
            compact: ? (default True)
            storage_to_dict: when True returns a dict, otherwise a list(default True)
            datetime_to_str: convert datetime fields as strings (default False)
        """

        # test for multiple rows
        multi = False
        f = self.first()
        if f and isinstance(key, basestring):
            multi = any([isinstance(v, f.__class__) for v in f.values()])
            if ("." not in key) and multi:
                # No key provided, default to int indices
                def new_key():
                    i = 0
                    while True:
                        yield i
                        i += 1
                key_generator = new_key()
                key = lambda r: key_generator.next()

        rows = self.as_list(compact, storage_to_dict, datetime_to_str,
                            custom_types)
        if isinstance(key, str) and key.count('.') == 1:
            (table, field) = key.split('.')
            return dict([(r[table][field], r) for r in rows])
        elif isinstance(key, str):
            return dict([(r[key], r) for r in rows])
        else:
            return dict([(key(r), r) for r in rows])

    def xml(self, strict=False, row_name='row', rows_name='rows'):
        """
        Serializes the table using sqlhtml.SQLTABLE (if present)
        """
        if not strict and not self.db.has_representer('rows_xml'):
            strict = True

        if strict:
            return '<%s>\n%s\n</%s>' % (
                rows_name,
                '\n'.join(
                    row.as_xml(
                        row_name=row_name,
                        colnames=self.colnames
                    ) for row in self),
                rows_name
            )

        rv = self.db.represent('rows_xml', self)
        if hasattr(rv, 'xml') and callable(getattr(rv, 'xml')):
            return rv.xml()
        return rv

    def as_xml(self, row_name='row', rows_name='rows'):
        return self.xml(strict=True, row_name=row_name, rows_name=rows_name)

    def as_json(self, mode='object', default=None):
        """
        Serializes the rows to a JSON list or object with objects
        mode='object' is not implemented (should return a nested
        object structure)
        """
        items = [record.as_json(
                    mode=mode, default=default, serialize=False,
                    colnames=self.colnames
                ) for record in self]

        return serializers.json(items)

    def export_to_csv_file(self, ofile, null='<NULL>', *args, **kwargs):
        """
        Exports data to csv, the first line contains the column names

        Args:
            ofile: where the csv must be exported to
            null: how null values must be represented (default '<NULL>')
            delimiter: delimiter to separate values (default ',')
            quotechar: character to use to quote string values (default '"')
            quoting: quote system, use csv.QUOTE_*** (default csv.QUOTE_MINIMAL)
            represent: use the fields .represent value (default False)
            colnames: list of column names to use (default self.colnames)

        This will only work when exporting rows objects!!!!
        DO NOT use this with db.export_to_csv()
        """
        delimiter = kwargs.get('delimiter', ',')
        quotechar = kwargs.get('quotechar', '"')
        quoting = kwargs.get('quoting', csv.QUOTE_MINIMAL)
        represent = kwargs.get('represent', False)
        writer = csv.writer(ofile, delimiter=delimiter,
                            quotechar=quotechar, quoting=quoting)

        def unquote_colnames(colnames):
            unq_colnames = []
            for col in colnames:
                m = self.db._adapter.REGEX_TABLE_DOT_FIELD.match(col)
                if not m:
                    unq_colnames.append(col)
                else:
                    unq_colnames.append('.'.join(m.groups()))
            return unq_colnames

        colnames = kwargs.get('colnames', self.colnames)
        write_colnames = kwargs.get('write_colnames', True)
        # a proper csv starting with the column names
        if write_colnames:
            writer.writerow(unquote_colnames(colnames))

        def none_exception(value):
            """
            Returns a cleaned up value that can be used for csv export:

            - unicode text is encoded as such
            - None values are replaced with the given representation (default <NULL>)
            """
            if value is None:
                return null
            elif PY2 and isinstance(value, unicode):
                return value.encode('utf8')
            elif isinstance(value, Reference):
                return long(value)
            elif hasattr(value, 'isoformat'):
                return value.isoformat()[:19].replace('T', ' ')
            elif isinstance(value, (list, tuple)):  # for type='list:..'
                return bar_encode(value)
            return value

        repr_cache = {}
        for record in self:
            row = []
            for col in colnames:
                m = self.db._adapter.REGEX_TABLE_DOT_FIELD.match(col)
                if not m:
                    row.append(record._extra[col])
                else:
                    (t, f) = m.groups()
                    field = self.db[t][f]
                    if isinstance(record.get(t, None), (Row, dict)):
                        value = record[t][f]
                    else:
                        value = record[f]
                    if field.type == 'blob' and value is not None:
                        value = base64.b64encode(value)
                    elif represent and field.represent:
                        if field.type.startswith('reference'):
                            if field not in repr_cache:
                                repr_cache[field] = {}
                            if value not in repr_cache[field]:
                                repr_cache[field][value] = field.represent(
                                    value, record
                                )
                            value = repr_cache[field][value]
                        else:
                            value = field.represent(value, record)
                    row.append(none_exception(value))
            writer.writerow(row)

    # for consistent naming yet backwards compatible
    as_csv = __str__
    json = as_json


class Rows(BasicRows):
    """
    A wrapper for the return value of a select. It basically represents a table.
    It has an iterator and each row is represented as a `Row` dictionary.
    """

    # ## TODO: this class still needs some work to care for ID/OID

    def __init__(self, db=None, records=[], colnames=[], compact=True,
                 rawrows=None):
        self.db = db
        self.records = records
        self.colnames = colnames
        self.compact = compact
        self.response = rawrows

    def __repr__(self):
        return '<Rows (%s)>' % len(self.records)

    def setvirtualfields(self, **keyed_virtualfields):
        """
        For reference::

            db.define_table('x', Field('number', 'integer'))
            if db(db.x).isempty(): [db.x.insert(number=i) for i in range(10)]

            from gluon.dal import lazy_virtualfield

            class MyVirtualFields(object):
                # normal virtual field (backward compatible, discouraged)
                def normal_shift(self): return self.x.number+1
                # lazy virtual field (because of @staticmethod)
                @lazy_virtualfield
                def lazy_shift(instance, row, delta=4): return row.x.number+delta
            db.x.virtualfields.append(MyVirtualFields())

            for row in db(db.x).select():
                print row.number, row.normal_shift, row.lazy_shift(delta=7)

        """
        if not keyed_virtualfields:
            return self
        for row in self.records:
            for (tablename, virtualfields) in iteritems(keyed_virtualfields):
                attributes = dir(virtualfields)
                if tablename not in row:
                    box = row[tablename] = Row()
                else:
                    box = row[tablename]
                updated = False
                for attribute in attributes:
                    if attribute[0] != '_':
                        method = getattr(virtualfields, attribute)
                        if hasattr(method, '__lazy__'):
                            box[attribute] = VirtualCommand(method, row)
                        elif type(method) == types.MethodType:
                            if not updated:
                                virtualfields.__dict__.update(row)
                                updated = True
                            box[attribute] = method()
        return self

    def __and__(self, other):
        if self.colnames != other.colnames:
            raise Exception('Cannot & incompatible Rows objects')
        records = self.records + other.records
        return self.__class__(
            self.db, records, self.colnames,
            compact=self.compact or other.compact)

    def __or__(self, other):
        if self.colnames != other.colnames:
            raise Exception('Cannot | incompatible Rows objects')
        records = [record for record in other.records
                   if record not in self.records]
        records = self.records + records
        return self.__class__(
            self.db, records, self.colnames,
            compact=self.compact or other.compact)

    def __len__(self):
        return len(self.records)

    def __getslice__(self, a, b):
        return self.__class__(
            self.db, self.records[a:b], self.colnames, compact=self.compact)

    def __getitem__(self, i):
        row = self.records[i]
        keys = list(row.keys())
        if self.compact and len(keys) == 1 and keys[0] != '_extra':
            return row[keys[0]]
        return row

    def __iter__(self):
        """
        Iterator over records
        """

        for i in xrange(len(self)):
            yield self[i]

    def __eq__(self, other):
        if isinstance(other, Rows):
            return (self.records == other.records)
        else:
            return False

    def column(self, column=None):
        return [r[str(column) if column else self.colnames[0]] for r in self]

    def first(self):
        if not self.records:
            return None
        return self[0]

    def last(self):
        if not self.records:
            return None
        return self[-1]

    def append(self, row):
        self.records.append(row)

    def insert(self, position, row):
        self.records.insert(position, row)

    def find(self, f, limitby=None):
        """
        Returns a new Rows object, a subset of the original object,
        filtered by the function `f`
        """
        if not self:
            return self.__class__(
                self.db, [], self.colnames, compact=self.compact)
        records = []
        if limitby:
            a, b = limitby
        else:
            a, b = 0, len(self)
        k = 0
        for i, row in enumerate(self):
            if f(row):
                if a <= k:
                    records.append(self.records[i])
                k += 1
                if k == b:
                    break
        return self.__class__(
            self.db, records, self.colnames, compact=self.compact)

    def exclude(self, f):
        """
        Removes elements from the calling Rows object, filtered by the function
        `f`, and returns a new Rows object containing the removed elements
        """
        if not self.records:
            return self.__class__(
                self.db, [], self.colnames, compact=self.compact)
        removed = []
        i = 0
        while i < len(self):
            row = self[i]
            if f(row):
                removed.append(self.records[i])
                del self.records[i]
            else:
                i += 1
        return self.__class__(
            self.db, removed, self.colnames, compact=self.compact)

    def sort(self, f, reverse=False):
        """
        Returns a list of sorted elements (not sorted in place)
        """
        rows = self.__class__(self.db, [], self.colnames, compact=self.compact)
        # When compact=True, iterating over self modifies each record,
        # so when sorting self, it is necessary to return a sorted
        # version of self.records rather than the sorted self directly.
        rows.records = [r for (r, s) in sorted(zip(self.records, self),
                                               key=lambda r: f(r[1]),
                                               reverse=reverse)]
        return rows

    def join(self, field, name=None, constraint=None, fields=[], orderby=None):
        if len(self) == 0: return self
        mode = 'referencing' if field.type == 'id' else 'referenced'
        func = lambda ids: field.belongs(ids)
        db, ids, maps = self.db, [], {}
        if not fields:
            fields = [f for f in field._table if f.readable]
        if mode == 'referencing':
            # try all refernced field names
            names = [name] if name else list(set(
                    f.name for f in field._table._referenced_by if f.name in self[0]))
            # get all the ids
            ids = [row.get(name) for row in self for name in names]
            # filter out the invalid ids
            ids = filter(lambda id: str(id).isdigit(), ids)
            # build the query
            query = func(ids)
            if constraint: query = query & constraint
            tmp = not field.name in [f.name for f in fields]
            if tmp:
                fields.append(field)
            other = db(query).select(*fields, orderby=orderby, cacheable=True)
            for row in other:
                id = row[field.name]
                maps[id] = row
            for row in self:
                for name in names:
                    row[name] = maps.get(row[name])
        if mode == 'referenced':
            if not name:
                name = field._tablename
            # build the query
            query = func([row.id for row in self])
            if constraint: query = query & constraint
            name = name or field._tablename
            tmp = not field.name in [f.name for f in fields]
            if tmp:
                fields.append(field)
            other = db(query).select(*fields, orderby=orderby, cacheable=True)
            for row in other:
                id = row[field]
                if not id in maps: maps[id] = []
                if tmp:
                    try:
                        del row[field.name]
                    except:
                        del row[field.tablename][field.name]
                        if not row[field.tablename] and len(row.keys())==2:
                            del row[field.tablename]
                            row = row[row.keys()[0]]
                maps[id].append(row)
            for row in self:
                row[name] = maps.get(row.id, [])
        return self


    def group_by_value(self, *fields, **args):
        """
        Regroups the rows, by one of the fields
        """
        one_result = False
        if 'one_result' in args:
            one_result = args['one_result']

        def build_fields_struct(row, fields, num, groups):
            """
            helper function:
            """
            if num > len(fields) - 1:
                if one_result:
                    return row
                else:
                    return [row]

            key = fields[num]
            value = row[key]

            if value not in groups:
                groups[value] = build_fields_struct(row, fields, num + 1, {})
            else:
                struct = build_fields_struct(
                    row, fields, num + 1, groups[value])

                # still have more grouping to do
                if isinstance(struct, dict):
                    groups[value].update()
                # no more grouping, first only is off
                elif isinstance(struct, list):
                    groups[value] += struct
                # no more grouping, first only on
                else:
                    groups[value] = struct

            return groups

        if len(fields) == 0:
            return self

        # if select returned no results
        if not self.records:
            return {}

        grouped_row_group = dict()

        # build the struct
        for row in self:
            build_fields_struct(row, fields, 0, grouped_row_group)

        return grouped_row_group

    def render(self, i=None, fields=None):
        """
        Takes an index and returns a copy of the indexed row with values
        transformed via the "represent" attributes of the associated fields.

        Args:
            i: index. If not specified, a generator is returned for iteration
                over all the rows.
            fields: a list of fields to transform (if None, all fields with
                "represent" attributes will be transformed)
        """
        if i is None:
            return (self.render(i, fields=fields) for i in range(len(self)))
        if not self.db.has_representer('rows_render'):
            raise RuntimeError("Rows.render() needs a `rows_render` \
                               representer in DAL instance")
        row = copy.deepcopy(self.records[i])
        keys = list(row.keys())
        tables = [f.tablename for f in fields] if fields \
            else [k for k in keys if k != '_extra']
        for table in tables:
            repr_fields = [f.name for f in fields if f.tablename == table] \
                if fields else [
                    k for k in row[table].keys()
                    if (hasattr(self.db[table], k) and
                        isinstance(self.db[table][k], Field) and
                        self.db[table][k].represent)]
            for field in repr_fields:
                row[table][field] = self.db.represent(
                    'rows_render', self.db[table][field], row[table][field],
                    row[table])

        if self.compact and len(keys) == 1 and keys[0] != '_extra':
            return row[keys[0]]
        return row


@implements_iterator
class IterRows(BasicRows):
    def __init__(self, db, sql, fields, colnames, blob_decode, cacheable):
        self.db = db
        self.fields = fields
        self.colnames = colnames
        self.blob_decode = blob_decode
        self.cacheable = cacheable
        (self.fields_virtual, self.fields_lazy, self.tmps) = \
            self.db._adapter._parse_expand_colnames(colnames)
        self.cursor = self.db._adapter.cursor
        self.db._adapter.execute(sql)
        self.db._adapter.lock_cursor(self.cursor)
        self._head = None
        self.last_item = None
        self.last_item_id = None
        self.compact = True
        self.sql = sql

    def __next__(self):
        db_row = self.cursor.fetchone()
        if db_row is None:
            raise StopIteration
        row = self.db._adapter._parse(db_row, self.tmps, self.fields,
                                      self.colnames, self.blob_decode,
                                      self.cacheable, self.fields_virtual,
                                      self.fields_lazy)
        if self.compact:
            # The following is to translate
            # <Row {'t0': {'id': 1L, 'name': 'web2py'}}>
            # in
            # <Row {'id': 1L, 'name': 'web2py'}>
            # normally accomplished by Rows.__get_item__
            keys = list(row.keys())
            if len(keys) == 1 and keys[0] != '_extra':
                row = row[keys[0]]
        return row

    def __iter__(self):
        if self._head:
            yield self._head
        try:
            row = next(self)
            while row is not None:
                yield row
                row = next(self)
        except StopIteration:
            # Iterator is over, adjust the cursor logic
            self.db._adapter.close_cursor(self.cursor)
            raise StopIteration
        return

    def first(self):
        if self._head is None:
            try:
                self._head = next(self)
            except StopIteration:
                # TODO should I raise something?
                return None
        return self._head

    def __getitem__(self, key):
        if not isinstance(key, (int, long)):
            raise TypeError

        if key == self.last_item_id:
            return self.last_item

        n_to_drop = key
        if self.last_item_id is not None:
            if self.last_item_id < key:
                n_to_drop -= (self.last_item_id + 1)
            else:
                raise IndexError

        # fetch and drop the first key - 1 elements
        for i in xrange(n_to_drop):
            self.cursor._fetchone()
        row = next(self)
        if row is None:
            raise IndexError
        else:
            self.last_item_id = key
            self.last_item = row
            return row

#    # rowcount it doesn't seem to be reliable on all drivers
#    def __len__(self):
#        return self.db._adapter.cursor.rowcount
