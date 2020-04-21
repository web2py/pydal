# -*- coding: utf-8 -*-
# pylint: disable=no-member,not-an-iterable

import base64
import binascii
import cgi
import copy
import csv
import datetime
import decimal
import os
import shutil
import sys
import types
import re
from collections import OrderedDict
from ._compat import (
    PY2,
    StringIO,
    BytesIO,
    pjoin,
    exists,
    hashlib_md5,
    basestring,
    iteritems,
    xrange,
    implements_iterator,
    implements_bool,
    copyreg,
    reduce,
    to_bytes,
    to_native,
    to_unicode,
    long,
    text_type,
)
from ._globals import DEFAULT, IDENTITY, AND, OR
from ._gae import Key
from .exceptions import NotFoundException, NotAuthorizedException
from .helpers.regex import (
    REGEX_TABLE_DOT_FIELD,
    REGEX_ALPHANUMERIC,
    REGEX_PYTHON_KEYWORDS,
    REGEX_UPLOAD_EXTENSION,
    REGEX_UPLOAD_PATTERN,
    REGEX_UPLOAD_CLEANUP,
    REGEX_VALID_TB_FLD,
    REGEX_TYPE,
    REGEX_TABLE_DOT_FIELD_OPTIONAL_QUOTES,
)
from .helpers.classes import (
    Reference,
    MethodAdder,
    SQLCallableList,
    SQLALL,
    Serializable,
    BasicStorage,
    SQLCustomType,
    OpRow,
    cachedprop,
)
from .helpers.methods import (
    list_represent,
    bar_decode_integer,
    bar_decode_string,
    bar_encode,
    archive_record,
    cleanup,
    use_common_filters,
    attempt_upload_on_insert,
    attempt_upload_on_update,
    delete_uploaded_files,
)
from .helpers.serializers import serializers
from .utils import deprecated

if not PY2:
    unicode = str

DEFAULTLENGTH = {
    "string": 512,
    "password": 512,
    "upload": 512,
    "text": 2 ** 15,
    "blob": 2 ** 31,
}

DEFAULT_REGEX = {
    "id": "[1-9]\d*",
    "decimal": "\d{1,10}\.\d{2}",
    "integer": "[+-]?\d*",
    "float": "[+-]?\d*(\.\d*)?",
    "double": "[+-]?\d*(\.\d*)?",
    "date": "\d{4}\-\d{2}\-\d{2}",
    "time": "\d{2}\:\d{2}(\:\d{2}(\.\d*)?)?",
    "datetime": "\d{4}\-\d{2}\-\d{2} \d{2}\:\d{2}(\:\d{2}(\.\d*)?)?",
}


def csv_reader(utf8_data, dialect=csv.excel, encoding="utf-8", **kwargs):
    """like csv.reader but allows to specify an encoding, defaults to utf-8"""
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [to_unicode(cell, encoding) for cell in row]


class Row(BasicStorage):

    """
    A dictionary that lets you do d['a'] as well as d.a
    this is only used to store a `Row`
    """

    def __getitem__(self, k):
        key = str(k)

        _extra = BasicStorage.get(self, "_extra", None)
        if _extra is not None:
            v = _extra.get(key, DEFAULT)
            if v is not DEFAULT:
                return v

        try:
            return BasicStorage.__getattribute__(self, key)
        except AttributeError:
            pass

        m = REGEX_TABLE_DOT_FIELD.match(key)
        if m:
            key2 = m.group(2)
            try:
                return BasicStorage.__getitem__(self, m.group(1))[key2]
            except (KeyError, TypeError):
                pass
            try:
                return BasicStorage.__getitem__(self, key2)
            except KeyError:
                pass

        lg = BasicStorage.get(self, "__get_lazy_reference__", None)
        if callable(lg):
            v = self[key] = lg(key)
            return v

        raise KeyError(key)

    def __repr__(self):
        return "<Row %s>" % self.as_dict(custom_types=[LazySet])

    def __int__(self):
        return self.get("id")

    def __long__(self):
        return long(int(self))

    def __hash__(self):
        return id(self)

    __str__ = __repr__

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
        except (KeyError, AttributeError, TypeError):
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
                    d[k] = v.isoformat().replace("T", " ")[:19]
            elif not isinstance(v, tuple(SERIALIZABLE_TYPES)):
                del d[k]
        return d

    def as_xml(self, row_name="row", colnames=None, indent="  "):
        def f(row, field, indent="  "):
            if isinstance(row, Row):
                spc = indent + "  \n"
                items = [f(row[x], x, indent + "  ") for x in row]
                return "%s<%s>\n%s\n%s</%s>" % (
                    indent,
                    field,
                    spc.join(item for item in items if item),
                    indent,
                    field,
                )
            elif not callable(row):
                if re.match(REGEX_ALPHANUMERIC, field):
                    return "%s<%s>%s</%s>" % (indent, field, row, field)
                else:
                    return '%s<extra name="%s">%s</extra>' % (indent, field, row)
            else:
                return None

        return f(self, row_name, indent=indent)

    def as_json(
        self, mode="object", default=None, colnames=None, serialize=True, **kwargs
    ):
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
    return Row, (dict(s),)


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
        self._migrate = None
        self._tablename = self._dalname = tablename
        if (
            not isinstance(tablename, str)
            or hasattr(DAL, tablename)
            or not REGEX_VALID_TB_FLD.match(tablename)
            or REGEX_PYTHON_KEYWORDS.match(tablename)
        ):
            raise SyntaxError(
                "Field: invalid table name: %s, "
                'use rname for "funny" names' % tablename
            )
        self._rname = args.get("rname") or db and db._adapter.dialect.quote(tablename)
        self._raw_rname = args.get("rname") or db and tablename
        self._sequence_name = (
            args.get("sequence_name")
            or db
            and db._adapter.dialect.sequence_name(self._raw_rname)
        )
        self._trigger_name = (
            args.get("trigger_name")
            or db
            and db._adapter.dialect.trigger_name(tablename)
        )
        self._common_filter = args.get("common_filter")
        self._format = args.get("format")
        self._singular = args.get("singular", tablename.replace("_", " ").capitalize())
        self._plural = args.get("plural")
        # horrible but for backward compatibility of appadmin
        if "primarykey" in args and args["primarykey"] is not None:
            self._primarykey = args.get("primarykey")

        self._before_insert = [attempt_upload_on_insert(self)]
        self._before_update = [delete_uploaded_files, attempt_upload_on_update(self)]
        self._before_delete = [delete_uploaded_files]
        self._after_insert = []
        self._after_update = []
        self._after_delete = []

        self._virtual_fields = []
        self._virtual_methods = []

        self.add_method = MethodAdder(self)

        fieldnames = set()
        newfields = []
        _primarykey = getattr(self, "_primarykey", None)
        if _primarykey is not None:
            if not isinstance(_primarykey, list):
                raise SyntaxError(
                    "primarykey must be a list of fields from table '%s'" % tablename
                )
            if len(_primarykey) == 1:
                self._id = [
                    f
                    for f in fields
                    if isinstance(f, Field) and f.name == _primarykey[0]
                ][0]
        elif not [
            f
            for f in fields
            if (isinstance(f, Field) and f.type == "id")
            or (isinstance(f, dict) and f.get("type", None) == "id")
        ]:
            field = Field("id", "id")
            newfields.append(field)
            fieldnames.add("id")
            self._id = field

        virtual_fields = []

        def include_new(field):
            newfields.append(field)
            fieldnames.add(field.name)
            if field.type == "id":
                self._id = field

        for field in fields:
            if isinstance(field, (FieldVirtual, FieldMethod)):
                virtual_fields.append(field)
            elif isinstance(field, Field) and field.name not in fieldnames:
                if field.db is not None:
                    field = copy.copy(field)
                include_new(field)
            elif isinstance(field, (list, tuple)):
                for other in field:
                    include_new(other)
            elif isinstance(field, Table):
                table = field
                for field in table:
                    if field.name not in fieldnames and field.type != "id":
                        t2 = not table._actual and self._tablename
                        include_new(field.clone(point_self_references_to=t2))
            elif isinstance(field, dict) and field["fieldname"] not in fieldnames:
                include_new(Field(**field))
            elif not isinstance(field, (Field, Table)):
                raise SyntaxError(
                    "define_table argument is not a Field, Table of list: %s" % field
                )
        fields = newfields
        self._fields = SQLCallableList()
        self.virtualfields = []

        if db and db._adapter.uploads_in_blob is True:
            uploadfields = [f.name for f in fields if f.type == "blob"]
            for field in fields:
                fn = field.uploadfield
                if (
                    isinstance(field, Field)
                    and field.type == "upload"
                    and fn is True
                    and not field.uploadfs
                ):
                    fn = field.uploadfield = "%s_blob" % field.name
                if (
                    isinstance(fn, str)
                    and fn not in uploadfields
                    and not field.uploadfs
                ):
                    fields.append(
                        Field(fn, "blob", default="", writable=False, readable=False)
                    )

        fieldnames_set = set()
        reserved = dir(Table) + ["fields"]
        if db and db._check_reserved:
            check_reserved_keyword = db.check_reserved_keyword
        else:

            def check_reserved_keyword(field_name):
                if field_name in reserved:
                    raise SyntaxError("field name %s not allowed" % field_name)

        for field in fields:
            field_name = field.name
            check_reserved_keyword(field_name)
            if db and db._ignore_field_case:
                fname_item = field_name.lower()
            else:
                fname_item = field_name
            if fname_item in fieldnames_set:
                raise SyntaxError(
                    "duplicate field %s in table %s" % (field_name, tablename)
                )
            else:
                fieldnames_set.add(fname_item)

            self.fields.append(field_name)
            self[field_name] = field
            if field.type == "id":
                self["id"] = field
            field.bind(self)
        self.ALL = SQLALL(self)

        if _primarykey is not None:
            for k in _primarykey:
                if k not in self.fields:
                    raise SyntaxError(
                        "primarykey must be a list of fields from table '%s "
                        % tablename
                    )
                else:
                    self[k].notnull = True
        for field in virtual_fields:
            self[field.name] = field

    @property
    def fields(self):
        return self._fields

    def _structure(self):
        keys = [
            "name",
            "type",
            "writable",
            "listable",
            "searchable",
            "regex",
            "options",
            "default",
            "label",
            "unique",
            "notnull",
            "required",
        ]

        def noncallable(obj):
            return obj if not callable(obj) else None

        return [
            {key: noncallable(getattr(field, key)) for key in keys}
            for field in self
            if field.readable and not field.type == "password"
        ]

    @cachedprop
    def _upload_fieldnames(self):
        return set(field.name for field in self if field.type == "upload")

    def update(self, *args, **kwargs):
        raise RuntimeError("Syntax Not Supported")

    def _enable_record_versioning(
        self,
        archive_db=None,
        archive_name="%(tablename)s_archive",
        is_active="is_active",
        current_record="current_record",
        current_record_label=None,
        migrate=None,
        redefine=None,
    ):
        db = self._db
        archive_db = archive_db or db
        archive_name = archive_name % dict(tablename=self._dalname)
        if archive_name in archive_db.tables():
            return  # do not try define the archive if already exists
        fieldnames = self.fields()
        same_db = archive_db is db
        field_type = self if same_db else "bigint"
        clones = []
        for field in self:
            nfk = same_db or not field.type.startswith("reference")
            clones.append(
                field.clone(unique=False, type=field.type if nfk else "bigint")
            )

        d = dict(format=self._format)
        if migrate:
            d["migrate"] = migrate
        elif isinstance(self._migrate, basestring):
            d["migrate"] = self._migrate + "_archive"
        elif self._migrate:
            d["migrate"] = self._migrate
        if redefine:
            d["redefine"] = redefine
        archive_db.define_table(
            archive_name,
            Field(current_record, field_type, label=current_record_label),
            *clones,
            **d
        )

        self._before_update.append(
            lambda qset, fs, db=archive_db, an=archive_name, cn=current_record: archive_record(
                qset, fs, db[an], cn
            )
        )
        if is_active and is_active in fieldnames:
            self._before_delete.append(lambda qset: qset.update(is_active=False))
            newquery = lambda query, t=self, name=self._tablename: reduce(
                AND,
                [
                    tab.is_active == True
                    for tab in db._adapter.tables(query).values()
                    if tab._raw_rname == self._raw_rname
                ],
            )
            query = self._common_filter
            if query:
                self._common_filter = lambda q: reduce(AND, [query(q), newquery(q)])
            else:
                self._common_filter = newquery

    def _validate(self, **vars):
        errors = Row()
        for key, value in iteritems(vars):
            value, error = getattr(self, key).validate(value, vars.get("id"))
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
                field_type.startswith("reference ")
                or field_type.startswith("list:reference ")
            ):

                is_list = field_type[:15] == "list:reference "
                if is_list:
                    ref = field_type[15:].strip()
                else:
                    ref = field_type[10:].strip()

                if not ref:
                    SyntaxError("Table: reference to nothing: %s" % ref)
                if "." in ref:
                    rtablename, throw_it, rfieldname = ref.partition(".")
                else:
                    rtablename, rfieldname = ref, None
                if rtablename not in db:
                    pr[rtablename] = pr.get(rtablename, []) + [field]
                    continue
                rtable = db[rtablename]
                if rfieldname:
                    if not hasattr(rtable, "_primarykey"):
                        raise SyntaxError(
                            "keyed tables can only reference other keyed tables (for now)"
                        )
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
                if referee.type.startswith("list:reference "):
                    self._referenced_by_list.append(referee)
                else:
                    self._referenced_by.append(referee)

    def _filter_fields(self, record, id=False):
        return dict(
            [
                (k, v)
                for (k, v) in iteritems(record)
                if k in self.fields and (getattr(self, k).type != "id" or id)
            ]
        )

    def _build_query(self, key):
        """ for keyed table only """
        query = None
        for k, v in iteritems(key):
            if k in self._primarykey:
                if query:
                    query = query & (getattr(self, k) == v)
                else:
                    query = getattr(self, k) == v
            else:
                raise SyntaxError(
                    "Field %s is not part of the primary key of %s"
                    % (k, self._tablename)
                )
        return query

    def __getitem__(self, key):
        if str(key).isdigit() or (Key is not None and isinstance(key, Key)):
            # non negative key or gae
            return (
                self._db(self._id == str(key))
                .select(limitby=(0, 1), orderby_on_limitby=False)
                .first()
            )
        elif isinstance(key, dict):
            # keyed table
            query = self._build_query(key)
            return (
                self._db(query).select(limitby=(0, 1), orderby_on_limitby=False).first()
            )
        elif key is not None:
            try:
                return getattr(self, key)
            except:
                raise KeyError(key)

    def __call__(self, key=DEFAULT, **kwargs):
        for_update = kwargs.get("_for_update", False)
        if "_for_update" in kwargs:
            del kwargs["_for_update"]

        orderby = kwargs.get("_orderby", None)
        if "_orderby" in kwargs:
            del kwargs["_orderby"]

        if key is not DEFAULT:
            if isinstance(key, Query):
                record = (
                    self._db(key)
                    .select(
                        limitby=(0, 1),
                        for_update=for_update,
                        orderby=orderby,
                        orderby_on_limitby=False,
                    )
                    .first()
                )
            elif not str(key).isdigit():
                record = None
            else:
                record = (
                    self._db(self._id == key)
                    .select(
                        limitby=(0, 1),
                        for_update=for_update,
                        orderby=orderby,
                        orderby_on_limitby=False,
                    )
                    .first()
                )
            if record:
                for k, v in iteritems(kwargs):
                    if record[k] != v:
                        return None
            return record
        elif kwargs:
            query = reduce(
                lambda a, b: a & b,
                [getattr(self, k) == v for k, v in iteritems(kwargs)],
            )
            return (
                self._db(query)
                .select(
                    limitby=(0, 1),
                    for_update=for_update,
                    orderby=orderby,
                    orderby_on_limitby=False,
                )
                .first()
            )
        else:
            return None

    def __setitem__(self, key, value):
        if key is None:
            # table[None] = value (shortcut for insert)
            self.insert(**self._filter_fields(value))
        elif str(key).isdigit():
            # table[non negative key] = value (shortcut for update)
            if not self._db(self._id == key).update(**self._filter_fields(value)):
                raise SyntaxError("No such record: %s" % key)
        elif isinstance(key, dict):
            # keyed table
            if not isinstance(value, dict):
                raise SyntaxError("value must be a dictionary: %s" % value)
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
                    "key must have all fields from primary key: %s" % self._primarykey
                )
        else:
            if isinstance(value, FieldVirtual):
                value.bind(self, str(key))
                self._virtual_fields.append(value)
            elif isinstance(value, FieldMethod):
                value.bind(self, str(key))
                self._virtual_methods.append(value)
            self.__dict__[str(key)] = value

    def __setattr__(self, key, value):
        if key[:1] != "_" and key in self:
            raise SyntaxError("Object exists and cannot be redefined: %s" % key)
        self[key] = value

    def __delitem__(self, key):
        if isinstance(key, dict):
            query = self._build_query(key)
            if not self._db(query).delete():
                raise SyntaxError("No such record: %s" % key)
        elif not str(key).isdigit() or not self._db(self._id == key).delete():
            raise SyntaxError("No such record: %s" % key)

    def __iter__(self):
        for fieldname in self.fields:
            yield getattr(self, fieldname)

    def __repr__(self):
        return "<Table %s (%s)>" % (self._tablename, ", ".join(self.fields()))

    def __str__(self):
        if self._tablename == self._dalname:
            return self._tablename
        return self._db._adapter.dialect._as(self._dalname, self._tablename)

    @property
    @deprecated("sqlsafe", "sql_shortref", "Table")
    def sqlsafe(self):
        return self.sql_shortref

    @property
    @deprecated("sqlsafe_alias", "sql_fullref", "Table")
    def sqlsafe_alias(self):
        return self.sql_fullref

    @property
    def sql_shortref(self):
        if self._tablename == self._dalname:
            return self._rname
        return self._db._adapter.sqlsafe_table(self._tablename)

    @property
    def sql_fullref(self):
        if self._tablename == self._dalname:
            return self._rname
        return self._db._adapter.sqlsafe_table(self._tablename, self._rname)

    def query_name(self, *args, **kwargs):
        return (self.sql_fullref,)

    def _drop(self, mode=""):
        return self._db._adapter.dialect.drop_table(self, mode)

    def drop(self, mode=""):
        return self._db._adapter.drop_table(self, mode)

    def _filter_fields_for_operation(self, fields):
        new_fields = {}  # format: new_fields[name] = (field, value)
        input_fieldnames = set(fields)
        table_fieldnames = set(self.fields)
        empty_fieldnames = OrderedDict((name, name) for name in self.fields)
        for name in list(input_fieldnames & table_fieldnames):
            field = getattr(self, name)
            value = field.filter_in(fields[name]) if field.filter_in else fields[name]
            new_fields[name] = (field, value)
            del empty_fieldnames[name]
        return list(empty_fieldnames), new_fields

    def _compute_fields_for_operation(self, fields, to_compute):
        row = OpRow(self)
        for name, tup in iteritems(fields):
            field, value = tup
            if isinstance(
                value,
                (
                    types.LambdaType,
                    types.FunctionType,
                    types.MethodType,
                    types.BuiltinFunctionType,
                    types.BuiltinMethodType,
                ),
            ):
                value = value()
            row.set_value(name, value, field)
        for name, field in to_compute:
            try:
                row.set_value(name, field.compute(row), field)
            except (KeyError, AttributeError):
                # error silently unless field is required!
                if field.required and name not in fields:
                    raise RuntimeError("unable to compute required field: %s" % name)
        return row

    def _fields_and_values_for_insert(self, fields):
        empty_fieldnames, new_fields = self._filter_fields_for_operation(fields)
        to_compute = []
        for name in empty_fieldnames:
            field = getattr(self, name)
            if field.compute:
                to_compute.append((name, field))
            elif field.default is not None:
                new_fields[name] = (field, field.default)
            elif field.required:
                raise RuntimeError("Table: missing required field: %s" % name)
        return self._compute_fields_for_operation(new_fields, to_compute)

    def _fields_and_values_for_update(self, fields):
        empty_fieldnames, new_fields = self._filter_fields_for_operation(fields)
        to_compute = []
        for name in empty_fieldnames:
            field = getattr(self, name)
            if field.compute:
                to_compute.append((name, field))
            if field.update is not None:
                new_fields[name] = (field, field.update)
        return self._compute_fields_for_operation(new_fields, to_compute)

    def _insert(self, **fields):
        row = self._fields_and_values_for_insert(fields)
        return self._db._adapter._insert(self, row.op_values())

    def insert(self, **fields):
        row = self._fields_and_values_for_insert(fields)
        if any(f(row) for f in self._before_insert):
            return 0
        ret = self._db._adapter.insert(self, row.op_values())
        if ret and self._after_insert:
            for f in self._after_insert:
                f(row, ret)
        return ret

    def _validate_fields(self, fields, defattr="default", id=None):
        response = Row()
        response.id, response.errors, new_fields = None, Row(), Row()
        for field in self:
            # we validate even if not passed in case it is required
            error = default = None
            if not field.required and not field.compute:
                default = getattr(field, defattr)
                if callable(default):
                    default = default()
            if not field.compute:
                value = fields.get(field.name, default)
                value, error = field.validate(value, id)
            if error:
                response.errors[field.name] = "%s" % error
            elif field.name in fields:
                # only write if the field was passed and no error
                new_fields[field.name] = value
        return response, new_fields

    def validate_and_insert(self, **fields):
        response, new_fields = self._validate_fields(fields, "default")
        if not response.errors:
            response.id = self.insert(**new_fields)
        return response

    def validate_and_update(self, _key, **fields):
        record = self(**_key) if isinstance(_key, dict) else self(_key)
        response, new_fields = self._validate_fields(fields, "update", record.id)
        #: do the update
        if not response.errors and record:
            if "_id" in self:
                myset = self._db(self._id == record[self._id.name])
            else:
                query = None
                for key, value in iteritems(_key):
                    if query is None:
                        query = getattr(self, key) == value
                    else:
                        query = query & (getattr(self, key) == value)
                myset = self._db(query)
            response.updated = myset.update(**new_fields)
        if record:
            response.id = record.id
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
        if _key is DEFAULT or _key == "":
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
            if hasattr(self, "_primarykey"):
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
        data = [self._fields_and_values_for_insert(item) for item in items]
        if any(f(el) for el in data for f in self._before_insert):
            return 0
        ret = self._db._adapter.bulk_insert(self, [el.op_values() for el in data])
        ret and [
            [f(el, ret[k]) for k, el in enumerate(data)] for f in self._after_insert
        ]
        return ret

    def _truncate(self, mode=""):
        return self._db._adapter.dialect.truncate(self, mode)

    def truncate(self, mode=""):
        return self._db._adapter.truncate(self, mode)

    def import_from_csv_file(
        self,
        csvfile,
        id_map=None,
        null="<NULL>",
        unique="uuid",
        id_offset=None,  # id_offset used only when id_map is None
        transform=None,
        validate=False,
        encoding="utf-8",
        **kwargs
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
        This assumes that there is a field of type id that is integer and in
        incrementing order.
        Will keep the id numbers in restored table.
        """
        if validate:
            inserting = self.validate_and_insert
        else:
            inserting = self.insert

        delimiter = kwargs.get("delimiter", ",")
        quotechar = kwargs.get("quotechar", '"')
        quoting = kwargs.get("quoting", csv.QUOTE_MINIMAL)
        restore = kwargs.get("restore", False)
        if restore:
            self._db[self].truncate()

        reader = csv_reader(
            csvfile,
            delimiter=delimiter,
            encoding=encoding,
            quotechar=quotechar,
            quoting=quoting,
        )
        colnames = None
        if isinstance(id_map, dict):
            if self._tablename not in id_map:
                id_map[self._tablename] = {}
            id_map_self = id_map[self._tablename]

        def fix(field, value, id_map, id_offset):
            list_reference_s = "list:reference"
            if value == null:
                value = None
            elif field.type == "blob":
                value = base64.b64decode(value)
            elif field.type == "double" or field.type == "float":
                if not value.strip():
                    value = None
                else:
                    value = float(value)
            elif field.type in ("integer", "bigint"):
                if not value.strip():
                    value = None
                else:
                    value = long(value)
            elif field.type.startswith("list:string"):
                value = bar_decode_string(value)
            elif field.type.startswith(list_reference_s):
                ref_table = field.type[len(list_reference_s) :].strip()
                if id_map is not None:
                    value = [
                        id_map[ref_table][long(v)] for v in bar_decode_string(value)
                    ]
                else:
                    value = [v for v in bar_decode_string(value)]
            elif field.type.startswith("list:"):
                value = bar_decode_integer(value)
            elif id_map and field.type.startswith("reference"):
                try:
                    value = id_map[field.type[9:].strip()][long(value)]
                except KeyError:
                    pass
            elif id_offset and field.type.startswith("reference"):
                try:
                    value = id_offset[field.type[9:].strip()] + long(value)
                except KeyError:
                    pass
            return value

        def is_id(colname):
            if colname in self:
                return getattr(self, colname).type == "id"
            else:
                return False

        first = True
        unique_idx = None
        for lineno, line in enumerate(reader):
            if not line:
                return
            if not colnames:
                # assume this is the first line of the input, contains colnames
                colnames = [x.split(".", 1)[-1] for x in line]

                cols, cid = {}, None
                for i, colname in enumerate(colnames):
                    if is_id(colname):
                        cid = colname
                    elif colname in self.fields:
                        cols[colname] = getattr(self, colname)
                    if colname == unique:
                        unique_idx = i
            elif len(line) == len(colnames):
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
                            if field.type != "id":
                                ditems[fieldname] = value
                            else:
                                csv_id = long(value)
                        except ValueError:
                            raise RuntimeError("Unable to parse line:%s" % (lineno + 1))
                if not (id_map or csv_id is None or id_offset is None or unique_idx):
                    curr_id = inserting(**ditems)
                    if first:
                        first = False
                        # First curr_id is bigger than csv_id,
                        # then we are not restoring but
                        # extending db table with csv db table
                        id_offset[self._tablename] = (
                            (curr_id - csv_id) if curr_id > csv_id else 0
                        )
                    # create new id until we get the same as old_id+offset
                    while curr_id < csv_id + id_offset[self._tablename]:
                        self._db(getattr(self, cid) == curr_id).delete()
                        curr_id = inserting(**ditems)
                # Validation. Check for duplicate of 'unique' &,
                # if present, update instead of insert.
                elif not unique_idx:
                    new_id = inserting(**ditems)
                else:
                    unique_value = line[unique_idx]
                    query = getattr(self, unique) == unique_value
                    record = self._db(query).select().first()
                    if record:
                        record.update_record(**ditems)
                        new_id = record[self._id.name]
                    else:
                        new_id = inserting(**ditems)
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
            plural=self._plural,
        )

        for field in self:
            if (field.readable or field.writable) or (not sanitize):
                table_as_dict["fields"].append(
                    field.as_dict(flat=flat, sanitize=sanitize)
                )
        return table_as_dict

    def with_alias(self, alias):
        try:
            if self._db[alias]._rname == self._rname:
                return self._db[alias]
        except AttributeError:  # we never used this alias
            pass
        other = copy.copy(self)
        other["ALL"] = SQLALL(other)
        other["_tablename"] = alias
        for fieldname in other.fields:
            tmp = getattr(self, fieldname).clone()
            tmp.bind(other)
            other[fieldname] = tmp
        if "id" in self and "id" not in other.fields:
            other["id"] = other[self.id.name]
        other._id = other[self._id.name]
        self._db[alias] = other
        return other

    def on(self, query):
        return Expression(self._db, self._db._adapter.dialect.on, self, query)

    def create_index(self, name, *fields, **kwargs):
        return self._db._adapter.create_index(self, name, *fields, **kwargs)

    def drop_index(self, name):
        return self._db._adapter.drop_index(self, name)


class Select(BasicStorage):
    def __init__(self, db, query, fields, attributes):
        self._db = db
        self._tablename = None  # alias will be stored here
        self._rname = self._raw_rname = self._dalname = None
        self._common_filter = None
        self._query = query
        # if false, the subquery will never reference tables from parent scope
        self._correlated = attributes.pop("correlated", True)
        self._attributes = attributes
        self._qfields = list(fields)
        self._fields = SQLCallableList()
        self._virtual_fields = []
        self._virtual_methods = []
        self.virtualfields = []
        self._sql_cache = None
        self._colnames_cache = None
        fieldcheck = set()

        for item in fields:
            if isinstance(item, Field):
                checkname = item.name
                field = item.clone()
            elif isinstance(item, Expression):
                if item.op != item._dialect._as:
                    continue
                checkname = item.second
                field = Field(item.second, type=item.type)
            else:
                raise SyntaxError("Invalid field in Select")
            if db and db._ignore_field_case:
                checkname = checkname.lower()
            if checkname in fieldcheck:
                raise SyntaxError("duplicate field %s in select query" % field.name)
            fieldcheck.add(checkname)
            field.bind(self)
            self.fields.append(field.name)
            self[field.name] = field
        self.ALL = SQLALL(self)

    @property
    def fields(self):
        return self._fields

    def update(self, *args, **kwargs):
        raise RuntimeError("update() method not supported")

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        self.__dict__[str(key)] = value

    def __call__(self):
        adapter = self._db._adapter
        colnames, sql = self._compile()
        cache = self._attributes.get("cache", None)
        if cache and self._attributes.get("cacheable", False):
            return adapter._cached_select(
                cache, sql, self._fields, self._attributes, colnames
            )
        return adapter._select_aux(sql, self._qfields, self._attributes, colnames)

    def __setattr__(self, key, value):
        if key[:1] != "_" and key in self:
            raise SyntaxError("Object exists and cannot be redefined: %s" % key)
        self[key] = value

    def __iter__(self):
        for fieldname in self.fields:
            yield self[fieldname]

    def __repr__(self):
        return "<Select (%s)>" % ", ".join(map(str, self._qfields))

    def __str__(self):
        return self._compile(with_alias=(self._tablename is not None))[1]

    def with_alias(self, alias):
        other = copy.copy(self)
        other["ALL"] = SQLALL(other)
        other["_tablename"] = alias
        for fieldname in other.fields:
            tmp = self[fieldname].clone()
            tmp.bind(other)
            other[fieldname] = tmp
        return other

    def on(self, query):
        if not self._tablename:
            raise SyntaxError("Subselect must be aliased for use in a JOIN")
        return Expression(self._db, self._db._adapter.dialect.on, self, query)

    def _compile(self, outer_scoped=[], with_alias=False):
        if not self._correlated:
            outer_scoped = []
        if outer_scoped or not self._sql_cache:
            adapter = self._db._adapter
            attributes = self._attributes.copy()
            attributes["outer_scoped"] = outer_scoped
            colnames, sql = adapter._select_wcols(
                self._query, self._qfields, **attributes
            )
            # Do not cache when the query may depend on external tables
            if not outer_scoped:
                self._colnames_cache, self._sql_cache = colnames, sql
        else:
            colnames, sql = self._colnames_cache, self._sql_cache
        if with_alias and self._tablename is not None:
            sql = "(%s)" % sql[:-1]
            sql = self._db._adapter.dialect.alias(sql, self._tablename)
        return colnames, sql

    def query_name(self, outer_scoped=[]):
        if self._tablename is None:
            raise SyntaxError("Subselect must be aliased for use in a JOIN")
        colnames, sql = self._compile(outer_scoped, True)
        # This method should also return list of placeholder values
        # in the future
        return (sql,)

    @property
    def sql_shortref(self):
        if self._tablename is None:
            raise SyntaxError("Subselect must be aliased for use in a JOIN")
        return self._db._adapter.dialect.quote(self._tablename)

    def _filter_fields(self, record, id=False):
        return dict(
            [
                (k, v)
                for (k, v) in iteritems(record)
                if k in self.fields and (self[k].type != "id" or id)
            ]
        )


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

    def __init__(self, db, op, first=None, second=None, type=None, **optional_args):
        self.db = db
        self.op = op
        self.first = first
        self.second = second
        self._table = getattr(first, "_table", None)
        if not type and first and hasattr(first, "type"):
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
        return Expression(self.db, self._dialect.aggregate, self, "SUM", self.type)

    def max(self):
        return Expression(self.db, self._dialect.aggregate, self, "MAX", self.type)

    def min(self):
        return Expression(self.db, self._dialect.aggregate, self, "MIN", self.type)

    def len(self):
        return Expression(self.db, self._dialect.length, self, None, "integer")

    def avg(self):
        return Expression(self.db, self._dialect.aggregate, self, "AVG", self.type)

    def abs(self):
        return Expression(self.db, self._dialect.aggregate, self, "ABS", self.type)

    def cast(self, cast_as, **kwargs):
        return Expression(
            self.db,
            self._dialect.cast,
            self,
            self._dialect.types[cast_as] % kwargs,
            cast_as,
        )

    def lower(self):
        return Expression(self.db, self._dialect.lower, self, None, self.type)

    def upper(self):
        return Expression(self.db, self._dialect.upper, self, None, self.type)

    def replace(self, a, b):
        return Expression(self.db, self._dialect.replace, self, (a, b), self.type)

    def year(self):
        return Expression(self.db, self._dialect.extract, self, "year", "integer")

    def month(self):
        return Expression(self.db, self._dialect.extract, self, "month", "integer")

    def day(self):
        return Expression(self.db, self._dialect.extract, self, "day", "integer")

    def hour(self):
        return Expression(self.db, self._dialect.extract, self, "hour", "integer")

    def minutes(self):
        return Expression(self.db, self._dialect.extract, self, "minute", "integer")

    def coalesce(self, *others):
        return Expression(self.db, self._dialect.coalesce, self, others, self.type)

    def coalesce_zero(self):
        return Expression(self.db, self._dialect.coalesce_zero, self, None, self.type)

    def seconds(self):
        return Expression(self.db, self._dialect.extract, self, "second", "integer")

    def epoch(self):
        return Expression(self.db, self._dialect.epoch, self, None, "integer")

    def __getitem__(self, i):
        if isinstance(i, slice):
            start = i.start or 0
            stop = i.stop

            db = self.db
            if start < 0:
                pos0 = "(%s - %d)" % (self.len(), abs(start) - 1)
            else:
                pos0 = start + 1

            maxint = sys.maxint if PY2 else sys.maxsize
            if stop is None or stop == maxint:
                length = self.len()
            elif stop < 0:
                length = "(%s - %d - %s)" % (self.len(), abs(stop) - 1, pos0)
            else:
                length = "(%s - %s)" % (stop + 1, pos0)

            return Expression(
                db, self._dialect.substring, self, (pos0, length), self.type
            )
        else:
            return self[i : i + 1]

    def __str__(self):
        return str(self.db._adapter.expand(self, self.type))

    def __or__(self, other):  # for use in sortby
        return Expression(self.db, self._dialect.comma, self, other, self.type)

    def __invert__(self):
        if hasattr(self, "_op") and self.op == self._dialect.invert:
            return self.first
        return Expression(self.db, self._dialect.invert, self, type=self.type)

    def __add__(self, other):
        return Expression(self.db, self._dialect.add, self, other, self.type)

    def __sub__(self, other):
        if self.type in ("integer", "bigint"):
            result_type = "integer"
        elif self.type in ["date", "time", "datetime", "double", "float"]:
            result_type = "double"
        elif self.type.startswith("decimal("):
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
        elif not isinstance(value, (Select, basestring)):
            value = set(value)
            if kwattr.get("null") and None in value:
                value.remove(None)
                return (self == None) | Query(
                    self.db, self._dialect.belongs, self, value
                )
        return Query(self.db, self._dialect.belongs, self, value)

    def startswith(self, value):
        if self.type not in ("string", "text", "json", "jsonb", "upload"):
            raise SyntaxError("startswith used with incompatible field type")
        return Query(self.db, self._dialect.startswith, self, value)

    def endswith(self, value):
        if self.type not in ("string", "text", "json", "jsonb", "upload"):
            raise SyntaxError("endswith used with incompatible field type")
        return Query(self.db, self._dialect.endswith, self, value)

    def contains(self, value, all=False, case_sensitive=False):
        """
        For GAE contains() is always case sensitive
        """
        if isinstance(value, (list, tuple)):
            subqueries = [
                self.contains(str(v), case_sensitive=case_sensitive)
                for v in value
                if str(v)
            ]
            if not subqueries:
                return self.contains("")
            else:
                return reduce(all and AND or OR, subqueries)
        if self.type not in (
            "string",
            "text",
            "json",
            "jsonb",
            "upload",
        ) and not self.type.startswith("list:"):
            raise SyntaxError("contains used with incompatible field type")
        return Query(
            self.db, self._dialect.contains, self, value, case_sensitive=case_sensitive
        )

    def with_alias(self, alias):
        return Expression(self.db, self._dialect._as, self, alias, self.type)

    # GIS expressions

    def st_asgeojson(self, precision=15, options=0):
        return Expression(
            self.db,
            self._dialect.st_asgeojson,
            self,
            dict(precision=precision, options=options),
            "string",
        )

    def st_astext(self):
        return Expression(self.db, self._dialect.st_astext, self, type="string")

    def st_aswkb(self):
        return Expression(self.db, self._dialect.st_aswkb, self, type="string")

    def st_x(self):
        return Expression(self.db, self._dialect.st_x, self, type="string")

    def st_y(self):
        return Expression(self.db, self._dialect.st_y, self, type="string")

    def st_distance(self, other):
        return Expression(self.db, self._dialect.st_distance, self, other, "double")

    def st_simplify(self, value):
        return Expression(self.db, self._dialect.st_simplify, self, value, self.type)

    def st_simplifypreservetopology(self, value):
        return Expression(
            self.db, self._dialect.st_simplifypreservetopology, self, value, self.type
        )

    def st_transform(self, value):
        return Expression(self.db, self._dialect.st_transform, self, value, self.type)

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
        return Query(self.db, self._dialect.st_dwithin, self, (value, distance))

    # JSON Expressions

    def json_key(self, key):
        """
        Get the json in key which you can use to build queries or as one of the
        fields you want to get in a select.

        Example:
            Usage::

                To use as one of the fields you want to get in a select

                >>> tj = db.define_table('tj', Field('testjson', 'json'))
                >>> tj.insert(testjson={u'a': {u'a1': 2, u'a0': 1}, u'b': 3, u'c': {u'c0': {u'c01': [2, 4]}}})
                >>> row = db(db.tj).select(db.tj.testjson.json_key('a').with_alias('a')).first()
                >>> row.a
                {u'a1': 2, u'a0': 1}

                Using it as part of building a query

                >>> row = db(tj.testjson.json_key('a').json_key_value('a0') == 1).select().first()
                >>> row
                <Row {'testjson': {u'a': {u'a1': 2, u'a0': 1}, u'c': {u'c0': {u'c01': [2, 4]}}, u'b': 3}, 'id': 1L}>

        """
        return Expression(self.db, self._dialect.json_key, self, key)

    def json_key_value(self, key):
        """
        Get the value int or text in key

        Example:
            Usage::

                To use as one of the fields you want to get in a select

                >>> tj = db.define_table('tj', Field('testjson', 'json'))
                >>> tj.insert(testjson={u'a': {u'a1': 2, u'a0': 1}, u'b': 3, u'c': {u'c0': {u'c01': [2, 4]}}})
                >>> row = db(db.tj).select(db.tj.testjson.json_key_value('b').with_alias('b')).first()
                >>> row.b
                '3'

                Using it as part of building a query

                >>> row = db(db.tj.testjson.json_key('a').json_key_value('a0') == 1).select().first()
                >>> row
                <Row {'testjson': {u'a': {u'a1': 2, u'a0': 1}, u'c': {u'c0': {u'c01': [2, 4]}}, u'b': 3}, 'id': 1L}>

        """
        return Expression(self.db, self._dialect.json_key_value, self, key)

    def json_path(self, path):
        """
        Get the json in path which you can use for more queries

        Example:
            Usage::

                >>> tj = db.define_table('tj', Field('testjson', 'json'))
                >>> tj.insert(testjson={u'a': {u'a1': 2, u'a0': 1}, u'b': 3, u'c': {u'c0': {u'c01': [2, 4]}}})
                >>> row = db(db.tj.id > 0).select(db.tj.testjson.json_path('{c, c0, c01, 0}').with_alias('firstc01')).first()
                >>> row.firstc01
                2
        """
        return Expression(self.db, self._dialect.json_path, self, path)

    def json_path_value(self, path):
        """
        Get the value in path which you can use for more queries

        Example:
            Usage::

                >>> tj = db.define_table('tj', Field('testjson', 'json'))
                >>> tj.insert(testjson={u'a': {u'a1': 2, u'a0': 1}, u'b': 3, u'c': {u'c0': {u'c01': [2, 4]}}})
                >>> db(db.tj.testjson.json_path_value('{a, a1}') == 2).select().first()
                <Row {'testjson': {u'a': {u'a1': 2, u'a0': 1}, u'c': {u'c0': {u'c01': [2, 4]}}, u'b': 3}, 'id': 1L}>
        """
        return Expression(self.db, self._dialect.json_path_value, self, path)

    # JSON Queries

    def json_contains(self, jsonvalue):
        """
        Containment operator, jsonvalue parameter must be a json string
        e.g. '{"country": "Peru"}'

        Example:
            Usage::

                >>> tj = db.define_table('tj', Field('testjson', 'json'))
                >>> tj.insert(testjson={u'a': {u'a1': 2, u'a0': 1}, u'b': 3, u'c': {u'c0': {u'c01': [2, 4]}}})
                >>> db(db.tj.testjson.json_contains('{"c": {"c0":{"c01": [2]}}}')).select().first()
                <Row {'testjson': {u'a': {u'a1': 2, u'a0': 1}, u'c': {u'c0': {u'c01': [2, 4]}}, u'b': 3}, 'id': 1L}>
        """
        return Query(self.db, self._dialect.json_contains, self, jsonvalue)


class FieldVirtual(object):
    def __init__(
        self,
        name,
        f=None,
        ftype="string",
        label=None,
        table_name=None,
        readable=True,
        listable=True,
    ):
        # for backward compatibility
        (self.name, self.f) = (name, f) if f else ("unknown", name)
        self.type = ftype
        self.label = label or self.name.replace("_", " ").title()
        self.represent = lambda v, r=None: v
        self.formatter = IDENTITY
        self.comment = None
        self.readable = readable
        self.listable = listable
        self.searchable = False
        self.writable = False
        self.requires = None
        self.widget = None
        self.tablename = table_name
        self.filter_out = None

    def bind(self, table, name):
        if self.tablename is not None:
            raise ValueError("FieldVirtual %s is already bound to a table" % self)
        if self.name == "unknown":  # for backward compatibility
            self.name = name
        elif name != self.name:
            raise ValueError("Cannot rename FieldVirtual %s to %s" % (self.name, name))
        self.tablename = table._tablename

    def __str__(self):
        return "%s.%s" % (self.tablename, self.name)


class FieldMethod(object):
    def __init__(self, name, f=None, handler=None):
        # for backward compatibility
        (self.name, self.f) = (name, f) if f else ("unknown", name)
        self.handler = handler or VirtualCommand

    def bind(self, table, name):
        if self.name == "unknown":  # for backward compatibility
            self.name = name
        elif name != self.name:
            raise ValueError("Cannot rename FieldMethod %s to %s" % (self.name, name))


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
                regex=None, options=None,
                uploadfield=True, widget=None, label=None, comment=None,
                uploadfield=True, # True means store on disk,
                                  # 'a_field_name' means store in this field in db
                                  # False means file content will be discarded.
                writable=True, readable=True, searchable=True, listable=True,
                update=None, authorize=None,
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

    def __init__(
        self,
        fieldname,
        type="string",
        length=None,
        default=DEFAULT,
        required=False,
        requires=DEFAULT,
        ondelete="CASCADE",
        notnull=False,
        unique=False,
        uploadfield=True,
        widget=None,
        label=None,
        comment=None,
        writable=True,
        readable=True,
        searchable=True,
        listable=True,
        regex=None,
        options=None,
        update=None,
        authorize=None,
        autodelete=False,
        represent=None,
        uploadfolder=None,
        uploadseparate=False,
        uploadfs=None,
        compute=None,
        custom_store=None,
        custom_retrieve=None,
        custom_retrieve_file_properties=None,
        custom_delete=None,
        filter_in=None,
        filter_out=None,
        custom_qualifier=None,
        map_none=None,
        rname=None,
        **others
    ):
        self._db = self.db = None  # both for backward compatibility
        self.table = self._table = None
        self.op = None
        self.first = None
        self.second = None
        if PY2 and isinstance(fieldname, unicode):
            try:
                fieldname = str(fieldname)
            except UnicodeEncodeError:
                raise SyntaxError("Field: invalid unicode field name")
        self.name = fieldname = cleanup(fieldname)
        if (
            not isinstance(fieldname, str)
            or hasattr(Table, fieldname)
            or not REGEX_VALID_TB_FLD.match(fieldname)
            or REGEX_PYTHON_KEYWORDS.match(fieldname)
        ):
            raise SyntaxError(
                "Field: invalid field name: %s, "
                'use rname for "funny" names' % fieldname
            )

        if not isinstance(type, (Table, Field)):
            self.type = type
        else:
            self.type = "reference %s" % type

        self.length = (
            length if length is not None else DEFAULTLENGTH.get(self.type, 512)
        )
        self.default = default if default is not DEFAULT else (update or None)
        self.required = required  # is this field required
        self.ondelete = ondelete.upper()  # this is for reference fields only
        self.notnull = notnull
        self.unique = unique
        # split to deal with decimal(,)
        self.regex = regex
        if not regex and isinstance(self.type, str):
            self.regex = DEFAULT_REGEX.get(self.type.split("(")[0])
        self.options = options
        self.uploadfield = uploadfield
        self.uploadfolder = uploadfolder
        self.uploadseparate = uploadseparate
        self.uploadfs = uploadfs
        self.widget = widget
        self.comment = comment
        self.writable = writable
        self.readable = readable
        self.searchable = searchable
        self.listable = listable
        self.update = update
        self.authorize = authorize
        self.autodelete = autodelete
        self.represent = (
            list_represent
            if represent is None and type in ("list:integer", "list:string")
            else represent
        )
        self.compute = compute
        self.isattachment = True
        self.custom_store = custom_store
        self.custom_retrieve = custom_retrieve
        self.custom_retrieve_file_properties = custom_retrieve_file_properties
        self.custom_delete = custom_delete
        self.filter_in = filter_in
        self.filter_out = filter_out
        self.custom_qualifier = custom_qualifier
        self.label = label if label is not None else fieldname.replace("_", " ").title()
        self.requires = requires if requires is not None else []
        self.map_none = map_none
        self._rname = self._raw_rname = rname
        stype = self.type
        if isinstance(self.type, SQLCustomType):
            stype = self.type.type
        self._itype = REGEX_TYPE.match(stype).group(0) if stype else None
        for key in others:
            setattr(self, key, others[key])

    def bind(self, table):
        if self._table is not None:
            raise ValueError("Field %s is already bound to a table" % self.longname)
        self.db = self._db = table._db
        self.table = self._table = table
        self.tablename = self._tablename = table._tablename
        if self._db and self._rname is None:
            self._rname = self._db._adapter.sqlsafe_field(self.name)
            self._raw_rname = self.name

    def set_attributes(self, *args, **attributes):
        self.__dict__.update(*args, **attributes)
        return self

    def clone(self, point_self_references_to=False, **args):
        field = copy.copy(self)
        if point_self_references_to and self.type == "reference %s" % self._tablename:
            field.type = "reference %s" % point_self_references_to
        field.__dict__.update(args)
        field.db = field._db = None
        field.table = field._table = None
        field.tablename = field._tablename = None
        if self._db and self._rname == self._db._adapter.sqlsafe_field(self.name):
            # Reset the name because it may need to be requoted by bind()
            field._rname = field._raw_rname = None
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
        filename = os.path.basename(filename.replace("/", os.sep).replace("\\", os.sep))
        m = re.search(REGEX_UPLOAD_EXTENSION, filename)
        extension = m and m.group(1) or "txt"
        uuid_key = self._db.uuid().replace("-", "")[-16:]
        encoded_filename = to_native(base64.b16encode(to_bytes(filename)).lower())
        newfilename = "%s.%s.%s.%s" % (
            self._tablename,
            self.name,
            uuid_key,
            encoded_filename,
        )
        newfilename = (
            newfilename[: (self.length - 1 - len(extension))] + "." + extension
        )
        self_uploadfield = self.uploadfield
        if isinstance(self_uploadfield, Field):
            blob_uploadfield_name = self_uploadfield.uploadfield
            keys = {
                self_uploadfield.name: newfilename,
                blob_uploadfield_name: file.read(),
            }
            self_uploadfield.table.insert(**keys)
        elif self_uploadfield is True:
            if self.uploadfs:
                dest_file = self.uploadfs.open(text_type(newfilename), "wb")
            else:
                if path:
                    pass
                elif self.uploadfolder:
                    path = self.uploadfolder
                elif self.db._adapter.folder:
                    path = pjoin(self.db._adapter.folder, "..", "uploads")
                else:
                    raise RuntimeError(
                        "you must specify a Field(..., uploadfolder=...)"
                    )
                if self.uploadseparate:
                    if self.uploadfs:
                        raise RuntimeError("not supported")
                    path = pjoin(
                        path, "%s.%s" % (self._tablename, self.name), uuid_key[:2]
                    )
                if not exists(path):
                    os.makedirs(path)
                pathfilename = pjoin(path, newfilename)
                dest_file = open(pathfilename, "wb")
            try:
                shutil.copyfileobj(file, dest_file)
            except IOError:
                raise IOError(
                    'Unable to store file "%s" because invalid permissions, '
                    "readonly file system, or filename too long" % pathfilename
                )
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
        filename = file_properties["filename"]
        if isinstance(self_uploadfield, str):  # ## if file is in DB
            stream = BytesIO(to_bytes(row[self_uploadfield] or ""))
        elif isinstance(self_uploadfield, Field):
            blob_uploadfield_name = self_uploadfield.uploadfield
            query = self_uploadfield == name
            data = self_uploadfield.table(query)[blob_uploadfield_name]
            stream = BytesIO(to_bytes(data))
        elif self.uploadfs:
            # ## if file is on pyfilesystem
            stream = self.uploadfs.open(text_type(name), "rb")
        else:
            # ## if file is on regular filesystem
            # this is intentionally a string with filename and not a stream
            # this propagates and allows stream_file_or_304_or_206 to be called
            fullname = pjoin(file_properties["path"], name)
            if nameonly:
                return (filename, fullname)
            stream = open(fullname, "rb")
        return (filename, stream)

    def retrieve_file_properties(self, name, path=None):
        m = re.match(REGEX_UPLOAD_PATTERN, name)
        if not m or not self.isattachment:
            raise TypeError("Can't retrieve %s file properties" % name)
        self_uploadfield = self.uploadfield
        if self.custom_retrieve_file_properties:
            return self.custom_retrieve_file_properties(name, path)
        if m.group("name"):
            try:
                filename = base64.b16decode(m.group("name"), True).decode("utf-8")
                filename = re.sub(REGEX_UPLOAD_CLEANUP, "_", filename)
            except (TypeError, AttributeError, binascii.Error):
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
                path = pjoin(self.db._adapter.folder, "..", "uploads")
        if self.uploadseparate:
            t = m.group("table")
            f = m.group("field")
            u = m.group("uuidkey")
            path = pjoin(path, "%s.%s" % (t, f), u[:2])
        return dict(path=path, filename=filename)

    def formatter(self, value):
        if value is None:
            return self.map_none
        requires = self.requires
        if not requires or requires is DEFAULT:
            return value
        if not isinstance(requires, (list, tuple)):
            requires = [requires]
        elif isinstance(requires, tuple):
            requires = list(requires)
        else:
            requires = copy.copy(requires)
        requires.reverse()
        for item in requires:
            if hasattr(item, "formatter"):
                value = item.formatter(value)
        return value

    def validate(self, value, record_id=None):
        requires = self.requires
        if not requires or requires is DEFAULT:
            return ((value if value != self.map_none else None), None)
        if not isinstance(requires, (list, tuple)):
            requires = [requires]
        for validator in requires:
            # notice that some validator may have different behavior
            # depending on the record id, for example
            # IS_NOT_IN_DB should exclude the current record_id from check
            (value, error) = validator(value, record_id)
            if error:
                return (value, error)
        return ((value if value != self.map_none else None), None)

    def count(self, distinct=None):
        return Expression(self.db, self._dialect.count, self, distinct, "integer")

    def as_dict(self, flat=False, sanitize=True):
        attrs = (
            "name",
            "authorize",
            "represent",
            "ondelete",
            "custom_store",
            "autodelete",
            "custom_retrieve",
            "filter_out",
            "uploadseparate",
            "widget",
            "uploadfs",
            "update",
            "custom_delete",
            "uploadfield",
            "uploadfolder",
            "custom_qualifier",
            "unique",
            "writable",
            "compute",
            "map_none",
            "default",
            "type",
            "required",
            "readable",
            "requires",
            "comment",
            "label",
            "length",
            "notnull",
            "custom_retrieve_file_properties",
            "filter_in",
        )
        serializable = (int, long, basestring, float, tuple, bool, type(None))

        def flatten(obj):
            if isinstance(obj, dict):
                return dict((flatten(k), flatten(v)) for k, v in obj.items())
            elif isinstance(obj, (tuple, list, set)):
                return [flatten(v) for v in obj]
            elif isinstance(obj, serializable):
                return obj
            elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
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
        if self._table:
            return "%s.%s" % (self.tablename, self.name)
        return "<no table>.%s" % self.name

    def __hash__(self):
        return id(self)

    @property
    def sqlsafe(self):
        if self._table is None:
            raise SyntaxError("Field %s is not bound to any table" % self.name)
        return self._table.sql_shortref + "." + self._rname

    @property
    @deprecated("sqlsafe_name", "_rname", "Field")
    def sqlsafe_name(self):
        return self._rname

    @property
    def longname(self):
        if self._table is None:
            raise SyntaxError("Field %s is not bound to any table" % self.name)
        return self._table._tablename + "." + self.name


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

    def __init__(
        self,
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
        return "<Query %s>" % str(self)

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

        SERIALIZABLE_TYPES = (
            tuple,
            dict,
            set,
            list,
            int,
            long,
            float,
            basestring,
            type(None),
            bool,
        )

        def loop(d):
            newd = dict()
            for k, v in d.items():
                if k in ("first", "second"):
                    if isinstance(v, self.__class__):
                        newd[k] = loop(v.__dict__)
                    elif isinstance(v, Field):
                        newd[k] = {"tablename": v._tablename, "fieldname": v.name}
                    elif isinstance(v, Expression):
                        newd[k] = loop(v.__dict__)
                    elif isinstance(v, SERIALIZABLE_TYPES):
                        newd[k] = v
                    elif isinstance(
                        v, (datetime.date, datetime.time, datetime.datetime)
                    ):
                        newd[k] = text_type(v)
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

        if (
            ignore_common_filters is not None
            and use_common_filters(query) == ignore_common_filters
        ):
            query = copy.copy(query)
            query.ignore_common_filters = ignore_common_filters
        self.query = query

    def __repr__(self):
        return "<Set %s>" % str(self.query)

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
            return Set(
                self.db, self.query & query, ignore_common_filters=ignore_common_filters
            )
        else:
            return Set(self.db, query, ignore_common_filters=ignore_common_filters)

    def _count(self, distinct=None):
        return self.db._adapter._count(self.query, distinct)

    def _select(self, *fields, **attributes):
        adapter = self.db._adapter
        tablenames = adapter.tables(
            self.query,
            attributes.get("join", None),
            attributes.get("left", None),
            attributes.get("orderby", None),
            attributes.get("groupby", None),
        )
        fields = adapter.expand_all(fields, tablenames)
        return adapter._select(self.query, fields, attributes)

    def _delete(self):
        db = self.db
        table = db._adapter.get_table(self.query)
        return db._adapter._delete(table, self.query)

    def _update(self, **update_fields):
        db = self.db
        table = db._adapter.get_table(self.query)
        row = table._fields_and_values_for_update(update_fields)
        return db._adapter._update(table, self.query, row.op_values())

    def as_dict(self, flat=False, sanitize=True):
        if flat:
            uid = dbname = uri = None
            codec = self.db._db_codec
            if not sanitize:
                uri, dbname, uid = (self.db._dbname, str(self.db), self.db._db_uid)
            d = {"query": self.query.as_dict(flat=flat)}
            d["db"] = {"uid": uid, "codec": codec, "name": dbname, "uri": uri}
            return d
        else:
            return self.__dict__

    def parse(self, dquery):
        """Experimental: Turn a dictionary into a Query object"""
        self.dquery = dquery
        return self.build(self.dquery)

    def build(self, d):
        """Experimental: see .parse()"""
        op, first, second = (d["op"], d["first"], d.get("second", None))
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
            built = ~self.build(first)  # pylint: disable=invalid-unary-operand-type
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
            elif op in (
                "LOWER",
                "UPPER",
                "EPOCH",
                "PRIMARY_KEY",
                "COALESCE_ZERO",
                "RAW",
                "INVERT",
            ):
                built = Expression(self.db, opm, left)
            elif op in (
                "COUNT",
                "EXTRACT",
                "AGGREGATE",
                "SUBSTRING",
                "REGEXP",
                "LIKE",
                "ILIKE",
                "STARTSWITH",
                "ENDSWITH",
                "ADD",
                "SUB",
                "MUL",
                "DIV",
                "MOD",
                "AS",
                "ON",
                "COMMA",
                "NOT_NULL",
                "COALESCE",
                "CONTAINS",
                "BELONGS",
            ):
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
                cache_model = cache["model"]
                time_expire = cache["expiration"]
                key = cache.get("key")
                if not key:
                    key = db._uri + "/" + sql
                    key = hashlib_md5(key).hexdigest()
            else:
                cache_model, time_expire = cache
                key = db._uri + "/" + sql
                key = hashlib_md5(key).hexdigest()
            return cache_model(
                key,
                lambda self=self, distinct=distinct: db._adapter.count(
                    self.query, distinct
                ),
                time_expire,
            )
        return db._adapter.count(self.query, distinct)

    def select(self, *fields, **attributes):
        adapter = self.db._adapter
        tablenames = adapter.tables(
            self.query,
            attributes.get("join", None),
            attributes.get("left", None),
            attributes.get("orderby", None),
            attributes.get("groupby", None),
        )
        fields = adapter.expand_all(fields, tablenames)
        return adapter.select(self.query, fields, attributes)

    def iterselect(self, *fields, **attributes):
        adapter = self.db._adapter
        tablenames = adapter.tables(
            self.query,
            attributes.get("join", None),
            attributes.get("left", None),
            attributes.get("orderby", None),
            attributes.get("groupby", None),
        )
        fields = adapter.expand_all(fields, tablenames)
        return adapter.iterselect(self.query, fields, attributes)

    def nested_select(self, *fields, **attributes):
        adapter = self.db._adapter
        tablenames = adapter.tables(
            self.query,
            attributes.get("join", None),
            attributes.get("left", None),
            attributes.get("orderby", None),
            attributes.get("groupby", None),
        )
        fields = adapter.expand_all(fields, tablenames)
        return adapter.nested_select(self.query, fields, attributes)

    def delete(self):
        db = self.db
        table = db._adapter.get_table(self.query)
        if any(f(self) for f in table._before_delete):
            return 0
        ret = db._adapter.delete(table, self.query)
        ret and [f(self) for f in table._after_delete]
        return ret

    def delete_naive(self):
        """
        Same as delete but does not call table._before_delete and _after_delete
        """
        db = self.db
        table = db._adapter.get_table(self.query)
        ret = db._adapter.delete(table, self.query)
        return ret

    def update(self, **update_fields):
        db = self.db
        table = db._adapter.get_table(self.query)
        row = table._fields_and_values_for_update(update_fields)
        if not row._values:
            raise ValueError("No fields to update")
        if any(f(self, row) for f in table._before_update):
            return 0
        ret = db._adapter.update(table, self.query, row.op_values())
        ret and [f(self, row) for f in table._after_update]
        return ret

    def update_naive(self, **update_fields):
        """
        Same as update but does not call table._before_update and _after_update
        """
        table = self.db._adapter.get_table(self.query)
        row = table._fields_and_values_for_update(update_fields)
        if not row._values:
            raise ValueError("No fields to update")
        ret = self.db._adapter.update(table, self.query, row.op_values())
        return ret

    def validate_and_update(self, **update_fields):
        table = self.db._adapter.get_table(self.query)
        response = Row()
        response.errors = Row()
        new_fields = copy.copy(update_fields)
        for key, value in iteritems(update_fields):
            value, error = table[key].validate(value, update_fields.get("id"))
            if error:
                response.errors[key] = "%s" % error
            else:
                new_fields[key] = value
        if response.errors:
            response.updated = None
        else:
            row = table._fields_and_values_for_update(new_fields)
            if not row._values:
                raise ValueError("No fields to update")
            if any(f(self, row) for f in table._before_update):
                ret = 0
            else:
                ret = self.db._adapter.update(table, self.query, row.op_values())
                ret and [f(self, row) for f in table._after_update]
            response.updated = ret
        return response


class LazyReferenceGetter(object):
    def __init__(self, table, id):
        self.db = table._db
        self.tablename = table._tablename
        self.id = id

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
        self.db, self.tablename, self.fieldname, self.id = (
            field.db,
            field._tablename,
            field.name,
            id,
        )

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

    def delete_naive(self):
        return self._getset().delete_naive()

    def update(self, **update_fields):
        return self._getset().update(**update_fields)

    def update_naive(self, **update_fields):
        return self._getset().update_naive(**update_fields)

    def validate_and_update(self, **update_fields):
        return self._getset().validate_and_update(**update_fields)


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

    def as_trees(self, parent_name="parent_id", children_name="children", render=False):
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
        rows = (
            list(self.render(fields=None if render is True else render))
            if render
            else self
        )
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

    def as_list(
        self,
        compact=True,
        storage_to_dict=True,
        datetime_to_str=False,
        custom_types=None,
    ):
        """
        Returns the data as a list or dictionary.

        Args:
            storage_to_dict: when True returns a dict, otherwise a list
            datetime_to_str: convert datetime fields as strings
        """
        (oc, self.compact) = (self.compact, compact)
        if storage_to_dict:
            items = [item.as_dict(datetime_to_str, custom_types) for item in self]
        else:
            items = [item for item in self]
        self.compact = oc
        return items

    def as_dict(
        self,
        key="id",
        compact=True,
        storage_to_dict=True,
        datetime_to_str=False,
        custom_types=None,
    ):
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
                key = lambda r: next(key_generator)

        rows = self.as_list(compact, storage_to_dict, datetime_to_str, custom_types)
        if isinstance(key, str) and key.count(".") == 1:
            (table, field) = key.split(".")
            return dict([(r[table][field], r) for r in rows])
        elif isinstance(key, str):
            return dict([(r[key], r) for r in rows])
        else:
            return dict([(key(r), r) for r in rows])

    def xml(self, strict=False, row_name="row", rows_name="rows"):
        """
        Serializes the table using sqlhtml.SQLTABLE (if present)
        """
        if not strict and not self.db.has_representer("rows_xml"):
            strict = True

        if strict:
            return "<%s>\n%s\n</%s>" % (
                rows_name,
                "\n".join(
                    row.as_xml(row_name=row_name, colnames=self.colnames)
                    for row in self
                ),
                rows_name,
            )

        rv = self.db.represent("rows_xml", self)
        if hasattr(rv, "xml") and callable(getattr(rv, "xml")):
            return rv.xml()
        return rv

    def as_xml(self, row_name="row", rows_name="rows"):
        return self.xml(strict=True, row_name=row_name, rows_name=rows_name)

    def as_json(self, mode="object", default=None):
        """
        Serializes the rows to a JSON list or object with objects
        mode='object' is not implemented (should return a nested
        object structure)
        """
        items = [
            record.as_json(
                mode=mode, default=default, serialize=False, colnames=self.colnames
            )
            for record in self
        ]

        return serializers.json(items)

    @property
    def colnames_fields(self):
        """
        Returns the list of fields matching colnames, possibly
        including virtual fields (i.e. Field.Virtual and
        Field.Method instances).
        Use this property instead of plain fields attribute
        whenever you have an entry in colnames which references
        a virtual field, and you still need a correspondance
        between column names and fields.

        NOTE that references to the virtual fields must have been
             **forced** in some way within colnames, because in the general
             case it is not possible to have them as a result of a select.
        """
        colnames = self.colnames
        # instances of Field or Expression only are allowed in fields
        plain_fields = self.fields
        if len(colnames) > len(plain_fields):
            # correspondance between colnames and fields is broken,
            # search for missing virtual fields
            fields = []
            fi = 0
            for col in colnames:
                m = re.match(REGEX_TABLE_DOT_FIELD_OPTIONAL_QUOTES, col)
                if m:
                    t, f = m.groups()
                    table = self.db[t]
                    field = table[f]
                    if field in table._virtual_fields + table._virtual_methods:
                        fields.append(field)
                        continue
                fields.append(plain_fields[fi])
                fi += 1
            assert len(colnames) == len(fields)
            return fields
        return plain_fields

    def export_to_csv_file(self, ofile, null="<NULL>", *args, **kwargs):
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
        delimiter = kwargs.get("delimiter", ",")
        quotechar = kwargs.get("quotechar", '"')
        quoting = kwargs.get("quoting", csv.QUOTE_MINIMAL)
        represent = kwargs.get("represent", False)
        writer = csv.writer(
            ofile, delimiter=delimiter, quotechar=quotechar, quoting=quoting
        )

        def unquote_colnames(colnames):
            unq_colnames = []
            for col in colnames:
                m = self.db._adapter.REGEX_TABLE_DOT_FIELD.match(col)
                if not m:
                    unq_colnames.append(col)
                else:
                    unq_colnames.append(".".join(m.groups()))
            return unq_colnames

        colnames = kwargs.get("colnames", self.colnames)
        write_colnames = kwargs.get("write_colnames", True)
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
                return value.encode("utf8")
            elif isinstance(value, Reference):
                return long(value)
            elif hasattr(value, "isoformat"):
                return value.isoformat()[:19].replace("T", " ")
            elif isinstance(value, (list, tuple)):  # for type='list:..'
                return bar_encode(value)
            return value

        repr_cache = {}
        fieldlist = self.colnames_fields
        fieldmap = dict(zip(self.colnames, fieldlist))
        for record in self:
            row = []
            for col in colnames:
                field = fieldmap[col]
                if isinstance(field, (Field, FieldVirtual)):
                    t = field.tablename
                    f = field.name
                    if isinstance(record.get(t, None), (Row, dict)):
                        value = record[t][f]
                    else:
                        value = record[f]
                    if field.type == "blob" and value is not None:
                        value = base64.b64encode(value)
                    elif represent and field.represent:
                        if field.type.startswith("reference"):
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
                else:
                    row.append(record._extra[col])
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

    def __init__(
        self, db=None, records=[], colnames=[], compact=True, rawrows=None, fields=[]
    ):
        self.db = db
        self.records = records
        self.fields = fields
        self.colnames = colnames
        self.compact = compact
        self.response = rawrows

    def __repr__(self):
        return "<Rows (%s)>" % len(self.records)

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
                    if attribute[0] != "_":
                        method = getattr(virtualfields, attribute)
                        if hasattr(method, "__lazy__"):
                            box[attribute] = VirtualCommand(method, row)
                        elif type(method) == types.MethodType:
                            if not updated:
                                virtualfields.__dict__.update(row)
                                updated = True
                            box[attribute] = method()
        return self

    def __add__(self, other):
        if self.colnames != other.colnames:
            raise Exception("Cannot & incompatible Rows objects")
        records = self.records + other.records
        return self.__class__(
            self.db,
            records,
            self.colnames,
            fields=self.fields,
            compact=self.compact or other.compact,
        )

    def __and__(self, other):
        if self.colnames != other.colnames:
            raise Exception("Cannot & incompatible Rows objects")
        records = []
        other_records = list(other.records)
        for record in self.records:
            if record in other_records:
                records.append(record)
                other_records.remove(record)
        return self.__class__(
            self.db,
            records,
            self.colnames,
            fields=self.fields,
            compact=self.compact or other.compact,
        )

    def __or__(self, other):
        if self.colnames != other.colnames:
            raise Exception("Cannot | incompatible Rows objects")
        records = [record for record in other.records if record not in self.records]
        records = self.records + records
        return self.__class__(
            self.db,
            records,
            self.colnames,
            fields=self.fields,
            compact=self.compact or other.compact,
        )

    def __len__(self):
        return len(self.records)

    def __getslice__(self, a, b):
        return self.__class__(
            self.db,
            self.records[a:b],
            self.colnames,
            compact=self.compact,
            fields=self.fields,
        )

    def __getitem__(self, i):
        if isinstance(i, slice):
            return self.__getslice__(i.start, i.stop)
        row = self.records[i]
        keys = list(row.keys())
        if self.compact and len(keys) == 1 and keys[0] != "_extra":
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
            return self.records == other.records
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
                self.db, [], self.colnames, compact=self.compact, fields=self.fields
            )
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
            self.db, records, self.colnames, compact=self.compact, fields=self.fields
        )

    def exclude(self, f):
        """
        Removes elements from the calling Rows object, filtered by the function
        `f`, and returns a new Rows object containing the removed elements
        """
        if not self.records:
            return self.__class__(
                self.db, [], self.colnames, compact=self.compact, fields=self.fields
            )
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
            self.db, removed, self.colnames, compact=self.compact, fields=self.fields
        )

    def sort(self, f, reverse=False):
        """
        Returns a list of sorted elements (not sorted in place)
        """
        rows = self.__class__(
            self.db, [], self.colnames, compact=self.compact, fields=self.fields
        )
        # When compact=True, iterating over self modifies each record,
        # so when sorting self, it is necessary to return a sorted
        # version of self.records rather than the sorted self directly.
        rows.records = [
            r
            for (r, s) in sorted(
                zip(self.records, self), key=lambda r: f(r[1]), reverse=reverse
            )
        ]
        return rows

    def join(self, field, name=None, constraint=None, fields=[], orderby=None):
        if len(self) == 0:
            return self
        mode = "referencing" if field.type == "id" else "referenced"
        func = lambda ids: field.belongs(ids)
        db, ids, maps = self.db, [], {}
        if not fields:
            fields = [f for f in field._table if f.readable]
        if mode == "referencing":
            # try all refernced field names
            names = (
                [name]
                if name
                else list(
                    set(
                        f.name for f in field._table._referenced_by if f.name in self[0]
                    )
                )
            )
            # get all the ids
            ids = [row.get(name) for row in self for name in names]
            # filter out the invalid ids
            ids = filter(lambda id: str(id).isdigit(), ids)
            # build the query
            query = func(ids)
            if constraint:
                query = query & constraint
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
        if mode == "referenced":
            if not name:
                name = field._tablename
            # build the query
            query = func([row.id for row in self])
            if constraint:
                query = query & constraint
            name = name or field._tablename
            tmp = not field.name in [f.name for f in fields]
            if tmp:
                fields.append(field)
            other = db(query).select(*fields, orderby=orderby, cacheable=True)
            for row in other:
                id = row[field]
                if not id in maps:
                    maps[id] = []
                if tmp:
                    try:
                        del row[field.name]
                    except:
                        del row[field.tablename][field.name]
                        if not row[field.tablename] and len(row.keys()) == 2:
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
        if "one_result" in args:
            one_result = args["one_result"]

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
                struct = build_fields_struct(row, fields, num + 1, groups[value])

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
        if not self.db.has_representer("rows_render"):
            raise RuntimeError(
                "Rows.render() needs a `rows_render` \
                               representer in DAL instance"
            )
        row = copy.deepcopy(self.records[i])
        keys = list(row.keys())
        if not fields:
            fields = [f for f in self.fields if isinstance(f, Field) and f.represent]
        for field in fields:
            row[field._tablename][field.name] = self.db.represent(
                "rows_render",
                field,
                row[field._tablename][field.name],
                row[field._tablename],
            )

        if self.compact and len(keys) == 1 and keys[0] != "_extra":
            return row[keys[0]]
        return row

    def __getstate__(self):
        ret = self.__dict__.copy()
        ret.pop("fields", None)
        return ret

    def _restore_fields(self, fields):
        if not hasattr(self, "fields"):
            self.fields = fields
        return self


@implements_iterator
class IterRows(BasicRows):
    def __init__(self, db, sql, fields, colnames, blob_decode, cacheable):
        self.db = db
        self.fields = fields
        self.colnames = colnames
        self.blob_decode = blob_decode
        self.cacheable = cacheable
        (
            self.fields_virtual,
            self.fields_lazy,
            self.tmps,
        ) = self.db._adapter._parse_expand_colnames(fields)
        self.sql = sql
        self._head = None
        self.last_item = None
        self.last_item_id = None
        self.compact = True
        self.sql = sql
        # get a new cursor in order to be able to iterate without undesired behavior
        # not completely safe but better than before
        self.cursor = self.db._adapter.cursor
        self.db._adapter.execute(sql)
        # give the adapter a new cursor since this one is busy
        self.db._adapter.reset_cursor()

    def __next__(self):
        db_row = self.cursor.fetchone()
        if db_row is None:
            raise StopIteration
        row = self.db._adapter._parse(
            db_row,
            self.tmps,
            self.fields,
            self.colnames,
            self.blob_decode,
            self.cacheable,
            self.fields_virtual,
            self.fields_lazy,
        )
        if self.compact:
            # The following is to translate
            # <Row {'t0': {'id': 1L, 'name': 'web2py'}}>
            # in
            # <Row {'id': 1L, 'name': 'web2py'}>
            # normally accomplished by Rows.__get_item__
            keys = list(row.keys())
            if len(keys) == 1 and keys[0] != "_extra":
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
            return
        return

    def first(self):
        if self._head is None:
            try:
                self._head = next(self)
            except StopIteration:
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
                n_to_drop -= self.last_item_id + 1
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
