import copy
import datetime
import locale
import os
import pickle
import sys
from ._compat import PY2, string_types, pjoin, iteritems, to_bytes, exists
from ._load import portalocker
from .helpers.classes import SQLCustomType, DatabaseStoredFile


class Migrator(object):
    def __init__(self, adapter):
        self.adapter = adapter

    @property
    def db(self):
        return self.adapter.db

    @property
    def dialect(self):
        return self.adapter.dialect

    @property
    def dbengine(self):
        return self.adapter.dbengine

    def create_table(self, table, migrate=True, fake_migrate=False, polymodel=None):
        db = table._db
        table._migrate = migrate
        fields = []
        # PostGIS geo fields are added after the table has been created
        postcreation_fields = []
        sql_fields = {}
        sql_fields_aux = {}
        TFK = {}
        tablename = table._tablename
        types = self.adapter.types
        for sortable, field in enumerate(table, start=1):
            if self.db._ignore_field_case:
                field_name = field.name.lower()
            else:
                field_name = field.name
            field_type = field.type
            if isinstance(field_type, SQLCustomType):
                ftype = field_type.native or field_type.type
            elif field_type.startswith(("reference", "big-reference")):
                if field_type.startswith("reference"):
                    referenced = field_type[10:].strip()
                    type_name = "reference"
                else:
                    referenced = field_type[14:].strip()
                    type_name = "big-reference"

                if referenced == ".":
                    referenced = tablename
                constraint_name = self.dialect.constraint_name(
                    table._raw_rname, field._raw_rname
                )
                # if not '.' in referenced \
                #         and referenced != tablename \
                #         and hasattr(table,'_primarykey'):
                #     ftype = types['integer']
                # else:
                try:
                    rtable = db[referenced]
                    rfield = rtable._id
                    rfieldname = rfield.name
                    rtablename = referenced
                except (KeyError, ValueError, AttributeError) as e:
                    self.db.logger.debug("Error: %s" % e)
                    try:
                        rtablename, rfieldname = referenced.split(".")
                        rtable = db[rtablename]
                        rfield = rtable[rfieldname]
                    except Exception as e:
                        self.db.logger.debug("Error: %s" % e)
                        raise KeyError(
                            "Cannot resolve reference %s in %s definition"
                            % (referenced, table._tablename)
                        )

                # must be PK reference or unique
                if (
                    not rfield.type.startswith(("reference", "big-reference"))
                    and getattr(rtable, "_primarykey", None)
                    and rfieldname in rtable._primarykey
                    or rfield.unique
                ):
                    ftype = types[rfield.type[:9]] % dict(length=rfield.length)
                    # multicolumn primary key reference?
                    if not rfield.unique and len(rtable._primarykey) > 1:
                        # then it has to be a table level FK
                        if rtablename not in TFK:
                            TFK[rtablename] = {}
                        TFK[rtablename][rfieldname] = field_name
                    else:
                        fk = rtable._rname + " (" + rfield._rname + ")"
                        ftype = ftype + types["reference FK"] % dict(
                            # should be quoted
                            constraint_name=constraint_name,
                            foreign_key=fk,
                            table_name=table._rname,
                            field_name=field._rname,
                            on_delete_action=field.ondelete,
                        )
                else:
                    # make a guess here for circular references
                    if referenced in db:
                        id_fieldname = db[referenced]._id._rname
                    elif referenced == tablename:
                        id_fieldname = table._id._rname
                    else:  # make a guess
                        id_fieldname = self.dialect.quote("id")
                    # gotcha: the referenced table must be defined before
                    # the referencing one to be able to create the table
                    # Also if it's not recommended, we can still support
                    # references to tablenames without rname to make
                    # migrations and model relationship work also if tables
                    # are not defined in order
                    if referenced == tablename:
                        real_referenced = db[referenced]._rname
                    else:
                        real_referenced = (
                            referenced in db and db[referenced]._rname or referenced
                        )
                    rfield = db[referenced]._id
                    ftype_info = dict(
                        index_name=self.dialect.quote(field._raw_rname + "__idx"),
                        field_name=field._rname,
                        constraint_name=self.dialect.quote(constraint_name),
                        foreign_key="%s (%s)" % (real_referenced, rfield._rname),
                        on_delete_action=field.ondelete,
                    )
                    ftype_info["null"] = (
                        " NOT NULL" if field.notnull else self.dialect.allow_null
                    )
                    ftype_info["unique"] = " UNIQUE" if field.unique else ""
                    ftype = types[type_name] % ftype_info
            elif field_type.startswith("list:reference"):
                ftype = types[field_type[:14]]
            elif field_type.startswith("decimal"):
                precision, scale = map(int, field_type[8:-1].split(","))
                ftype = types[field_type[:7]] % dict(precision=precision, scale=scale)
            elif field_type.startswith("geo"):
                if not hasattr(self.adapter, "srid"):
                    raise RuntimeError("Adapter does not support geometry")
                srid = self.adapter.srid
                geotype, parms = field_type[:-1].split("(")
                if geotype not in types:
                    raise SyntaxError(
                        "Field: unknown field type: %s for %s"
                        % (field_type, field_name)
                    )
                ftype = types[geotype]
                if self.dbengine == "postgres" and geotype == "geometry":
                    if self.db._ignore_field_case is True:
                        field_name = field_name.lower()
                    # parameters: schema, srid, dimension
                    dimension = 2  # GIS.dimension ???
                    parms = parms.split(",")
                    if len(parms) == 3:
                        schema, srid, dimension = parms
                    elif len(parms) == 2:
                        schema, srid = parms
                    else:
                        schema = parms[0]
                    ftype = (
                        "SELECT AddGeometryColumn ('%%(schema)s', '%%(tablename)s', '%%(fieldname)s', %%(srid)s, '%s', %%(dimension)s);"
                        % types[geotype]
                    )
                    ftype = ftype % dict(
                        schema=schema,
                        tablename=table._raw_rname,
                        fieldname=field._raw_rname,
                        srid=srid,
                        dimension=dimension,
                    )
                    postcreation_fields.append(ftype)
            elif field_type not in types:
                raise SyntaxError(
                    "Field: unknown field type: %s for %s" % (field_type, field_name)
                )
            else:
                ftype = types[field_type] % {"length": field.length}

            if not field_type.startswith(("id", "reference", "big-reference")):
                if field.notnull:
                    ftype += " NOT NULL"
                else:
                    ftype += self.dialect.allow_null
                if field.unique:
                    ftype += " UNIQUE"
                if field.custom_qualifier:
                    ftype += " %s" % field.custom_qualifier

            # add to list of fields
            sql_fields[field_name] = dict(
                length=field.length,
                unique=field.unique,
                notnull=field.notnull,
                sortable=sortable,
                type=str(field_type),
                sql=ftype,
                rname=field._rname,
                raw_rname=field._raw_rname,
            )

            if field.notnull and field.default is not None:
                # Caveat: sql_fields and sql_fields_aux
                # differ for default values.
                # sql_fields is used to trigger migrations and sql_fields_aux
                # is used for create tables.
                # The reason is that we do not want to trigger
                # a migration simply because a default value changes.
                not_null = self.dialect.not_null(field.default, field_type)
                ftype = ftype.replace("NOT NULL", not_null)
            sql_fields_aux[field_name] = dict(sql=ftype)
            # Postgres - PostGIS:
            # geometry fields are added after the table has been created, not now
            if not (self.dbengine == "postgres" and field_type.startswith("geom")):
                fields.append("%s %s" % (field._rname, ftype))
        other = ";"

        # backend-specific extensions to fields
        if self.dbengine == "mysql":
            if not hasattr(table, "_primarykey"):
                fields.append("PRIMARY KEY (%s)" % (table._id._rname))
            engine = self.adapter.adapter_args.get("engine", "InnoDB")
            other = " ENGINE=%s CHARACTER SET utf8;" % engine

        fields = ",\n    ".join(fields)
        for rtablename in TFK:
            rtable = db[rtablename]
            rfields = TFK[rtablename]
            pkeys = [rtable[pk]._rname for pk in rtable._primarykey]
            fk_fields = [table[rfields[k]] for k in rtable._primarykey]
            fkeys = [f._rname for f in fk_fields]
            constraint_name = self.dialect.constraint_name(
                table._raw_rname, "_".join(f._raw_rname for f in fk_fields)
            )
            on_delete = list(set(f.ondelete for f in fk_fields))
            if len(on_delete) > 1:
                raise SyntaxError(
                    "Table %s has incompatible ON DELETE actions in multi-field foreign key."
                    % table._dalname
                )
            fields = (
                fields
                + ",\n    "
                + types["reference TFK"]
                % dict(
                    constraint_name=constraint_name,
                    table_name=table._rname,
                    field_name=", ".join(fkeys),
                    foreign_table=rtable._rname,
                    foreign_key=", ".join(pkeys),
                    on_delete_action=on_delete[0],
                )
            )

        if getattr(table, "_primarykey", None):
            query = "CREATE TABLE %s(\n    %s,\n    %s) %s" % (
                table._rname,
                fields,
                self.dialect.primary_key(
                    ", ".join([table[pk]._rname for pk in table._primarykey])
                ),
                other,
            )
        else:
            query = "CREATE TABLE %s(\n    %s\n)%s" % (table._rname, fields, other)

        uri = self.adapter.uri
        if uri.startswith("sqlite:///") or uri.startswith("spatialite:///"):
            if PY2:
                path_encoding = (
                    sys.getfilesystemencoding()
                    or locale.getdefaultlocale()[1]
                    or "utf8"
                )
                dbpath = uri[9 : uri.rfind("/")].decode("utf8").encode(path_encoding)
            else:
                dbpath = uri[9 : uri.rfind("/")]
        else:
            dbpath = self.adapter.folder

        if not migrate:
            return query
        elif uri.startswith("sqlite:memory") or uri.startswith("spatialite:memory"):
            table._dbt = None
        elif isinstance(migrate, string_types):
            table._dbt = pjoin(dbpath, migrate)
        else:
            table._dbt = pjoin(dbpath, "%s_%s.table" % (db._uri_hash, tablename))

        if not table._dbt or not self.file_exists(table._dbt):
            if table._dbt:
                self.log(
                    "timestamp: %s\n%s\n"
                    % (datetime.datetime.today().isoformat(), query),
                    table,
                )
            if not fake_migrate:
                self.adapter.create_sequence_and_triggers(query, table)
                db.commit()
                # Postgres geom fields are added now,
                # after the table has been created
                for query in postcreation_fields:
                    self.adapter.execute(query)
                    db.commit()
            if table._dbt:
                tfile = self.file_open(table._dbt, "wb")
                pickle.dump(sql_fields, tfile)
                self.file_close(tfile)
                if fake_migrate:
                    self.log("faked!\n", table)
                else:
                    self.log("success!\n", table)
        else:
            tfile = self.file_open(table._dbt, "rb")
            try:
                sql_fields_old = pickle.load(tfile)
            except EOFError:
                self.file_close(tfile)
                raise RuntimeError("File %s appears corrupted" % table._dbt)
            self.file_close(tfile)
            # add missing rnames
            for key, item in sql_fields_old.items():
                tmp = sql_fields.get(key)
                if tmp:
                    item.setdefault("rname", tmp["rname"])
                    item.setdefault("raw_rname", tmp["raw_rname"])
                else:
                    item.setdefault("rname", self.dialect.quote(key))
                    item.setdefault("raw_rname", key)
            if sql_fields != sql_fields_old:
                self.migrate_table(
                    table,
                    sql_fields,
                    sql_fields_old,
                    sql_fields_aux,
                    None,
                    fake_migrate=fake_migrate,
                )
        return query

    def _fix(self, item):
        k, v = item
        if not isinstance(v, dict):
            v = dict(type="unknown", sql=v)
        if self.db._ignore_field_case is not True:
            return k, v
        return k.lower(), v

    def migrate_table(
        self,
        table,
        sql_fields,
        sql_fields_old,
        sql_fields_aux,
        logfile,
        fake_migrate=False,
    ):
        # logfile is deprecated (moved to adapter.log method)
        db = table._db
        db._migrated.append(table._tablename)
        tablename = table._tablename
        if self.dbengine in ("firebird",):
            drop_expr = "ALTER TABLE %s DROP %s;"
        else:
            drop_expr = "ALTER TABLE %s DROP COLUMN %s;"
        field_types = dict(
            (x.lower(), table[x].type) for x in sql_fields.keys() if x in table
        )
        # make sure all field names are lower case to avoid
        # migrations because of case change
        sql_fields = dict(map(self._fix, iteritems(sql_fields)))
        sql_fields_old = dict(map(self._fix, iteritems(sql_fields_old)))
        sql_fields_aux = dict(map(self._fix, iteritems(sql_fields_aux)))
        if db._debug:
            db.logger.debug("migrating %s to %s" % (sql_fields_old, sql_fields))

        keys = list(sql_fields.keys())
        for key in sql_fields_old:
            if key not in keys:
                keys.append(key)
        new_add = self.dialect.concat_add(table._rname)

        metadata_change = False
        sql_fields_current = copy.copy(sql_fields_old)
        for key in keys:
            query = None
            if key not in sql_fields_old:
                sql_fields_current[key] = sql_fields[key]
                if self.dbengine in ("postgres",) and sql_fields[key][
                    "type"
                ].startswith("geometry"):
                    # 'sql' == ftype in sql
                    query = [sql_fields[key]["sql"]]
                else:
                    query = [
                        "ALTER TABLE %s ADD %s %s;"
                        % (
                            table._rname,
                            sql_fields[key]["rname"],
                            sql_fields_aux[key]["sql"].replace(", ", new_add),
                        )
                    ]
                metadata_change = True
            elif self.dbengine in ("sqlite", "spatialite"):
                if key in sql_fields:
                    sql_fields_current[key] = sql_fields[key]
                    # Field rname has changes, add new column
                    if (
                        sql_fields[key]["raw_rname"].lower()
                        != sql_fields_old[key]["raw_rname"].lower()
                    ):
                        tt = sql_fields_aux[key]["sql"].replace(", ", new_add)
                        query = [
                            "ALTER TABLE %s ADD %s %s;"
                            % (table._rname, sql_fields[key]["rname"], tt),
                            "UPDATE %s SET %s=%s;"
                            % (
                                table._rname,
                                sql_fields[key]["rname"],
                                sql_fields_old[key]["rname"],
                            ),
                        ]
                metadata_change = True
            elif key not in sql_fields:
                del sql_fields_current[key]
                ftype = sql_fields_old[key]["type"]
                if self.dbengine == "postgres" and ftype.startswith("geometry"):
                    geotype, parms = ftype[:-1].split("(")
                    schema = parms.split(",")[0]
                    query = [
                        "SELECT DropGeometryColumn ('%(schema)s', \
                             '%(table)s', '%(field)s');"
                        % dict(
                            schema=schema,
                            table=table._raw_rname,
                            field=sql_fields_old[key]["raw_rname"],
                        )
                    ]
                else:
                    query = [drop_expr % (table._rname, sql_fields_old[key]["rname"])]
                metadata_change = True
            # The field has a new rname, temp field is not needed
            elif (
                sql_fields[key]["raw_rname"].lower()
                != sql_fields_old[key]["raw_rname"].lower()
            ):
                sql_fields_current[key] = sql_fields[key]
                tt = sql_fields_aux[key]["sql"].replace(", ", new_add)
                query = [
                    "ALTER TABLE %s ADD %s %s;"
                    % (table._rname, sql_fields[key]["rname"], tt),
                    "UPDATE %s SET %s=%s;"
                    % (
                        table._rname,
                        sql_fields[key]["rname"],
                        sql_fields_old[key]["rname"],
                    ),
                    drop_expr % (table._rname, sql_fields_old[key]["rname"]),
                ]
                metadata_change = True
            elif (
                sql_fields[key]["sql"] != sql_fields_old[key]["sql"]
                and not isinstance(field_types.get(key), SQLCustomType)
                and not sql_fields[key]["type"].startswith("reference")
                and not sql_fields[key]["type"].startswith("double")
                and not sql_fields[key]["type"].startswith("id")
            ):
                sql_fields_current[key] = sql_fields[key]
                tt = sql_fields_aux[key]["sql"].replace(", ", new_add)
                key_tmp = self.dialect.quote(key + "__tmp")
                query = [
                    "ALTER TABLE %s ADD %s %s;" % (table._rname, key_tmp, tt),
                    "UPDATE %s SET %s=%s;"
                    % (table._rname, key_tmp, sql_fields_old[key]["rname"]),
                    drop_expr % (table._rname, sql_fields_old[key]["rname"]),
                    "ALTER TABLE %s ADD %s %s;"
                    % (table._rname, sql_fields[key]["rname"], tt),
                    "UPDATE %s SET %s=%s;"
                    % (table._rname, sql_fields[key]["rname"], key_tmp),
                    drop_expr % (table._rname, key_tmp),
                ]
                metadata_change = True
            elif sql_fields[key] != sql_fields_old[key]:
                sql_fields_current[key] = sql_fields[key]
                metadata_change = True

            if query:
                self.log(
                    "timestamp: %s\n" % datetime.datetime.today().isoformat(), table
                )
                for sub_query in query:
                    self.log(sub_query + "\n", table)
                    if fake_migrate:
                        if db._adapter.commit_on_alter_table:
                            self.save_dbt(table, sql_fields_current)
                        self.log("faked!\n", table)
                    else:
                        self.adapter.execute(sub_query)
                        # Caveat: mysql, oracle and firebird
                        # do not allow multiple alter table
                        # in one transaction so we must commit
                        # partial transactions and
                        # update table._dbt after alter table.
                        if db._adapter.commit_on_alter_table:
                            db.commit()
                            self.save_dbt(table, sql_fields_current)
                            self.log("success!\n", table)

            elif metadata_change:
                self.save_dbt(table, sql_fields_current)

        if metadata_change and not (query and db._adapter.commit_on_alter_table):
            db.commit()
            self.save_dbt(table, sql_fields_current)
            self.log("success!\n", table)

    def save_dbt(self, table, sql_fields_current):
        tfile = self.file_open(table._dbt, "wb")
        pickle.dump(sql_fields_current, tfile)
        self.file_close(tfile)

    def log(self, message, table=None):
        isabs = None
        logfilename = self.adapter.adapter_args.get("logfile", "sql.log")
        writelog = bool(logfilename)
        if writelog:
            isabs = os.path.isabs(logfilename)
        if table and table._dbt and writelog and self.adapter.folder:
            if isabs:
                table._loggername = logfilename
            else:
                table._loggername = pjoin(self.adapter.folder, logfilename)
            logfile = self.file_open(table._loggername, "ab")
            logfile.write(to_bytes(message))
            self.file_close(logfile)

    @staticmethod
    def file_open(filename, mode="rb", lock=True):
        # to be used ONLY for files that on GAE may not be on filesystem
        if lock:
            fileobj = portalocker.LockedFile(filename, mode)
        else:
            fileobj = open(filename, mode)
        return fileobj

    @staticmethod
    def file_close(fileobj):
        # to be used ONLY for files that on GAE may not be on filesystem
        if fileobj:
            fileobj.close()

    @staticmethod
    def file_delete(filename):
        os.unlink(filename)

    @staticmethod
    def file_exists(filename):
        # to be used ONLY for files that on GAE may not be on filesystem
        return exists(filename)


class InDBMigrator(Migrator):
    def file_exists(self, filename):
        return DatabaseStoredFile.exists(self.db, filename)

    def file_open(self, filename, mode="rb", lock=True):
        return DatabaseStoredFile(self.db, filename, mode)

    @staticmethod
    def file_close(fileobj):
        fileobj.close_connection()

    def file_delete(self, filename):
        query = "DELETE FROM web2py_filesystem WHERE path='%s'" % filename
        self.db.executesql(query)
        self.db.commit()
