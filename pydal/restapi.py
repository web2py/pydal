import collections
import copy
import datetime
import fnmatch
import functools
import re
import traceback

__version__ = "0.1"

__all__ = ["RestAPI", "Policy", "ALLOW_ALL_POLICY", "DENY_ALL_POLICY"]

MAX_LIMIT = 1000


class PolicyViolation(ValueError):
    pass


class InvalidFormat(ValueError):
    pass


class NotFound(ValueError):
    pass


def maybe_call(value):
    return value() if callable(value) else value


def error_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        data = {}
        try:
            data = func(*args, **kwargs)
            if not data.get("errors"):
                data["status"] = "success"
                data["code"] = 200
            else:
                data["status"] = "error"
                data["message"] = "Validation Errors"
                data["code"] = 422
        except PolicyViolation as e:
            print(traceback.format_exc())
            data["status"] = "error"
            data["message"] = str(e)
            data["code"] = 401
        except NotFound as e:
            print(traceback.format_exc())
            data["status"] = "error"
            data["message"] = str(e)
            data["code"] = 404
        except (InvalidFormat, KeyError, ValueError) as e:
            print(traceback.format_exc())
            data["status"] = "error"
            data["message"] = str(e)
            data["code"] = 400
        finally:
            data["timestamp"] = datetime.datetime.utcnow().isoformat()
            data["api_version"] = __version__
        return data

    return wrapper


class Policy(object):

    model = {
        "POST": {"authorize": False, "fields": None},
        "PUT": {"authorize": False, "fields": None},
        "DELETE": {"authorize": False},
        "GET": {
            "authorize": False,
            "fields": None,
            "query": None,
            "allowed_patterns": [],
            "denied_patterns": [],
            "limit": MAX_LIMIT,
            "allow_lookup": False,
        },
    }

    def __init__(self):
        self.info = {}

    def set(self, tablename, method='GET', **attributes):
        method = method.upper()
        if not method in self.model:
            raise InvalidFormat("Invalid policy method: %s" % method)
        invalid_keys = [key for key in attributes if key not in self.model[method]]
        if invalid_keys:
            raise InvalidFormat("Invalid keys: %s" % ",".join(invalid_keys))
        if not tablename in self.info:
            self.info[tablename] = copy.deepcopy(self.model)
        self.info[tablename][method].update(attributes)

    def get(self, tablename, method, name):
        policy = self.info.get(tablename) or self.info.get("*")
        if not policy:
            raise PolicyViolation("No policy for this object")
        return maybe_call(policy[method][name])

    def check_if_allowed(
        self, method, tablename, id=None, get_vars=None, post_vars=None, exceptions=True
    ):
        get_vars = get_vars or {}
        post_vars = post_vars or {}
        policy = self.info.get(tablename) or self.info.get("*")
        if not policy:
            if exceptions:
                raise PolicyViolation("No policy for this object")
            return False
        policy = policy.get(method.upper())
        if not policy:
            if exceptions:
                raise PolicyViolation("No policy for this method")
            return False
        authorize = policy.get("authorize")
        if authorize is False or (
            callable(authorize) and not authorize(tablename, id, get_vars, post_vars)
        ):
            if exceptions:
                raise PolicyViolation("Not authorized")
            return False
        for key in get_vars:
            if any(fnmatch.fnmatch(key, p) for p in policy["denied_patterns"]):
                if exceptions:
                    raise PolicyViolation("Pattern is not allowed")
                return False
            allowed_patterns = policy["allowed_patterns"]
            if "**" not in allowed_patterns and not any(
                fnmatch.fnmatch(key, p) for p in allowed_patterns
            ):
                if exceptions:
                    raise PolicyViolation("Pattern is not explicitely allowed")
                return False
        return True

    def check_if_lookup_allowed(self, tablename, exceptions=True):
        policy = self.info.get(tablename) or self.info.get("*")
        if not policy:
            if exceptions:
                raise PolicyViolation("No policy for this object")
            return False
        policy = policy.get("GET")
        if not policy:
            if exceptions:
                raise PolicyViolation("No policy for this method")
            return False
        if policy.get("allow_lookup"):
            return True
        return False

    def allowed_fieldnames(self, table, method="GET"):
        method = method.upper()
        policy = self.info.get(table._tablename) or self.info.get("*", {})
        policy = policy[method]
        allowed_fieldnames = policy.get("fields")
        if allowed_fieldnames is None:
            allowed_fieldnames = [
                f.name
                for f in table
                if (method == "GET" and maybe_call(f.readable))
                or (method != "GET" and maybe_call(f.writable))
            ]
        return allowed_fieldnames

    def check_fieldnames(self, table, fieldnames, method="GET"):
        allowed_fieldnames = self.allowed_fieldnames(table, method)
        invalid_fieldnames = set(fieldnames) - set(allowed_fieldnames)
        if invalid_fieldnames:
            raise InvalidFormat("Invalid fields: %s" % list(invalid_fieldnames))


DENY_ALL_POLICY = Policy()
ALLOW_ALL_POLICY = Policy()
ALLOW_ALL_POLICY.set(
    tablename="*",
    method="GET",
    authorize=True,
    allowed_patterns=["**"],
    allow_lookup=True,
)
ALLOW_ALL_POLICY.set(tablename="*", method="POST", authorize=True)
ALLOW_ALL_POLICY.set(tablename="*", method="PUT", authorize=True)
ALLOW_ALL_POLICY.set(tablename="*", method="DELETE", authorize=True)


class RestAPI(object):

    re_table_and_fields = re.compile(r"\w+([\w+(,\w+)+])?")
    re_lookups = re.compile(
        r"((\w*\!?\:)?(\w+(\[\w+(,\w+)*\])?)(\.\w+(\[\w+(,\w+)*\])?)*)"
    )
    re_no_brackets = re.compile(r"\[.*?\]")

    def __init__(self, db, policy):
        self.db = db
        self.policy = policy
        self.allow_count = 'legacy'

    @error_wrapper
    def __call__(self, method, tablename, id=None, get_vars=None, post_vars=None, allow_count='legacy'):
        method = method.upper()
        get_vars = get_vars or {}
        post_vars = post_vars or {}
        self.allow_count = allow_count
        # validate incoming request
        tname, tfieldnames = RestAPI.parse_table_and_fields(tablename)
        if not tname in self.db.tables:
            raise InvalidFormat("Invalid table name: %s" % tname)
        if self.policy:
            self.policy.check_if_allowed(method, tablename, id, get_vars, post_vars)
            if method in ["POST", "PUT"]:
                self.policy.check_fieldnames(
                    self.db[tablename], post_vars.keys(), method
                )
        # apply rules
        if method == "GET":
            if id:
                get_vars["id.eq"] = id
            return self.search(tablename, get_vars)
        elif method == "POST":
            table = self.db[tablename]
            return table.validate_and_insert(**post_vars).as_dict()
        elif method == "PUT":
            id = id or post_vars["id"]
            if not id:
                raise InvalidFormat("No item id specified")
            table = self.db[tablename]
            data = table.validate_and_update(id, **post_vars).as_dict()
            if not data.get("errors") and not data.get("updated"):
                raise NotFound("Item not found")
            return data
        elif method == "DELETE":
            id = id or post_vars["id"]
            if not id:
                raise InvalidFormat("No item id specified")
            table = self.db[tablename]
            deleted = self.db(table._id == id).delete()
            if not deleted:
                raise NotFound("Item not found")
            return {"deleted": deleted}

    def table_model(self, table, fieldnames):
        """ converts a table into its form template """
        items = []
        fields = post_fields = put_fields = table.fields
        if self.policy:
            fields = self.policy.allowed_fieldnames(table, method="GET")
            put_fields = self.policy.allowed_fieldnames(table, method="PUT")
            post_fields = self.policy.allowed_fieldnames(table, method="POST")
        for fieldname in fields:
            if fieldnames and not fieldname in fieldnames:
                continue
            field = table[fieldname]
            item = {"name": field.name, "label": field.label}
            # https://github.com/collection-json/extensions/blob/master/template-validation.md
            item["default"] = (
                field.default() if callable(field.default) else field.default
            )
            parts = field.type.split()
            item["type"] = parts[0].split("(")[0]
            if len(parts) > 1:
                item["references"] = parts[1]
            if hasattr(field, "regex"):
                item["regex"] = field.regex
            item["required"] = field.required
            item["unique"] = field.unique
            item["post_writable"] = field.name in post_fields
            item["put_writable"] = field.name in put_fields
            item["options"] = field.options
            if field.type == "id":
                item["referenced_by"] = [
                    "%s.%s" % (f._tablename, f.name)
                    for f in table._referenced_by
                    if self.policy
                    and self.policy.check_if_allowed(
                        "GET", f._tablename, exceptions=False
                    )
                ]
            items.append(item)
        return items

    @staticmethod
    def make_query(field, condition, value):
        expression = {
            "eq": lambda: field == value,
            "ne": lambda: field != value,
            "lt": lambda: field < value,
            "gt": lambda: field > value,
            "le": lambda: field <= value,
            "ge": lambda: field >= value,
            "startswith": lambda: field.startswith(str(value)),
            "in": lambda: field.belongs(
                value.split(",") if isinstance(value, str) else list(value)
            ),
            "contains": lambda: field.contains(value),
        }
        return expression[condition]()

    @staticmethod
    def parse_table_and_fields(text):
        if not RestAPI.re_table_and_fields.match(text):
            raise ValueError
        parts = text.split("[")
        if len(parts) == 1:
            return parts[0], []
        elif len(parts) == 2:
            return parts[0], parts[1][:-1].split(",")

    def search(self, tname, vars):
        def check_table_permission(tablename):
            if self.policy:
                self.policy.check_if_allowed("GET", tablename)

        def check_table_lookup_permission(tablename):
            if self.policy:
                self.policy.check_if_lookup_allowed(tablename)

        def filter_fieldnames(table, fieldnames):
            if self.policy:
                if fieldnames:
                    self.policy.check_fieldnames(table, fieldnames)
                else:
                    fieldnames = self.policy.allowed_fieldnames(table)
            elif not fieldnames:
                fieldnames = table.fields
            return fieldnames

        db = self.db
        tname, tfieldnames = RestAPI.parse_table_and_fields(tname)
        check_table_permission(tname)
        tfieldnames = filter_fieldnames(db[tname], tfieldnames)
        query = []
        offset = 0
        limit = 100
        model = False
        options_list = False
        table = db[tname]
        queries = []
        if self.policy:
            common_query = self.policy.get(tname, "GET", "query")
            if common_query:
                queries.append(common_query)
        hop1 = collections.defaultdict(list)
        hop2 = collections.defaultdict(list)
        hop3 = collections.defaultdict(list)
        model_fieldnames = tfieldnames
        lookup = {}
        orderby = None
        do_count = False
        for key, value in vars.items():
            if key == "@offset":
                offset = int(value)
            elif key == "@limit":
                limit = min(
                    int(value),
                    self.policy.get(tname, "GET", "limit")
                    if self.policy
                    else MAX_LIMIT,
                )
            elif key == "@order":
                orderby = [
                    ~table[f[1:]] if f[:1] == "~" else table[f]
                    for f in value.split(",")
                    if f.lstrip("~") in table.fields
                ] or None
            elif key == "@lookup":
                lookup = {item[0]: {} for item in RestAPI.re_lookups.findall(value)}
            elif key == "@model":
                model = str(value).lower()[:1] == "t"
            elif key == "@options_list":
                options_list = str(value).lower()[:1] == "t"
            elif key == "@count":
                if self.allow_count:
                    do_count = str(value).lower()[:1] == "t"
            else:
                key_parts = key.rsplit(".")
                if not key_parts[-1] in (
                    "eq",
                    "ne",
                    "gt",
                    "lt",
                    "ge",
                    "le",
                    "startswith",
                    "contains",
                    "in",
                ):
                    key_parts.append("eq")
                is_negated = key_parts[0] == "not"
                if is_negated:
                    key_parts = key_parts[1:]
                key, condition = key_parts[:-1], key_parts[-1]
                if len(key) == 1:  # example: name.eq=='Chair'
                    query = self.make_query(table[key[0]], condition, value)
                    queries.append(query if not is_negated else ~query)
                elif len(key) == 2:  # example: color.name.eq=='red'
                    hop1[is_negated, key[0]].append((key[1], condition, value))
                elif len(key) == 3:  # example: a.rel.desc.eq=='above'
                    hop2[is_negated, key[0], key[1]].append((key[2], condition, value))
                elif len(key) == 4:  # example: a.rel.b.name.eq == 'Table'
                    hop3[is_negated, key[0], key[1], key[2]].append(
                        (key[3], condition, value)
                    )

        for item in hop1:
            is_negated, fieldname = item
            ref_tablename = table[fieldname].type.split(" ")[1]
            ref_table = db[ref_tablename]
            subqueries = [self.make_query(ref_table[k], c, v) for k, c, v in hop1[item]]
            subquery = functools.reduce(lambda a, b: a & b, subqueries)
            query = table[fieldname].belongs(db(subquery)._select(ref_table._id))
            queries.append(query if not is_negated else ~query)

        for item in hop2:
            is_negated, linkfield, linktable = item
            ref_table = db[linktable]
            subqueries = [self.make_query(ref_table[k], c, v) for k, c, v in hop2[item]]
            subquery = functools.reduce(lambda a, b: a & b, subqueries)
            query = table._id.belongs(db(subquery)._select(ref_table[linkfield]))
            queries.append(query if not is_negated else ~query)

        for item in hop3:
            is_negated, linkfield, linktable, otherfield = item
            ref_table = db[linktable]
            ref_ref_tablename = ref_table[otherfield].type.split(" ")[1]
            ref_ref_table = db[ref_ref_tablename]
            subqueries = [
                self.make_query(ref_ref_table[k], c, v) for k, c, v in hop3[item]
            ]
            subquery = functools.reduce(lambda a, b: a & b, subqueries)
            subquery &= ref_ref_table._id == ref_table[otherfield]
            query = table._id.belongs(
                db(subquery)._select(ref_table[linkfield], groupby=ref_table[linkfield])
            )
            queries.append(query if not is_negated else ~query)

        if not queries:
            queries.append(table)

        query = functools.reduce(lambda a, b: a & b, queries)
        tfields = [table[tfieldname] for tfieldname in tfieldnames if
                   table[tfieldname].type != 'password']
        passwords = [tfieldname for tfieldname in tfieldnames if
                     table[tfieldname].type == 'password']
        rows = db(query).select(
            *tfields, limitby=(offset, limit + offset), orderby=orderby
        )
        if passwords:
            dpass = {password: '******' for password in passwords}
            for row in rows:
                row.update(dpass)

        lookup_map = {}
        for key in list(lookup.keys()):
            name, key = key.split(":") if ":" in key else ("", key)
            clean_key = RestAPI.re_no_brackets.sub("", key)
            lookup_map[clean_key] = {
                "name": name.rstrip("!") or clean_key,
                "collapsed": name.endswith("!"),
            }
            key = key.split(".")

            if len(key) == 1:
                key, tfieldnames = RestAPI.parse_table_and_fields(key[0])
                ref_tablename = table[key].type.split(" ")[1]
                ref_table = db[ref_tablename]
                tfieldnames = filter_fieldnames(ref_table, tfieldnames)
                check_table_lookup_permission(ref_tablename)
                ids = [row[key] for row in rows]
                tfields = [ref_table[tfieldname] for tfieldname in tfieldnames if
                           ref_table[tfieldname].type != 'password']
                if not "id" in tfieldnames:
                    tfields.append(ref_table["id"])
                drows = db(ref_table._id.belongs(ids)).select(*tfields).as_dict()
                if tfieldnames and not "id" in tfieldnames:
                    for row in drows.values():
                        del row["id"]
                lkey, collapsed = lookup_map[key]["name"], lookup_map[key]["collapsed"]
                for row in rows:
                    new_row = drows.get(row[key])
                    if collapsed:
                        del row[key]                        
                        for rkey in tfieldnames:
                            row[lkey + "." + rkey] = new_row[rkey] if new_row else None
                    else:
                        row[lkey] = new_row

            elif len(key) == 2:
                lfield, key = key
                key, tfieldnames = RestAPI.parse_table_and_fields(key)
                check_table_lookup_permission(key)
                ref_table = db[key]
                tfieldnames = filter_fieldnames(ref_table, tfieldnames)
                ids = [row["id"] for row in rows]
                tfields = [ref_table[tfieldname] for tfieldname in tfieldnames]
                if not lfield in tfieldnames:
                    tfields.append(ref_table[lfield])
                lrows = db(ref_table[lfield].belongs(ids)).select(*tfields)
                drows = collections.defaultdict(list)
                for row in lrows:
                    row = row.as_dict()
                    drows[row[lfield]].append(row)
                    if not lfield in tfieldnames:
                        del row[lfield]
                lkey = lookup_map[lfield + "." + key]["name"]
                for row in rows:
                    row[lkey] = drows.get(row.id, [])

            elif len(key) == 3:
                lfield, key, rfield = key
                key, tfieldnames = RestAPI.parse_table_and_fields(key)
                rfield, tfieldnames2 = RestAPI.parse_table_and_fields(rfield)
                check_table_lookup_permission(key)
                ref_table = db[key]
                ref_ref_tablename = ref_table[rfield].type.split(" ")[1]
                check_table_lookup_permission(ref_ref_tablename)
                ref_ref_table = db[ref_ref_tablename]
                tfieldnames = filter_fieldnames(ref_table, tfieldnames)
                tfieldnames2 = filter_fieldnames(ref_ref_table, tfieldnames2)
                ids = [row["id"] for row in rows]
                tfields = [ref_table[tfieldname] for tfieldname in tfieldnames]
                if not lfield in tfieldnames:
                    tfields.append(ref_table[lfield])
                if not rfield in tfieldnames:
                    tfields.append(ref_table[rfield])
                tfields += [ref_ref_table[tfieldname] for tfieldname in tfieldnames2]
                left = ref_ref_table.on(ref_table[rfield] == ref_ref_table["id"])
                lrows = db(ref_table[lfield].belongs(ids)).select(*tfields, left=left)
                drows = collections.defaultdict(list)
                lkey = lfield + "." + key + "." + rfield
                lkey, collapsed = (
                    lookup_map[lkey]["name"],
                    lookup_map[lkey]["collapsed"],
                )
                for row in lrows:
                    row = row.as_dict()
                    new_row = row[key]
                    lfield_value, rfield_value = new_row[lfield], new_row[rfield]
                    if not lfield in tfieldnames:
                        del new_row[lfield]
                    if not rfield in tfieldnames:
                        del new_row[rfield]
                    if collapsed:
                        new_row.update(row[ref_ref_tablename])
                    else:
                        new_row[rfield] = row[ref_ref_tablename]
                    drows[lfield_value].append(new_row)
                for row in rows:
                    row[lkey] = drows.get(row.id, [])

        response = {}
        if not options_list:
            response["items"] = rows.as_list()
        else:
            if table._format:
                response["items"] = [
                    dict(value=row.id, text=(table._format % row)) for row in rows
                ]
            else:
                response["items"] = [dict(value=row.id, text=row.id) for row in rows]
        if do_count or (self.allow_count == 'legacy' and offset == 0):
            response["count"] = db(query).count()            
        if model:
            response["model"] = self.table_model(table, model_fieldnames)
        return response
