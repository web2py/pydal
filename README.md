# pyDAL

[![pip version](https://img.shields.io/pypi/v/pydal.svg?style=flat-square)](https://pypi.python.org/pypi/pydal)
[![master-test](https://github.com/web2py/pydal/actions/workflows/run_test.yaml/badge.svg)](https://github.com/web2py/pydal/actions/workflows/run_test.yaml)
[![Coverage Status](https://img.shields.io/codecov/c/github/web2py/pydal.svg?style=flat-square)](https://codecov.io/github/web2py/pydal)
[![API Docs Status](https://readthedocs.org/projects/pydal/badge/?version=latest&style=flat-square)](http://pydal.rtfd.org/)

A pure-Python **Database Abstraction Layer**. pyDAL generates SQL (or the
appropriate query objects for NoSQL backends) in real time using the
dialect of the configured back end, so you write Python instead of SQL
and the same application runs unchanged against many databases.

## Why pyDAL

pyDAL is intentionally **not** an Object-Relational Mapper. Most ORMs —
Django ORM, SQLAlchemy — map tables to Python classes and rows to
instances of those classes. pyDAL instead treats rows as plain Python
dictionaries (with attribute access for convenience) and keeps the API
close to SQL. The result:

- A small, predictable surface. If you know SQL you'll be productive in
  minutes.
- No declarative class hierarchies, no metaclass magic, no two-phase
  schema bootstrap.
- The same query DSL runs against ~15 backends. Swap the connection
  string and the app keeps working.
- Rows are *just dicts* — easy to serialize, easy to pass around, easy
  to inspect.

## Installation

```bash
pip install pyDAL
```

The only hard dependency is Python ≥ 3.7. The SQLite driver ships with
Python, so the first example below runs out of the box. For other
backends, install the appropriate Python driver (`psycopg2`, `pymysql`,
`pymongo`, …) — pyDAL picks it up automatically.

## A first example

```python
from pydal import DAL, Field

db = DAL("sqlite://storage.db")
db.define_table("thing", Field("name"))

db.thing.insert(name="Chair")
db.thing.insert(name="Table")

for row in db(db.thing.name.startswith("C")).select():
    print(row.id, row.name)
# 1 Chair

db.commit()
```

Every line of the example above maps directly to a SQL operation:
`define_table` to `CREATE TABLE`, `insert` to `INSERT`, the call to `db(…)`
builds a `WHERE` clause, and `.select()` runs the query and returns rows.

## What's in the box

- **Schema definition** with explicit field types, validators, defaults,
  foreign keys, and indexes.
- **Migrations**: when a table definition changes between runs, the
  appropriate `ALTER TABLE` is generated and applied.
- **Transactions** with explicit commit/rollback.
- **A query DSL** covering comparisons, logical operators, string
  match, regex, aggregates, date/time accessors, `IN`, `CASE`,
  `COALESCE`, and substring expressions.
- **Inner joins, left outer joins, cross joins**, with `.on()` syntax or
  implicit cross-table queries.
- **Subqueries**: in `IN` clauses, as join sources, or as inline
  expressions.
- **Common Table Expressions** (CTEs), including recursive CTEs.
- **Type-safe parameter binding** with placeholders (`?`, `$1`, `%s`,
  …) selected per dialect.
- **Lazy iteration** for large result sets (`iterselect`).
- **Built-in CSV import/export** per-table or for the whole database.
- A natural-language **QueryBuilder** that turns
  `'name starts with C and age >= 18'` into a real query.

## Supported databases

A non-exhaustive list:

| Database     | Driver                                |
| ------------ | ------------------------------------- |
| SQLite       | `sqlite3` (built-in)                  |
| PostgreSQL   | `psycopg2`, `pg8000`                  |
| MySQL        | `pymysql`, `MySQLdb`                  |
| MSSQL        | `pyodbc`                              |
| Oracle       | `cx_Oracle`                           |
| FireBird     | `kinterbasdb`, `fdb`, `pyodbc`        |
| DB2          | `pyodbc`                              |
| Informix     | `informixdb`                          |
| Ingres       | `ingresdbi`                           |
| Sybase / SAP | `Sybase`, `sapdb`                     |
| Teradata     | `pyodbc`                              |
| Snowflake    | `snowflake-connector-python`          |
| MongoDB      | `pymongo`                             |
| Google Firestore | `google-cloud-firestore`          |
| IMAP         | `imaplib` (built-in)                  |

---

## The DAL API: a guided tour

The DSL is built from seven core objects. Once you've seen them once,
the rest of the API is just method calls.

### `DAL` — the connection

```python
db = DAL("sqlite://storage.sqlite")
```

The constructor accepts a **connection string** (also called the
`uri`). Examples for the most common backends:

| Database     | Connection string                                          |
| ------------ | ---------------------------------------------------------- |
| SQLite       | `sqlite://storage.sqlite` or `sqlite:memory`               |
| PostgreSQL   | `postgres://user:pass@localhost/test`                      |
| MySQL        | `mysql://user:pass@localhost/test?set_encoding=utf8mb4`    |
| MSSQL ≥ 2012 | `mssql4://user:pass@localhost/test`                        |
| Oracle       | `oracle://user/pass@test`                                  |
| MongoDB      | `mongodb://user:pass@localhost/test`                       |

You can also pass `None` to build a "dry" DAL that generates SQL
without connecting, or `do_connect=False` to defer connection until
needed.

A few commonly-used DAL parameters:

- `pool_size` — number of pooled connections (default `0`). Ignored
  by SQLite.
- `folder` — where migration metadata is written. Set this explicitly
  when using pyDAL standalone with SQLite.
- `migrate` — global default for whether table changes generate
  `ALTER TABLE` statements (default `True`).
- `check_reserved` — list of backend names to validate identifiers
  against (e.g. `["postgres", "mssql"]`).

### `Table` — a database table

You don't instantiate `Table` directly; you define it via the DAL:

```python
db.define_table("person", Field("name"), Field("age", "integer"))
```

This returns a `Table` object also accessible as `db.person`. Every
table automatically gets an auto-increment integer primary key called
`id` unless you explicitly opt out via the `primarykey` argument.

Some useful `Table` arguments:

- `format` — a record representation, used for foreign-key display:
  `format="%(name)s"` or `format=lambda r: r.name`.
- `rname` — the *real* SQL name when the table is known by a different
  identifier in the database (e.g. a legacy name, a schema-qualified
  name like `"app1.dbo.legacy_table"`).
- `redefine=True` — allow redefining an existing table (triggers a
  migration if the schema differs).

### `Field` — a column

```python
Field("name", "string", length=80, default="anonymous", required=True)
```

The default type is `"string"`. Available types:

| Type                | Notes                                          |
| ------------------- | ---------------------------------------------- |
| `string`            | default length 512                             |
| `text`              | default length 32768                           |
| `blob`              | binary; default length 2 GiB                   |
| `boolean`           |                                                |
| `integer`           | 32-bit                                         |
| `bigint`            | 64-bit                                         |
| `double`            |                                                |
| `decimal(n, m)`     | fixed precision                                |
| `date`              |                                                |
| `time`              |                                                |
| `datetime`          |                                                |
| `password`          | string with optional hashing validator         |
| `upload`            | stores a filename; file is saved on disk       |
| `json`              | any JSON-serializable value                    |
| `reference <table>` | foreign key to `<table>`                       |
| `list:string`       | a list of strings, stored encoded             |
| `list:integer`      | a list of integers                             |
| `list:reference <t>`| a list of foreign keys                         |

Field options you'll reach for often: `default`, `notnull`, `unique`,
`required`, `requires=<validator>`, `compute=<func>`, `update=<value>`,
`label`, `readable`, `writable`, `rname`.

### `Query` — a WHERE clause

A `Query` is the result of comparing or combining fields and values:

```python
q = (db.person.age >= 18) & (db.person.name != "anonymous")
```

Supported operators: `==`, `!=`, `<`, `<=`, `>`, `>=`, plus methods
`like`, `ilike`, `regexp`, `startswith`, `endswith`, `contains`,
`belongs`. Combine with `&` (AND), `|` (OR), `~` (NOT).

> Python's `and` / `or` can't be overloaded, so you must use `&` and
> `|` — and because they bind tighter than `==`, the parentheses
> around each side are required.

### `Set` — a queryable set of records

Calling the DAL with a query produces a `Set`:

```python
adults = db(db.person.age >= 18)
```

A `Set` doesn't run any SQL yet — it just remembers the query. The
real work happens when you call one of its methods:

```python
adults.count()       # SELECT COUNT(*) FROM person WHERE …
adults.select()      # SELECT * FROM person WHERE …
adults.update(age=21)
adults.delete()
adults.isempty()
```

`Set` also has `_select`, `_update`, `_count`, `_delete` (with
underscore) that **return the generated SQL string instead of
executing it** — handy for inspection, embedding as a sub-query, or
debugging.

### `Rows` — the result of `select()`

`select()` returns a `Rows` object: iterable, indexable, sliceable,
and self-serializing to CSV via `str(rows)`.

```python
rows = db(db.person.age >= 18).select()
for row in rows:
    print(row.id, row.name)

len(rows)             # number of rows
rows[0]               # first Row
rows.first()
rows.last()
rows.as_dict()        # {id: row, ...}
rows.as_list()        # [{name: …}, …]
```

For large result sets, use `iterselect()` instead — it returns rows
one at a time without loading them all into memory.

### `Row` — a single record

A `Row` is a dict that also supports attribute access:

```python
row = rows[0]
row.name          # attribute
row["name"]       # item
row("person.name")  # qualified name (useful when join columns collide)
```

Row methods:

```python
row.update_record(name="Alice")   # persists the change to the DB
row.delete_record()
```

`update_record` is *not* the same as `row.update(...)` — the latter
updates only the in-memory dict.

### `Expression`

Many things you'd write in SQL — `UPPER(name)`, `age + 1`,
`SUM(salary)`, ordering clauses — appear as `Expression` objects in
pyDAL. You build them with field methods and arithmetic, and use
them anywhere a field is allowed:

```python
total = db.person.salary.sum()
db().select(total)
db().select(db.person.ALL, orderby=db.person.name | db.person.id)
db().select(db.person.name.upper())
```

`Field` is itself a subclass of `Expression`.

---

## Inserting, updating, deleting

### Insert

```python
rid = db.person.insert(name="Alex", age=30)         # returns the new id
db.person.bulk_insert([                              # one query, many rows
    {"name": "Bob", "age": 25},
    {"name": "Carl", "age": 42},
])
```

`update_or_insert` writes a new record only if no existing record
matches:

```python
db.person.update_or_insert(db.person.name == "John",
                           name="John", age=30)
```

`validate_and_insert` / `validate_and_update` run the field validators
first and return `{"id": …, "errors": {…}, "success": bool}`.

### Update and delete via a Set

```python
db(db.person.age < 18).delete()                # returns number deleted
db(db.person.age >= 18).update(adult=True)     # returns number updated
```

Update values can be **expressions**:

```python
db(db.person.name == "Alex").update(visits=db.person.visits + 1)
```

### Shortcuts

```python
person = db.person[42]               # → Row with id=42, or None
db.person[42] = {"name": "Alice"}    # update
db.person[None] = {"name": "Alice"}  # insert
del db.person[42]                    # delete
```

---

## Selecting

The basic shape:

```python
rows = db(query).select(*fields, **options)
```

Field lists work like a SQL `SELECT` clause:

```python
db().select(db.person.ALL)                     # all columns
db().select(db.person.id, db.person.name)      # specific columns
db(db.person).select(db.person.name)           # query is just the table
```

### Options

| Option              | Effect                                      |
| ------------------- | ------------------------------------------- |
| `orderby=`          | `ORDER BY`. Use `~field` for DESC.          |
| `groupby=`          | `GROUP BY`                                  |
| `having=`           | `HAVING` (with `groupby`)                   |
| `limitby=(off, end)`| `LIMIT end-off OFFSET off`                  |
| `distinct=True`     | `DISTINCT`                                  |
| `distinct=field`    | `DISTINCT ON (field)` (PostgreSQL)          |
| `for_update=True`   | `FOR UPDATE`                                |
| `join=`             | INNER JOIN (`table.on(condition)`)          |
| `left=`             | LEFT OUTER JOIN                             |
| `cache=`            | wrap the result in a cache decorator        |

Example:

```python
rows = db(db.person.age >= 18).select(
    db.person.id, db.person.name,
    orderby=~db.person.age,
    limitby=(0, 10),
)
```

### Joins

The simplest join is implicit — reference fields from two tables in the
query and pyDAL puts them in `FROM`:

```python
rows = db(db.person.id == db.thing.owner_id).select()
for row in rows:
    print(row.person.name, "owns", row.thing.name)
```

The explicit form uses `table.on(condition)`:

```python
rows = db(db.person).select(
    db.person.name, db.thing.name,
    join=db.thing.on(db.person.id == db.thing.owner_id),
)
```

`left=` produces a LEFT OUTER JOIN — useful when you want all rows of
the driving table even if the join has no match:

```python
rows = db().select(
    db.person.ALL, db.thing.ALL,
    left=db.thing.on(db.person.id == db.thing.owner_id),
)
```

### Self-references and table aliases

When you need to join a table to itself (parent/child trees, etc.),
use `with_alias`:

```python
db.define_table("person",
    Field("name"),
    Field("father_id", "reference person"))

Father = db.person.with_alias("father")
rows = db().select(
    db.person.name, Father.name,
    left=Father.on(db.person.father_id == Father.id),
)
```

---

## Operators and expressions

### Comparison and logical

```python
db(db.person.age == 21).select()
db(db.person.age != 21).select()
db((db.person.age > 18) & (db.person.age < 65)).select()
db((db.person.name == "Alex") | (db.person.name == "Bob")).select()
db(~(db.person.role == "admin")).select()
```

### String matching

```python
db(db.person.name.like("A%")).select()
db(db.person.name.ilike("a%")).select()                       # case-insensitive
db(db.person.name.startswith("A")).select()
db(db.person.name.endswith("son")).select()
db(db.person.name.contains("li")).select()
db(db.person.name.regexp("^A.*")).select()                    # backend-dependent
db(db.person.name.upper().like("AL%")).select()
```

### Aggregates

```python
db.person.salary.sum()
db.person.salary.avg()
db.person.salary.min()
db.person.salary.max()
db.person.id.count()
db.person.name.len()
```

Use them anywhere a field is accepted:

```python
total = db.person.salary.sum()
row = db().select(total).first()
print(row[total])
```

### Dates

```python
db(db.log.event_time.year() == 2026).select()
db(db.log.event_time.month() >= 6).select()
db(db.log.event_time.day() == 15).select()
db(db.log.event_time.hour() < 12).select()
```

### `belongs` / `IN`

```python
db(db.person.id.belongs([1, 2, 3])).select()
```

With a subquery (note `_select`, not `select` — we want SQL, not rows):

```python
recent = db(db.log.severity == 3)._select(db.log.user_id)
db(db.person.id.belongs(recent)).select()
```

### `case`

```python
condition = db.person.age >= 18
label = condition.case("adult", "minor")
rows = db().select(db.person.name, label)
for row in rows:
    print(row.person.name, row[label])
```

### Defaults: `coalesce`, `coalesce_zero`

```python
display = db.user.fullname.coalesce(db.user.username)
db().select(display)

total = db.user.points.coalesce_zero().sum()
db().select(total)
```

### Substrings

```python
db().select(db.thing.name[:3])                # first 3 characters
db(db.thing.name[:1] == "A").select()         # name starts with A
```

---

## Subqueries

pyDAL offers three ways to build a subquery. All three produce the
same result; the AST-native forms are recommended for new code
because their bound parameters flow through to the cursor cleanly.

```python
# 1. Recommended: AST-native.
sub = db(db.thing.color == "red").subselect(db.thing.owner_id)
db(db.person.id.belongs(sub)).select()

# 2. Legacy Select object — works as a subquery or as a join source.
sub = db(db.thing.color == "red").nested_select(db.thing.owner_id)
db(db.person.id.belongs(sub)).select()

# 3. Raw SQL string (inline only).
sub = db(db.thing.color == "red")._select(db.thing.owner_id)
db(db.person.id.belongs(sub)).select()
```

`nested_select` is also the way to use a SELECT as a join source —
give it an alias with `.with_alias(name)` and use it like a table:

```python
sub = db(db.thing.color == "red").nested_select(
    db.thing.owner_id, db.thing.name
).with_alias("red_things")

db(db.person).select(
    db.person.name, sub.name,
    join=sub.on(sub.owner_id == db.person.id),
)
```

## Common Table Expressions

A CTE — `WITH name AS (SELECT …)` — is built with `set.cte(name, *fields)`:

```python
recent = db(db.event.created > "2026-01-01").cte(
    "recent", db.event.id, db.event.user_id
)
db(db.user.id.belongs(recent.user_id)).select()
```

Recursive CTEs use `.union(lambda self: …)` to add the recursive step:

```python
descendants = (
    db(db.org.id == root_id).cte(
        "descendants",
        db.org.id, db.org.name, db.org.parent_id,
    )
    .union(lambda descendants:
        db(db.org.parent_id == descendants.id).nested_select(
            db.org.id, db.org.name, db.org.parent_id,
        )
    )
)
db().select(descendants.ALL)
```

---

## Computed and virtual fields

A **computed field** is calculated on insert/update and stored:

```python
db.define_table("person",
    Field("first"),
    Field("last"),
    Field("full", compute=lambda row: f"{row['first']} {row['last']}"),
)
```

A **virtual field** is computed every time you access it, from the
result of a select — not stored, not queryable, but free:

```python
class PersonMethods:
    def full(row):
        return row.first + " " + row.last

db.person.full = Field.Virtual("full", lambda row: row.first + " " + row.last)
```

## Common filters

Attach a query to a table and every `Set` against that table will pick
it up automatically. Useful for soft-delete or tenant isolation:

```python
db.thing._common_filter = lambda q: db.thing.deleted == False
```

Bypass with `db(query, ignore_common_filters=True)`.

## Callbacks

Hook into insert / update / delete events:

```python
db.thing._before_insert.append(lambda fields: ...)
db.thing._after_update.append(lambda set, fields: ...)
db.thing._after_delete.append(lambda set: ...)
```

Returning a truthy value from a `_before_*` callback cancels the
operation.

---

## Validators

A **validator** is a callable that checks (and often coerces) a value
before it reaches the database. You attach one — or a list — to a
field via `requires=`:

```python
from pydal.validators import IS_NOT_EMPTY, IS_EMAIL, IS_INT_IN_RANGE

db.define_table("person",
    Field("name", requires=IS_NOT_EMPTY()),
    Field("email", requires=[IS_NOT_EMPTY(), IS_EMAIL()]),
    Field("age", "integer", requires=IS_INT_IN_RANGE(0, 150)),
)
```

Validators run when you call `validate_and_insert` /
`validate_and_update` (and `Form` in py4web). A plain `insert` does
**not** invoke them — they're meant for input that crosses a trust
boundary. Each validator returns `(cleaned_value, error_or_None)`, so
a string `"42"` going through `IS_INT_IN_RANGE` is stored as the int
`42`.

```python
result = db.person.validate_and_insert(name="", email="bad", age=200)
# result == {"id": None, "errors": {"name": "Enter a value",
#                                   "email": "Enter a valid email address",
#                                   "age": "Enter an integer between 0 and 149"},
#            "success": False}
```

If you don't set `requires=`, pyDAL installs a **default validator
chain** appropriate to the field type — `IS_LENGTH` for strings,
`IS_INT_IN_RANGE` for integers, `IS_DATE` for dates, `IS_IN_DB` for
references, and so on (see `pydal/default_validators.py`).

### Built-in validators

| Validator                 | Purpose                                              |
| ------------------------- | ---------------------------------------------------- |
| `IS_NOT_EMPTY()`          | non-blank (also strips whitespace)                   |
| `IS_LENGTH(maxsize, minsize)` | string / file length bounds                      |
| `IS_MATCH(regex)`         | regex match                                          |
| `IS_EQUAL_TO(value)`      | exact equality (e.g. password confirmation)          |
| `IS_ALPHANUMERIC()`       | letters, digits, underscore                          |
| `IS_SLUG(maxlen, check)`  | converts to a URL slug                               |
| `IS_LOWER()` / `IS_UPPER()` | case coercion                                      |
| `IS_INT_IN_RANGE(min, max)` | integer, `min <= v < max` (exclusive upper)        |
| `IS_FLOAT_IN_RANGE(min, max)` | float, inclusive bounds                          |
| `IS_DECIMAL_IN_RANGE(min, max)` | `Decimal`, inclusive bounds                    |
| `IS_DATE(format)`         | parses to `datetime.date`                            |
| `IS_TIME()`               | parses `hh:mm[:ss] [am/pm]` to `datetime.time`       |
| `IS_DATETIME(format, timezone)` | parses to `datetime.datetime`                  |
| `IS_DATE_IN_RANGE(min, max)` / `IS_DATETIME_IN_RANGE(...)` | date/datetime + bounds |
| `IS_EMAIL(banned, forced)` | email address, with optional domain allow/deny      |
| `IS_LIST_OF_EMAILS()`     | comma- or semicolon-separated email list             |
| `IS_URL(mode, allowed_schemes, prepend_scheme)` | http(s) URL                    |
| `IS_IPV4()` / `IS_IPV6()` / `IS_IPADDRESS()` | IP addresses                      |
| `IS_JSON(native_json)`    | parses / validates JSON                              |
| `IS_IN_SET(items, multiple, zero, sort)` | value in an explicit list             |
| `IS_IN_DB(dbset, field, label, multiple)` | value is an existing FK              |
| `IS_NOT_IN_DB(dbset, field)` | value is unique (enforces a UNIQUE check)         |
| `IS_LIST_OF(other, minimum, maximum)` | list whose items each pass `other`       |
| `IS_LIST_OF_STRINGS()` / `IS_LIST_OF_INTS()` | parses CSV / JSON-list input      |
| `IS_FILE(filename, extension)` | uploaded file name / extension match            |
| `IS_IMAGE(extensions, maxsize, minsize, aspectratio)` | uploaded image checks      |
| `IS_UPLOAD_FILENAME(...)` | legacy; prefer `IS_FILE`                             |
| `IS_SAFE(sanitizer, mode)` | strips/rejects unsafe HTML                          |
| `CLEANUP(regex)`          | strips control characters                            |
| `CRYPT(key, digest_alg, min_length, salt)` | hashes passwords lazily             |
| `IS_STRONG(min, upper, lower, number, special, entropy)` | password complexity   |
| `IS_EXPR(expression)`     | arbitrary Python expression (`value` in scope)       |

### Combinators

- **`IS_EMPTY_OR(other, null=None)`** — make any validator
  *optional*. Blank input is converted to `null` (default `None`);
  non-blank input is passed to `other`. Aliased as `IS_NULL_OR`.
- **`ANY_OF([v1, v2, ...])`** — succeeds if at least one inner
  validator passes. Useful when a field accepts more than one shape.

```python
Field("contact", requires=ANY_OF([IS_EMAIL(), IS_IPADDRESS()]))
Field("nickname", requires=IS_EMPTY_OR(IS_LENGTH(3, 32)))
```

### Custom validators

Any callable `f(value) -> (cleaned, error_or_None)` works as a
validator. For richer behavior (translation, `record_id`-aware
uniqueness checks), subclass `Validator` and implement `validate`:

```python
from pydal.validators import Validator, ValidationError

class IS_EVEN(Validator):
    def __init__(self, error_message="Must be even"):
        self.error_message = error_message

    def validate(self, value, record_id=None):
        if int(value) % 2 != 0:
            raise ValidationError(self.translator(self.error_message))
        return int(value)

Field("n", "integer", requires=IS_EVEN())
```

Every validator accepts an `error_message=` constructor argument to
override the default message. Messages are passed through
`Validator.translator` if you wire up an i18n hook.

### Passwords

`CRYPT` returns a `LazyCrypt` object that hashes on demand and knows
how to compare itself with a stored `algo$salt$hash` string:

```python
db.define_table("user",
    Field("password", "password",
          requires=[IS_STRONG(min=10, upper=2, special=2), CRYPT()]),
)
db.user.validate_and_insert(password="hunter2-Strong!")
stored = db.user[1].password           # 'pbkdf2(1000,20,sha512)$...$...'
CRYPT()("hunter2-Strong!")[0] == stored  # True
```

---

## Migrations

By default, when you call `define_table` with a different schema from
last run, pyDAL emits the appropriate `ALTER TABLE` statements. The
metadata is kept in a small file under `folder/` (one per table).

Disable per-table:

```python
db.define_table("legacy", Field("name"), migrate=False)
```

Disable globally:

```python
db = DAL("...", migrate_enabled=False)
```

After a destructive schema change, you may need a *fake* migration —
tell pyDAL the current state matches the file without running any DDL:

```python
db.define_table("thing", ..., fake_migrate=True)
```

## CSV import/export

Per-table:

```python
db.thing.export_to_csv_file(open("thing.csv", "w"))
db.thing.import_from_csv_file(open("thing.csv"))
```

Whole database:

```python
db.export_to_csv_file(open("dump.csv", "w"))
db.import_from_csv_file(open("dump.csv"))
```

## Natural-language queries: `QueryBuilder`

Turn an English-ish string into a real query:

```python
from pydal import QueryBuilder

builder = QueryBuilder(db.thing)
q = builder.parse('name starts with "C" and color == "red"')
db(q).select()
```

Recognized tokens: `not`, `and`, `or`, `==`, `!=`, `<`, `>`, `<=`,
`>=`, `is`, `is null`, `is not null`, `is true`, `is false`,
`contains`, `starts with`, `belongs`, `upper`, `lower`. Custom
aliases let you localize the vocabulary or rename fields.

---

## Optional tools

The modules under `pydal.tools` and the top-level `pydal.restapi` are
**not** part of the core DAL — nothing in pydal imports from them. Use
them when they fit, ignore them otherwise. Each one persists state in
DAL-managed tables, so swapping the backend keeps working.

### Tagging records: `pydal.tools.tags`

`Tags` attaches hierarchical tag paths (`color/red`, `style/modern`) to
any table **without altering its schema** — tags live in a sibling
`<tablename>_tag_<name>` table that is created on first use.

```python
from pydal.tools.tags import Tags

tags = Tags(db.thing)                       # creates db.thing_tag_default
tags.add(thing_id, "color/red")
tags.add(thing_id, ["color/red", "style/modern"])   # idempotent
```

Reading and removing:

```python
tags.get(thing_id)                          # ["color/red", "style/modern"]
tags.remove(thing_id, "color/red")
```

`find` returns a `Query` you pass to `db(...)`. Tag paths support
**prefix matching**, so `find("color")` matches every record tagged
`color/*`:

```python
db(tags.find("color/red")).select()         # exactly that tag
db(tags.find("color")).select()             # any color/* tag
db(tags.find(["color/red", "style/modern"])).select()         # AND
db(tags.find(["color/red", "color/blue"], mode="or")).select()  # OR
```

A single table can carry multiple **independent** taxonomies by passing
a `name` to the constructor:

```python
categories = Tags(db.thing, name="categories")
flags      = Tags(db.thing, name="flags")
# creates db.thing_tag_categories and db.thing_tag_flags
```

### Background tasks: `pydal.tools.scheduler`

A minimal cron-style scheduler that persists task runs in a DAL-managed
`task_run` table and executes them in forked child processes.

```python
from pydal import DAL
from pydal.tools.scheduler import Scheduler, now, delta

db = DAL("sqlite://storage.sqlite")
scheduler = Scheduler(db, max_concurrent_runs=2, folder="/tmp/scheduler")

def send_report(user_id):
    ...
    return {"sent": True}

scheduler.register_task("send_report", send_report)

scheduler.enqueue_run(name="send_report", inputs={"user_id": 42})
scheduler.enqueue_run(name="send_report", inputs={"user_id": 7},
                      scheduled_for=now() + delta(60))         # in 60s
scheduler.enqueue_run(name="send_report", inputs={"user_id": 1},
                      period=3600)                              # hourly
scheduler.enqueue_run(name="send_report", inputs={"user_id": 9},
                      priority=-10, timeout=30)                 # higher prio, 30s cap

scheduler.start()        # spawns a background loop thread
# ... your program continues ...
scheduler.stop()         # joins the loop thread cleanly
```

Each call to `enqueue_run` inserts a row into `db.task_run`; the loop
picks the next ready row (lowest `priority` first, then oldest `id`),
forks a daemon process, and records the outcome:

| Status      | Meaning                                       |
| ----------- | --------------------------------------------- |
| `queued`    | waiting for a worker                          |
| `assigned`  | claimed by a worker, not yet forked           |
| `running`   | child process is executing                    |
| `completed` | finished, `output` column holds the return    |
| `failed`    | raised; traceback captured in `log`           |
| `timeout`   | exceeded `timeout` seconds, killed            |
| `dead`      | child process disappeared                     |
| `unknown`   | enqueued under a name not in `register_task`  |

Inputs and outputs are stored as JSON, so task arguments must be
JSON-serializable and returns must be too (or `None`). Task `stdout`/
`stderr` from the child are captured into the row's `log` column.

`Scheduler` constructor parameters:

- `db` — the DAL to persist `task_run` into.
- `max_concurrent_runs` — per-worker cap on in-flight children (default `2`).
- `folder` — where per-run log files are buffered (default `/tmp/scheduler`).
- `sleep_time` — seconds to sleep between idle polls (default `10`).
- `logger` — custom `logging.Logger` (default writes to stdout).

Multiple processes can share the same `db` and run their own
`Scheduler` instance — task assignment is race-safe via an
update-with-where check.

### JSON REST API: `pydal.restapi`

`RestAPI` is a JSON CRUD front-end for any DAL. You hand it a `Policy`
(what's allowed, on which tables, for which methods), and call it like
an HTTP handler:

```python
from pydal.restapi import RestAPI, Policy

policy = Policy()
policy.set(tablename="person", method="GET",
           authorize=True,
           allowed_patterns=["name.*", "age.*"],
           limit=200, allow_lookup=True)
policy.set(tablename="person", method="POST", authorize=True,
           fields=["name", "age"])
policy.set(tablename="person", method="PUT", authorize=True)
policy.set(tablename="person", method="DELETE", authorize=True)

api = RestAPI(db, policy)

api("GET",    "person", get_vars={"name.startswith": "A", "@limit": 10})
api("GET",    "person", id=42)
api("POST",   "person", post_vars={"name": "Alice", "age": 30})
api("PUT",    "person", id=42, post_vars={"age": 31})
api("DELETE", "person", id=42)
```

Every call returns a JSON-serializable `dict` with `status`, `code`,
`timestamp`, and `api_version`; errors are converted to structured
responses (`401` policy violation, `404` not found, `400` invalid,
`422` validation errors).

Two pre-built policies are shipped: `ALLOW_ALL_POLICY` (wildcard, all
methods authorized) and `DENY_ALL_POLICY` (empty).

**GET query language.** Regular get-vars are field predicates:

```
field[.subfield][.op]=value
```

where `op` is one of `eq` (default), `ne`, `lt`, `gt`, `le`, `ge`,
`startswith`, `contains`, `in` (comma-separated values). Prefix with
`not.` to negate. Up to four dotted hops traverse `reference` fields:

```
api("GET", "thing", get_vars={"owner.name.startswith": "A"})
```

`@`-prefixed meta-options control the response shape:

| Meta-option       | Effect                                            |
| ----------------- | ------------------------------------------------- |
| `@offset`/`@limit`| Pagination (capped by policy `limit`).            |
| `@order`          | Comma-separated fields; `~field` for DESC.        |
| `@lookup`         | Reference traversal — include joined records.     |
| `@model`          | Include the table schema in the response.        |
| `@options_list`   | Return `{value, text}` pairs instead of full rows.|
| `@count`          | Include a total `count` (independent of `@limit`).|

`Policy` attributes per `(tablename, method)`:

- `authorize` — `True`/`False` or `f(tablename, id, get_vars, post_vars) -> bool`.
- `fields` — list of allowed field names (`None` means all readable/writable).
- `query` — a common filter `Query` applied to every GET (e.g. tenant scoping).
- `allowed_patterns` / `denied_patterns` — `fnmatch` against get-var keys.
- `limit` — max value accepted for `@limit`.
- `allow_lookup` — whether `@lookup=` traversal is honored.

Use `tablename="*"` as a wildcard fallback for any table not explicitly
listed.

---

## Generating SQL without a database

You can use pyDAL purely as a SQL generator — no Postgres/MySQL driver
installed, no database server running. Open an in-memory SQLite (always
available, no driver to install), define your schema, then **swap the
dialect** on the existing adapter to render the same queries against
the target backend's syntax:

```python
from pydal import DAL, Field
from pydal.dialects.postgre import PostgreDialect

# Always-available "scratch" connection. No external database needed.
db = DAL("sqlite:memory", migrate=False)

# Retarget SQL emission to PostgreSQL, and ask for inline values
# (placeholder-free SQL) — handier for human inspection than the
# parameterized form used at runtime.
db._adapter.dialect = PostgreDialect(db._adapter)
db._adapter.compiler.parameterize = False

db.define_table("person", Field("name"), Field("age", "integer"))

q = (db.person.age >= 18) & (db.person.name.like("A%"))

# The five "_underscore" entry points return SQL strings without
# executing anything against the (in-memory) database.
print(db(q)._select(db.person.id, db.person.name))
# SELECT "person"."id", "person"."name" FROM "person"
# WHERE (("person"."age" >= 18) AND ("person"."name" LIKE 'A%' ESCAPE '\'));

print(db(db.person.age < 18)._delete())
# DELETE FROM "person" WHERE ("person"."age" < 18);

print(db(db.person.id == 1)._update(name="Alice"))
# UPDATE "person" SET "name"='Alice' WHERE ("person"."id" = 1);

print(db.person._insert(name="Alice", age=30))
# INSERT INTO "person"("name","age") VALUES ('Alice',30);

print(db(db.person.age >= 18)._count())
# SELECT COUNT(*) FROM "person" WHERE ("person"."age" >= 18);
```

The query AST is dialect-agnostic, so the same `Query` retargets when
you swap dialects mid-flight — handy for cross-backend comparisons:

```python
q = db.person.name.regexp("^A")

# Already swapped to Postgres above:
print(db(q)._select(db.person.id))
# SELECT "person"."id" FROM "person" WHERE ("person"."name" ~ '^A');

# Switch to MySQL on the spot:
from pydal.dialects.mysql import MySQLDialect
db._adapter.dialect = MySQLDialect(db._adapter)
print(db(q)._select(db.person.id))
# (MySQL-flavored SQL emitted for the same Query object)
```

## Raw SQL escape hatch

When the DSL doesn't cover what you need:

```python
rows = db.executesql("SELECT * FROM thing WHERE name = ?", placeholders=["Chair"])
```

For inspection without execution, every `Set` method has an underscore
counterpart that returns the generated SQL:

```python
db(db.thing.name == "Chair")._select()
# 'SELECT "thing"."id", "thing"."name" FROM "thing" WHERE ("thing"."name" = ?);'
```

---

## Ecosystem

pyDAL is a standalone library — drop it into any Python project. It is
also the data layer used by **py4web**, which can automatically
generate forms and grids from pyDAL table metadata. If you're building
a full web app, py4web saves you a lot of plumbing; if you just need a
database layer, pyDAL alone is enough.

## License

pyDAL is released under the BSD-3-Clause license. See `LICENSE.txt`.
