# pyDAL

[![pip version](https://img.shields.io/pypi/v/pydal.svg?style=flat-square)](https://pypi.python.org/pypi/pydal)
[![master-test](https://github.com/web2py/pydal/actions/workflows/run_test.yaml/badge.svg)](https://github.com/web2py/pydal/actions/workflows/run_test.yaml)
[![Coverage Status](https://img.shields.io/codecov/c/github/web2py/pydal.svg?style=flat-square)](https://codecov.io/github/web2py/pydal)
[![API Docs Status](https://readthedocs.org/projects/pydal/badge/?version=latest&style=flat-square)](http://pydal.rtfd.org/)

pyDAL is a pure Python Database Abstraction Layer.

It dynamically generates the SQL/noSQL in realtime using the specified dialect for the database backend, so that you do not have to write SQL code or learn different SQL dialects (the term SQL is used generically), and your code will be portable among different types of databases.
What makes pyDAL different from most of the other DALs is the syntax: it maps records to python dictionaries, which is simpler and closer to SQL. Other famous frameworks instead strictly rely on an Object Relational Mapping (ORM) like the Django ORM or the SQL Alchemy ORM, that maps tables to Python classes and rows to Objects.

Historically pyDAL comes from the original web2py's DAL, with the aim of being compatible with any Python program. However, pyDAL nowadays is an indipendent package that can be used in any Python 3.7+ context.

## Installation

You can install pyDAL using `pip`:

```bash
pip install pyDAL
```

## Usage and Documentation

Here is a quick example:

```pycon
>>> from pydal import DAL, Field
>>> db = DAL('sqlite://storage.db')
>>> db.define_table('thing', Field('name'))
>>> db.thing.insert(name='Chair')
>>> query = db.thing.name.startswith('C')
>>> rows = db(query).select()
>>> print rows[0].name
Chair
>>> db.commit()
```

The complete updated documentation is available on [the py4web manual](https://py4web.com/_documentation/static/en/chapter-07.html)

## What's in the box?

A little *taste* of pyDAL features:

* Transactions
* Aggregates
* Inner Joins
* Outer Joins
* Nested Selects

## Which databases are supported?

pyDAL supports the following databases:

* SQLite
* MySQL
* PostgreSQL
* MSSQL
* FireBird
* Oracle
* DB2
* Ingres
* Sybase
* Informix
* Teradata
* Cubrid
* SAPDB
* IMAP
* MongoDB
* Google Firestore

## License

pyDAL is released under the BSD-3c License.  For further details, please check the `LICENSE` file.
