# pyDAL

pyDAL is a pure Python Database Abstraction Layer.

It dynamically generates the SQL/noSQL in realtime using the specified dialect for the database backend, so that you do not have to write SQL code or learn different SQL dialects (the term SQL is used generically), and your code will be portable among different types of databases.

pyDAL comes from the original web2py's DAL, with the aim of being compatible with any Python program. pyDAL doesn't require web2py and can be used in any Python context.

[![pip version](https://img.shields.io/pypi/v/pydal.svg?style=flat-square)](https://pypi.python.org/pypi/pydal)
[![Build Status](https://img.shields.io/travis/web2py/pydal/master.svg?style=flat-square&label=Travis-CI)](https://travis-ci.org/web2py/pydal)
[![MS Build Status](https://img.shields.io/appveyor/ci/web2py/pydal/master.svg?style=flat-square&label=Appveyor-CI)](https://ci.appveyor.com/project/web2py/pydal)
[![Coverage Status](https://img.shields.io/codecov/c/github/web2py/pydal.svg?style=flat-square)](https://codecov.io/github/web2py/pydal)
[![API Docs Status](https://readthedocs.org/projects/pydal/badge/?version=latest&style=flat-square)](http://pydal.rtfd.org/)

## Installation

You can install pyDAL using `pip`:

    pip install pyDAL

## Usage and Documentation

Here is a quick example:

    >>> from pydal import DAL, Field
    >>> db = DAL('sqlite://storage.db')
    >>> db.define_table('thing', Field('name'))
    >>> db.thing.insert(name='Chair')
    >>> query = db.thing.name.startswith('C')
    >>> rows = db(query).select()
    >>> print rows[0].name
    Chair
    >>> db.commit()

The complete documentation is available on http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer

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

## License

pyDAL is released under the BSD-3c License.  For further details, please check the `LICENSE` file.
