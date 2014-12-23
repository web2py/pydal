# Database Abstraction Layer

This is the web2py's Database Abstraction Layer. It does not require web2py and can be used with any Python program.

## Tests

[![Build Status](https://img.shields.io/travis/web2py/pydal.svg?style=flat-square)](https://travis-ci.org/web2py/pydal)

It is documented here:

http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer

## Quick example

    >>> from pydal import DAL, Field
    >>> db = DAL('sqlite://storage.db')
    >>> db.define_table('thing',Field('name'))
    >>> db.thing.insert(name="Chair')
    >>> query = db.thing.name.startswith('C')
    >>> rows = db(query).select()
    >>> print rows[0].name
    Chair

## Supported Databases

sqlite, postgresql, mysql, mssql, db2, firebird, sybase, oracle, informix, teradata, sapdb, ingres, cubrid, imap, mongodb

## Features

Transactions, Aggregates, Inner Joins, Outer Joins, Nested Selects

## License

BSD v3


