"""
pyDAL is a pure Python Database Abstraction Layer.

It dynamically generates the SQL in real time using the specified dialect for
the database back end, so that you do not have to write SQL code or learn
different SQL dialects (the term SQL is used generically), and your code will
be portable among different types of databases.

pyDAL comes from the original web2py's DAL, with the aim of being
wide-compatible. pyDAL doesn't require web2py and can be used in any
Python context.


Links
-----
* `website <https://github.com/web2py/pydal>`_
* `documentation <http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer>`_
"""
DEFAULTS = dict(
    long_description=__doc__,
    packages=['pydal', 'pydal.adapters', 'pydal.helpers', 'pydal.contrib',
              'pydal.contrib.pg8000', 'pydal.contrib.pymysql',
              'pydal.contrib.pymysql.constants', 'pydal.contrib.pymysql.tests',
              'pydal.contrib.simplejson'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
)

from ConfigParser import SafeConfigParser
config_values = SafeConfigParser()
config_values.read('setup.cfg')
for k,v in config_values.items('metadata'):
    DEFAULTS[k] = v

from setuptools import setup

setup(
    **DEFAULTS
)
