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

import re
import ast
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('pydal/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

setup(
    name='pydal',
    version=version,
    url='https://github.com/web2py/pydal',
    license='BSD',
    author='Massimo Di Pierro',
    author_email='massimo.dipierro@gmail.com',
    maintainer='Massimo Di Pierro',
    maintainer_email='massimo.dipierro@gmail.com',
    description='a pure Python Database Abstraction Layer (for python version 2.7 and 3.x)',
    long_description=__doc__,
    packages=[
        'pydal', 'pydal.adapters', 'pydal.dialects', 'pydal.helpers',
        'pydal.parsers', 'pydal.representers', 'pydal.contrib'],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
