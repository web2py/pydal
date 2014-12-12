"""
pyDAL description...


Links
-----
* `website <http://>`_
* `documentation <http://>`_
* `git repo <http://>`_
"""

from setuptools import setup
setup(
    name='pyDAL',
    version='1.0',
    url='http://',
    license='BSD',
    author='Massimo Di Pierro',
    author_email='mdipierro@cs.depaul.edu',
    maintainer='Giovanni Barillari',
    maintainer_email='gi0baro@d4net.org',
    description='Some description needed here',
    long_description=__doc__,
    packages=['pydal', 'pydal.adapters', 'pydal.helpers', 'pydal.contrib',
              'pydal.contrib.pg8000', 'pydal.contrib.pymysql',
              'pydal.contrib.pymysql.constants', 'pydal.contrib.simplejson'],
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
