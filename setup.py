#!/usr/bin/env python
import os
from os import path
from setuptools import setup
import imp

MYDIR = path.abspath(os.path.dirname(__file__))
long_description = open(os.path.join(MYDIR, 'README.rst')).read()

version = imp.load_source('version',
                          path.join('.', 'redisimp', 'version.py')).__version__

setup(
    name='redisimp',
    version=version,
    description='Redis Import tool, copy data from one redis instance'
                ' into another or into a cluster.',
    author='John Loehrer',
    author_email='john@happybits.co',
    url='https://github.com/happybits/redisimp',
    packages=['redisimp'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],
    install_requires=[
        'redis>=2.10.2',
        'redislite>=3.0.0',
        'six'
    ],
    entry_points={
        'console_scripts': [
            'redisimp = redisimp.cli:main',
        ]
    },
    license='',
    include_package_data=True,
    long_description=long_description,
)
