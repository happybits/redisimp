#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from os import path
from setuptools import setup
from distutils.cmd import Command

NAME = 'redisimp'

ROOTDIR = path.abspath(os.path.dirname(__file__))

with open(os.path.join(ROOTDIR, 'README.rst')) as f:
    readme = f.read()

with open(os.path.join(ROOTDIR, NAME, 'VERSION'), 'r') as f:
    version = str(f.read().strip())


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import sys
        import subprocess

        raise SystemExit(
            subprocess.call([sys.executable, '-m', 'test']))


cmdclass = {'test': TestCommand}
ext_modules = []

setup(
    name=NAME,
    version=version,
    description='Redis Import tool, copy data from one redis instance'
                ' into another or into a cluster.',
    author='John Loehrer',
    author_email='72squared@gmail.com',
    url='https://github.com/happybits/%s' % NAME,
    download_url='https://github.com/72squared/%s/archive/%s.tar.gz' %
                 (NAME, version),
    packages=[NAME],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Environment :: Web Environment',
        'Operating System :: POSIX'],
    license='MIT',
    install_requires=['redis>=2.10.2', 'redislite>=3.0.0', 'six'],
    tests_require=['redislite>=3.0.271', 'redis-py-cluster>=1.3.0'],
    include_package_data=True,
    long_description=readme,
    entry_points={'console_scripts': ['redisimp = redisimp.cli:main']},
    cmdclass=cmdclass,
    ext_modules=ext_modules
)
