Redis Import Tool
=================

|BuildStatus| |CoverageStatus| |Version| |Python|


**RedisImp** allows you to quickly and safely import data from other redis hosts
 into a running redis instance or cluster.

.. code-block::

    redisimp -s 127.0.0.1:6379 -d 127.0.0.1:6380


The script will take all the keys from the source `127.0.0.1:6379` and copy
them into the destination `127.0.0.1:6380`.

It also allows you to copy data stored in RDB files.

.. code-block::

    redisimp -s ./dump.rdb -d 127.0.0.1:6380


You can also copy only a subset of keys by using a regex pattern:

.. code-block::

    redisimp -s 127.0.0.1:6379 -d 127.0.0.1:6380 --pattern '/^I\{[A-Za-z0-9_\-]+\}$/'




Or a glob style pattern:

.. code-block::

    redisimp -s 127.0.0.1:6379 -d 127.0.0.1:6380 --pattern 'I{*}'



.. |BuildStatus| image:: https://travis-ci.org/happybits/redisimp.svg?branch=master
    :target: https://travis-ci.org/happybits/redisimp

.. |CoverageStatus| image:: https://coveralls.io/repos/github/happybits/redisimp/badge.svg?branch=master
    :target: https://coveralls.io/github/happybits/redisimp?branch=master

.. |Version| image:: https://badge.fury.io/py/redisimp.svg
    :target: https://badge.fury.io/py/redisimp

.. |Python| image:: https://img.shields.io/badge/python-2.7,3.4,pypy-blue.svg
    :target:  https://pypi.python.org/pypi/redisimp/
