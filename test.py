#!/usr/bin/env python

# std lib
import os
import unittest

# 3rd party
import redis
import redislite
import rediscluster
import redislite.patch

# our package
import redisimp  # noqa

TEST_DIR = os.path.dirname(__file__)
SRC = redislite.StrictRedis(os.path.join(TEST_DIR, '.redis_src.db'))
SRC_ALT = redislite.StrictRedis(os.path.join(TEST_DIR, '.redis_src_alt.db'))
DST = redislite.StrictRedis(os.path.join(TEST_DIR, '.redis_dst.db'))


def flush_redis_data(conn):
    if conn is None:
        return

    if rediscluster and isinstance(conn, rediscluster.StrictRedisCluster):
        conns = [redis.StrictRedis(host=node['host'], port=node['port'])
                 for node in conn.connection_pool.nodes.nodes.values()
                 if node.get('server_type', None) == 'master']
        for conn in conns:
            conn.flushall()
    else:
        conn.flushdb()


def clean():
    flush_redis_data(SRC)
    flush_redis_data(DST)


class CopyTestCase(unittest.TestCase):

    def populate(self):
        pass

    def setUp(self):
        clean()
        self.populate()
        self.keys = set()
        for key in redisimp.copy(SRC, DST):
            self.keys.add(key)

    def tearDown(self):
        clean()


class MultiCopyTestCase(unittest.TestCase):

    def populate(self):
        pass

    def setUp(self):
        clean()
        self.populate()
        self.keys = set()
        for key in redisimp.multi_copy([SRC, SRC_ALT], DST):
            self.keys.add(key)

    def tearDown(self):
        clean()


class CopyStrings(CopyTestCase):

    def populate(self):
        SRC.set('foo', 'a')
        SRC.set('bar', 'b')
        SRC.set('bazz', 'c')

    def test(self):
        self.assertEqual(self.keys, {'foo', 'bar', 'bazz'})
        self.assertEqual(DST.get('foo'), u'a')
        self.assertEqual(DST.get('bar'), u'b')
        self.assertEqual(DST.get('bazz'), u'c')


class CopySortedSets(CopyTestCase):

    def populate(self):
        SRC.zadd('foo', 1, 'one')
        SRC.zadd('foo', 2, 'two')
        SRC.zadd('foo', 3, 'three')

        SRC.zadd('bar', 1, 'one')
        SRC.zadd('bar', 2, 'two')
        SRC.zadd('bar', 3, 'three')

    def test(self):
        self.assertEqual(self.keys, {'foo', 'bar'})
        self.assertEqual(DST.zrange('foo', 0, -1, withscores=True),
                         [(u'one', 1), (u'two', 2), (u'three', 3)])

        self.assertEqual(DST.zrange('bar', 0, -1, withscores=True),
                         [(u'one', 1), (u'two', 2), (u'three', 3)])


class MultiCopySortedSets(MultiCopyTestCase):

    def populate(self):
        SRC.zadd('foo', 1, 'one')
        SRC.zadd('foo', 2, 'two')
        SRC.zadd('foo', 3, 'three')

        SRC_ALT.zadd('bar', 1, 'one')
        SRC_ALT.zadd('bar', 2, 'two')
        SRC_ALT.zadd('bar', 3, 'three')

    def test(self):
        self.assertEqual(self.keys, {'foo', 'bar'})
        self.assertEqual(DST.zrange('foo', 0, -1, withscores=True),
                         [(u'one', 1), (u'two', 2), (u'three', 3)])

        self.assertEqual(DST.zrange('bar', 0, -1, withscores=True),
                         [(u'one', 1), (u'two', 2), (u'three', 3)])

if __name__ == '__main__':
    unittest.main(verbosity=2)
