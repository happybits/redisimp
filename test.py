#!/usr/bin/env python

# std lib
import os
import unittest
from StringIO import StringIO

# 3rd party
import redis
import redislite
import rediscluster
import redislite.patch

# our package
import redisimp  # noqa

TEST_DIR = os.path.dirname(__file__)
SRC_RDB = os.path.join(TEST_DIR, '.redis_src.db')
SRC = redislite.StrictRedis(SRC_RDB)
SRC_ALT_RDB = os.path.join(TEST_DIR, '.redis_src_alt.db')
SRC_ALT = redislite.StrictRedis(SRC_ALT_RDB)
DST_RDB = os.path.join(TEST_DIR, '.redis_dst.db')
DST = redislite.StrictRedis(DST_RDB)


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


class CopyWithFilter(unittest.TestCase):

    def setUp(self):
        clean()
        self.populate()
        self.keys = set()
        for key in redisimp.multi_copy([SRC], DST, filter='f*'):
            self.keys.add(key)

    def tearDown(self):
        clean()

    def populate(self):
        SRC.zadd('foo', 1, 'one')
        SRC.zadd('foo', 2, 'two')
        SRC.zadd('foo', 3, 'three')

        SRC.zadd('bar', 1, 'one')
        SRC.zadd('bar', 2, 'two')
        SRC.zadd('bar', 3, 'three')

    def test(self):
        self.assertEqual(self.keys, {'foo'})
        self.assertEqual(DST.zrange('foo', 0, -1, withscores=True),
                         [(u'one', 1), (u'two', 2), (u'three', 3)])

        self.assertEqual(DST.zrange('bar', 0, -1), [])


class MultiCopyWithFilter(unittest.TestCase):

    def setUp(self):
        clean()
        self.populate()
        self.keys = set()
        for key in redisimp.multi_copy([SRC, SRC], DST, filter='f*'):
            self.keys.add(key)

    def tearDown(self):
        clean()

    def populate(self):
        SRC.zadd('foo', 1, 'one')
        SRC.zadd('foo', 2, 'two')
        SRC.zadd('foo', 3, 'three')

        SRC.zadd('bar', 1, 'one')
        SRC.zadd('bar', 2, 'two')
        SRC.zadd('bar', 3, 'three')

    def test(self):
        self.assertEqual(self.keys, {'foo'})
        self.assertEqual(DST.zrange('foo', 0, -1, withscores=True),
                         [(u'one', 1), (u'two', 2), (u'three', 3)])

        self.assertEqual(DST.zrange('bar', 0, -1), [])


class TestParseArgs(unittest.TestCase):
    def test_minimal(self):
        args = redisimp.cli.parse_args(['-s', '0:6379', '-d', '0:6380'])
        self.assertEqual(args.verbose, False)
        self.assertEqual(args.workers, None)
        self.assertEqual(args.src, '0:6379')
        self.assertEqual(args.dst, '0:6380')

    def test_filter(self):
        args = redisimp.cli.parse_args(
            ['--filter', 'V{*}', '-s', '0:6379', '-d', '0:6380'])
        self.assertEqual(args.filter, 'V{*}')

    def test_workers(self):
        args = redisimp.cli.parse_args(
            ['--workers', '2', '-s', '0:6379', '-d', '0:6380'])
        self.assertEqual(args.workers, 2)

    def test_verbose(self):
        args = redisimp.cli.parse_args(
            ['-s', '0:6379', '-d', '0:6380', '-v'])
        self.assertEqual(args.verbose, True)


class TestMain(unittest.TestCase):

    def setUp(self):
        clean()
        self.populate()

    def tearDown(self):
        clean()

    def populate(self):
        SRC.zadd('foo{a}', 1, 'one')
        SRC.zadd('foo{a}', 2, 'two')
        SRC.zadd('foo{a}', 3, 'three')

        SRC.zadd('foo{b}', 1, 'one')
        SRC.zadd('foo{b}', 2, 'two')
        SRC.zadd('foo{b}', 3, 'three')

        SRC.zadd('bar', 1, 'one')
        SRC.zadd('bar', 2, 'two')
        SRC.zadd('bar', 3, 'three')

    def test(self):
        self.assertEqual(DST.zrange('foo{a}', 0, -1, withscores=True), [])
        self.assertEqual(DST.zrange('foo{b}', 0, -1, withscores=True), [])
        self.assertEqual(DST.zrange('bar', 0, -1, withscores=True), [])
        out = StringIO()
        redisimp.main(['-s', SRC_RDB, '-d', DST_RDB, '--filter', 'foo{*}'], out=out)
        res = DST.zrange('foo{a}', 0, -1, withscores=True)
        self.assertEqual(res, [('one', 1), ('two', 2), ('three', 3)])
        res = DST.zrange('foo{b}', 0, -1, withscores=True)
        self.assertEqual(res, [('one', 1), ('two', 2), ('three', 3)])
        res = DST.zrange('bar', 0, -1, withscores=True)
        self.assertEqual(res, [])

        output = out.getvalue().strip()
        self.assertEqual(output, "processed 2 keys")


if __name__ == '__main__':
    unittest.main(verbosity=2)
