import re
import rediscluster
from .rdbparser import parse_rdb
from itertools import islice, chain
import fnmatch
from six import string_types
try:
    from itertools import izip_longest as zip_longest  # noqa
except ImportError:
    from itertools import zip_longest  # noqa


__all__ = ['copy']


def _cmp(a, b):
    return (a > b) - (a < b)

def _chunks(iterable, size, fillvalue=None):
    """
    chunk(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')
    """
    return zip_longest(*[iter(iterable)] * size, fillvalue=fillvalue)


def _read_keys(src, batch_size=500, pattern=None):
    """
    iterate through batches of keys from source
    :param src: redis.StrictRedis
    :param batch_size: int
    :param pattern: str
    :yeild: array of keys
    :return: generator
    """
    if pattern is not None and pattern.startswith('/') and pattern.endswith('/'):
        regex_pattern = re.compile(pattern[1:-1])
        pattern = None
    else:
        regex_pattern = None

    cursor = 0
    while True:
        cursor, keys = src.scan(cursor=cursor, count=batch_size, match=pattern)
        if keys:
            if regex_pattern is not None:
                keys = [key for key in keys if regex_pattern.match("%s" % key)]
            yield keys

        if cursor == 0:
            break


def _read_data_and_pttl(src, keys):
    pipe = src.pipeline(transaction=False)
    for key in keys:
        pipe.dump(key)
        pipe.pttl(key)
    res = pipe.execute()

    for i, key in enumerate(keys):
        ii = i * 2
        data = res[ii]
        pttl = int(res[ii + 1])
        if len(data) < 1:
            continue
        if pttl < 1:
            pttl = 0
        yield key, data, pttl


def _compare_version(version1, version2):
    def normalize(v):
        return [int(x) for x in re.sub(r'(\.0+)*$', '', v).split(".")]

    return _cmp(normalize(version1), normalize(version2))


def _supports_replace(conn):
    if isinstance(conn, rediscluster.StrictRedisCluster):
        return True
    version = conn.info().get('redis_version')
    if not version:
        return False

    if _compare_version(version, '3.0.0') >= 0:
        return True
    else:
        return False


def _replace_restore(pipe, key, pttl, data):
    pipe.execute_command('RESTORE', key, pttl, data, 'REPLACE')


def _delete_restore(pipe, key, pttl, data):
    pipe.delete(key)
    pipe.restore(key, pttl, data)


def _get_restore_handler(conn):
    if _supports_replace(conn):
        return _replace_restore
    else:
        return _delete_restore


def _dry_run_copy(src, pattern=None):
    """
    yields the keys it processes as it goes.
    :param pattern:
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :return: None
    """
    for keys in _read_keys(src, pattern=pattern):
        for key in keys:
            yield key


def _clobber_copy(src, dst, pattern=None):
    """
    yields the keys it processes as it goes.
    :param pattern:
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :return: None
    """
    read = _read_data_and_pttl
    _restore = _get_restore_handler(dst)

    for keys in _read_keys(src, pattern=pattern):
        pipe = dst.pipeline(transaction=False)
        for key, data, pttl in read(src, keys):
            _restore(pipe, key, pttl, data)
            yield key
        pipe.execute()


def _backfill_copy(src, dst, pattern=None):
    """
    yields the keys it processes as it goes.
    WON'T OVERWRITE the key if it exists. It'll skip over it.
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :param pattern: str
    :return: None
    """
    read = _read_data_and_pttl
    for keys in _read_keys(src, pattern=pattern):
        # don't even bother reading the data if the key already exists in the src.
        pipe = dst.pipeline(transaction=False)
        for key in keys:
            pipe.exists(key)
        keys = [keys[i] for i, result in enumerate(pipe.execute()) if
                not result]
        if not keys:
            continue

        pipe = dst.pipeline(transaction=False)

        for key, data, pttl in read(src, keys):
            pipe.restore(key, pttl, data)

        for i, result in enumerate(pipe.execute(raise_on_error=False)):
            if not isinstance(result, Exception):
                yield keys[i]
                continue

            if 'is busy' in str(result):
                continue

            raise result


def rdb_regex_pattern(pattern):
    if pattern is None:
        def matchall(x):
            return True

        return matchall

    if pattern.startswith('/') and pattern.endswith('/'):
        return re.compile(pattern[1:-1]).match
    else:
        def fnmatch_pattern(name):
            return fnmatch.fnmatchcase("%s" % name, pattern)

        return fnmatch_pattern


def _rdb_clobber_copy(src, dst, pattern=None):
    """
    yields the keys it processes as it goes.
    :param pattern:
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :return: None
    """
    _restore = _get_restore_handler(dst)
    matcher = rdb_regex_pattern(pattern)
    for rows in _chunks(parse_rdb(src, matcher), 500):
        pipe = dst.pipeline(transaction=False)
        for row in rows:
            if row is None:
                continue
            key, data, pttl = row
            _restore(pipe, key, pttl, data)
            yield key
        pipe.execute()


def _rdb_dryrun_copy(src, pattern=None):
    """
    yields the keys it processes as it goes.
    WON'T OVERWRITE the key if it exists. It'll skip over it.
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :param pattern: str
    :return: None
    """
    matcher = rdb_regex_pattern(pattern)
    for rows in _chunks(parse_rdb(src, matcher), 500):
        for row in rows:
            if row is None:
                continue
            yield row[0]


def _rdb_backfill_copy(src, dst, pattern=None):
    """
    yields the keys it processes as it goes.
    WON'T OVERWRITE the key if it exists. It'll skip over it.
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :param pattern: str
    :return: None
    """
    matcher = rdb_regex_pattern(pattern)
    for rows in _chunks(parse_rdb(src, matcher), 500):
        # don't even bother reading the data if the key already exists in the src.
        pipe = dst.pipeline(transaction=False)
        for row in rows:
            if row is None:
                continue
            pipe.exists(row[0])
        rows = [rows[i] for i, result in enumerate(pipe.execute()) if
                not result]
        if not rows:
            continue

        pipe = dst.pipeline(transaction=False)

        for key, data, pttl in rows:
            pipe.restore(key, pttl, data)

        for i, result in enumerate(pipe.execute(raise_on_error=False)):
            if not isinstance(result, Exception):
                yield rows[i][0]
                continue

            if 'is busy' in str(result):
                continue

            raise result


def copy(src, dst, pattern=None, backfill=False):
    """
    Copy data from source to destination.
    Optionally filter the source keys by a given glob-style or regex pattern.
    Optionally only backfill keys, avoiding overwriting any pre-existing keys.
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis
    :param pattern: string
    :param backfill: bool
    :return: generator
    """
    if dst is None:
        if isinstance(src, string_types):
            return _rdb_dryrun_copy(src, pattern=pattern)
        else:
            return _dry_run_copy(src, pattern=pattern)

    if backfill:
        if isinstance(src, string_types):
            c = _rdb_backfill_copy
        else:
            c = _backfill_copy
    else:
        if isinstance(src, string_types):
            c = _rdb_clobber_copy
        else:
            c = _clobber_copy

    return c(src, dst, pattern)
