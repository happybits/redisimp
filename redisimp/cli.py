# std lib
import argparse
import sys
import time
import logging
from signal import signal, SIGTERM

# 3rd party
try:
    import rediscluster
except ImportError:
    rediscluster = None
import redis
import redislite
from redis.exceptions import BusyLoadingError

# internal
from .multi import multi_copy
from .version import __version__

__all__ = ['main']

# how long to wait in between each try
REDISLITE_LOAD_WAIT_INTERVAL_SECS = 1

# how many seconds total to wait before giving up on redislite rdb loading
REDISLITE_LOAD_WAIT_TIMEOUT = 10000


def parse_args(args=None):
    """
    parse the cli args and print out help if needed.
    :return: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description='redisimp v%s - import keys from one or more'
                    ' redis instances to another' % __version__)

    parser.add_argument('--version', action='version',
                        version='redisimp %s' % __version__)

    parser.add_argument(
        '-s', '--src', type=str, required=True,
        help='comma separated list of hosts in the form of hostname:port')

    parser.add_argument(
        '-d', '--dst', type=str, required=True,
        help='the destination in the form of hostname:port')

    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='iterate through all the keys to copy, dont do anything')

    parser.add_argument(
        '-p', '--pattern', type=str, default=None,
        help='a glob-style pattern to select the keys to copy')

    parser.add_argument(
        '-v', '--verbose', action='store_true', default=False,
        help='turn on verbose output')

    parser.add_argument(
        '-b', '--backfill', action='store_true', default=False,
        help="backfill data, don't overwrite keys in "
             "destination that exist already")

    return parser.parse_args(args=args)


def resolve_host(target):
    """
    :param target: str The host:port pair or path
    :return:
    """
    target = target.strip()
    if target.startswith('redis://') or target.startswith('unix://'):
        return redis.StrictRedis.from_url(target)

    try:
        hostname, port = target.split(':')
        return redis.StrictRedis(host=hostname, port=int(port))
    except ValueError:
        start = time.time()
        while True:
            try:
                redislite.StrictRedis.start_timeout = REDISLITE_LOAD_WAIT_TIMEOUT
                conn = redislite.StrictRedis(target)
            except BusyLoadingError:
                logging.info('%s loading', target)
                elapsed = time.time() - start
                if elapsed > REDISLITE_LOAD_WAIT_TIMEOUT:
                    raise BusyLoadingError('unable to load rdb %s' % target)
                time.sleep(REDISLITE_LOAD_WAIT_INTERVAL_SECS)
                continue

            if conn.info('persistence').get('loading', 0):
                logging.warn('%s loading', target)
                time.sleep(REDISLITE_LOAD_WAIT_INTERVAL_SECS)
                elapsed = time.time() - start
                if elapsed > REDISLITE_LOAD_WAIT_TIMEOUT:
                    raise BusyLoadingError('unable to load rdb %s' % target)
                continue
            return conn


def resolve_sources(srcstring):
    for hoststring in srcstring.split(','):
        hoststring = hoststring.strip()
        if len(hoststring) < 1:
            continue
        if hoststring.startswith('rdb://'):
            yield hoststring[6:]
        elif ':' not in hoststring:
            yield hoststring
        else:
            yield resolve_host(hoststring)


def resolve_destination(dststring):
    conn = resolve_host(dststring)
    if not conn.info('cluster').get('cluster_enabled', None):
        return conn

    if not rediscluster:
        raise RuntimeError('cluster destination specified and redis-py-cluster not installed')

    host, port = dststring.split(':')
    return rediscluster.StrictRedisCluster(
        startup_nodes=[{'host': host, 'port': port}], max_connections=1000)


# pylint: disable=unused-argument
def sigterm_handler(signum, frame):
    raise SystemExit('--- Caught SIGTERM; Attempting to quit gracefully ---')


def process(src, dst, verbose=False, pattern=None,
            backfill=False, dryrun=False, out=None):
    if out is None:
        out = sys.stdout
    dst = None if dryrun else resolve_destination(dst)
    processed = 0
    src_list = [s for s in resolve_sources(src)]

    for key in multi_copy(src_list, dst, pattern=pattern, backfill=backfill):
        processed += 1
        if verbose:
            print(key)

        if not verbose and processed % 1000 == 0:
            out.write('\r%d' % processed)
            out.flush()

    out.write('\n\nprocessed %s keys\n' % processed)
    out.flush()
    for src in src_list:
        del src

    # make sure to save data if it is redislite destnation
    if isinstance(dst, redislite.StrictRedis):
        try:
            dst.bgsave()
        except redis.ResponseError:
            pass

    del dst


def main(args=None, out=None):
    signal(SIGTERM, sigterm_handler)
    args = parse_args(args=args)

    process(src=args.src, dst=args.dst,
            verbose=args.verbose,
            pattern=args.pattern,
            backfill=args.backfill,
            dryrun=args.dry_run,
            out=out)
