# stdlib
from threading import Thread, Event
from Queue import Queue, Empty
import time

# internal
from .api import copy

__all__ = ['multi_copy']

WORKER_MAX = 10


def run(src_queue, channel, shutdown, dst, match=None):
    """
    A wrapper for the thread worker.

    :param src_queue: Queue A queue of StrictRedis type objects we copy from.
    :param channel: Queue push each key we copy back to the manager to inform.
    :param shutdown: Event If something goes wrong, the main thread can abort.
    :param dst: redis.StrictRedis Where we put all the data we are copying.
    :return: None
    """
    while True:
        try:
            src = src_queue.get(block=False)
        except Empty:
            return
        for key in copy(src, dst, match=match):
            channel.put(key)
            if shutdown.is_set():
                return


def _calc_worker_count(src_ct, worker_count):
    if worker_count is None or worker_count > src_ct:
        worker_count = src_ct

    if worker_count > WORKER_MAX:
        worker_count = WORKER_MAX
    return worker_count


def _create_worker(src_queue, channel, shutdown, dst, match=None):
    return Thread(target=run, kwargs={
        'src_queue': src_queue,
        'channel': channel,
        'shutdown': shutdown,
        'dst': dst,
        'match': match
    })


def _create_workers(src_queue, channel, shutdown, dst, workers, match=None):
    return [_create_worker(src_queue, channel, shutdown, dst, match=match)
            for _ in range(workers)]


def _create_src_queue(srclist):
    src_queue = Queue()
    for src in srclist:
        src_queue.put(src)
    return src_queue


def multi_copy(srclist, dst, worker_count=None, match=None):
    """
    Same semantics as copy in the api, but copy from a list of sources.
    Manages a pool of worker threads.

    :param srclist:
    :param dst:
    :param worker_count:
    :return:
    """

    src_ct = len(srclist)
    worker_count = _calc_worker_count(src_ct, worker_count)

    if worker_count < 2:
        for src in srclist:
            for key in copy(src, dst, match=match):
                yield key
        return

    src_queue = _create_src_queue(srclist)
    channel = Queue()
    shutdown = Event()

    threads = _create_workers(src_queue, channel, shutdown, dst, worker_count, match=match)
    try:

        for t in threads:
            t.start()

        backoff = 0
        while True:
            try:
                yield channel.get(block=False)
                backoff = 0
            except Empty:
                if not any([t.isAlive() for t in threads]):
                    break
                else:
                    if backoff < 1:
                        backoff += 0.005
                    time.sleep(backoff)
                    continue

    except (KeyboardInterrupt, SystemExit):
        shutdown.set()

    for t in threads:
        t.join()

    while not channel.empty():
        yield channel.get(block=False)
