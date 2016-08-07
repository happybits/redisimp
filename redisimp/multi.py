from .api import copy

__all__ = ['multi_copy']


def multi_copy(srclist, dst, pattern=None, backfill=False):
    """
    Same semantics as copy in the api, but copy from a list of sources.
    :param pattern:
    :param backfill:
    :param srclist:
    :param dst:
    :param worker_count:
    :return:
    """
    for src in srclist:
        for key in copy(src, dst, pattern=pattern, backfill=backfill):
            yield key
