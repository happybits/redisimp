__all__ = ['copy']


def read_keys(src, batch_size=500, match=None):
    """

    :param src: redis.StrictRedis
    :yeild: array of keys
    """

    cursor = 0
    while True:
        cursor, keys = src.scan(cursor=cursor, count=batch_size, match=match)
        if keys:
            yield keys

        if cursor == 0:
            break


def read_data_and_pttl(src, keys):
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


def copy(src, dst, match=None):
    """
    yeilds either the count or the keys it processes as it goes.
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :param verbose: bool
    :return: None
    """
    read = read_data_and_pttl
    for keys in read_keys(src, match=match):
        pipe = dst.pipeline(transaction=False)
        for key, data, pttl in read(src, keys):
            pipe.delete(key)
            pipe.restore(key, pttl, data)
            yield key
        pipe.execute()
