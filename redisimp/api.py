__all__ = ['copy']

BATCH_SIZE = 500


def read_keys(src):
    """

    :param src: redis.StrictRedis
    :yeild: array of keys
    """

    cursor = 0
    while True:
        cursor, keys = src.scan(cursor=cursor, count=BATCH_SIZE)
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


def copy(src, dst):
    """
    yeilds either the count or the keys it processes as it goes.
    :param src: redis.StrictRedis
    :param dst: redis.StrictRedis or rediscluster.StrictRedisCluster
    :param verbose: bool
    :return: None
    """
    read = read_data_and_pttl
    for keys in read_keys(src):
        pipe = dst.pipeline(transaction=False)
        for key, data, pttl in read(src, keys):
            pipe.delete(key)
            pipe.restore(key, pttl, data)
            yield key
        pipe.execute()
