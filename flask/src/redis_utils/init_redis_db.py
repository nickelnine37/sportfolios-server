import redis
import os
from src.redis_utils.arrays import toRedis, fromRedis
import numpy as np
import json

def init_redis_f():

    redis_db = redis.Redis(host='redis', port=6379, db=0)

    BASE_DIR = '/var/www'

    with open(os.path.join(BASE_DIR, 'init_data', 'teams_init.json'), 'r')  as f:
        x0s = json.loads(f.read())

    with redis_db.pipeline() as pipe:

        b0 = 4000
        bh = toRedis(b0 * np.ones(60))

        for market, x0 in x0s.items():

            # print(market)

            x0 = np.array(x0, dtype=np.float32)
            xh = np.repeat(np.array(x0, dtype=np.float32)[None, :], 60, axis=0)
            x0 = toRedis(x0)
            xh = toRedis(xh)

            pipe.hmset(market, {'x': x0, 'b': 4000})
            pipe.hmset(market + ':xhist', {'xH': xh, 'xD': xh, 'xW': xh, 'xM': xh, 'xm': xh})
            pipe.hmset(market + ':bhist', {'bH': bh, 'bD': bh, 'bW': bh, 'bM': bh, 'bm': bh} )

        print(pipe.execute())
    # print(redis.bgsave())

