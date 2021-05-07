import time

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

        t = int(time.time())
        hts = [t - 60 * 2 * i for i in range(60)]             # 2h
        dts = [t - i * 60 * 24 for i in range(60)]            # 1 day
        wts = [t - i * 60 * 24 * 7 for i in range(60)]        # 7 days
        mts = [t - i * 60 * 24 * 7 * 4 for i in range(60)]    # 28 days
        Mts = [t - i * 60 * 24 * 7 * 4 for i in range(60)]    # initially 28 days

        for market, data in x0s.items():

            x0 = data['x0']
            b0 = data['b']

            pipe.set(market, json.dumps({'x': x0, 'b': b0}))

            pipe.set(market + ':xhist', json.dumps({'xh': {t: x0 for t in hts},
                                                    'xd': {t: x0 for t in dts},
                                                    'xw': {t: x0 for t in wts},
                                                    'xm': {t: x0 for t in mts},
                                                    'xM': {t: x0 for t in Mts}}))

            pipe.set(market + ':bhist', json.dumps({'bh': {t: b0 for t in hts},
                                                    'bd': {t: b0 for t in dts},
                                                    'bw': {t: b0 for t in wts},
                                                    'bm': {t: b0 for t in mts},
                                                    'bM': {t: b0 for t in Mts}}))

        pipe.execute()


