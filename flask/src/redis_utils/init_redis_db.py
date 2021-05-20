import time

import redis
import os
from src.redis_utils.arrays import toRedis, fromRedis
import numpy as np
import json

def init_redis_f():

    redis_db = redis.Redis(host='redis', port=6379, db=0)

    BASE_DIR = '/var/www'

    with open(os.path.join(BASE_DIR, 'init_data', 'xhist.json'), 'r')  as f:
        x0s = json.loads(f.read())

    with open(os.path.join(BASE_DIR, 'init_data', 'bhist.json'), 'r')  as f:
        b0s = json.loads(f.read())

    with redis_db.pipeline() as pipe:

        # t = int(time.time())
        # hts = [t - 60 * 2 * i for i in range(60)]             # 2h
        # dts = [t - i * 60 * 24 for i in range(60)]            # 1 day
        # wts = [t - i * 60 * 24 * 7 for i in range(60)]        # 7 days
        # mts = [t - i * 60 * 24 * 7 * 4 for i in range(60)]    # 28 days
        # Mts = [t - i * 60 * 24 * 7 * 4 for i in range(60)]    # initially 28 days

        for market in x0s.keys():

            xhist = x0s[market]
            bhist = b0s[market]

            pipe.set(market, json.dumps({'x': list(xhist['h'].values())[0], 'b': list(bhist['h'].values())[0]}))
            pipe.set(market + ':xhist', json.dumps(xhist))
            pipe.set(market + ':bhist', json.dumps(bhist))

        pipe.execute()


