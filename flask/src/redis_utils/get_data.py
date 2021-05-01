import redis
import numpy as np
import json
import os
import logging
from src.redis_utils.arrays import toRedis, fromRedis
from src.lmsr.maker import LMSRMarketMaker

redis_db = redis.Redis(host='redis', port=6379, db=0)


def get_spot_prices(markets: list):

    with redis_db.pipeline() as pipe:

        for market in markets:
            pipe.hgetall(market)

        results = zip(markets, pipe.execute())

    return {market: float(LMSRMarketMaker(market, fromRedis(info[b'x']), float(info[b'b'])).back_spot_value()) if info != {} else None for market, info in results}



def get_spot_quantities(market: str):

    result = redis_db.hgetall(market)

    if result == {}:
        return {'b': None, 'x': None}

    return {'b': float(result[b'b']), 'x': fromRedis(result[b'x']).tolist()}