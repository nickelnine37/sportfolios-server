import redis
import numpy as np
import json
import os
import logging
from src.redis_utils.arrays import toRedis, fromRedis
from src.lmsr.maker import LMSRMarketMaker
from src.redis_utils.exceptions import ResourceNotFoundError

redis_db = redis.Redis(host='redis', port=6379, db=0)


def get_spot_prices(markets: list) -> dict:

    with redis_db.pipeline() as pipe:

        for market in markets:
            pipe.get(market)

        results = pipe.execute()

    out = {}

    for market, spot_quant in zip(markets, results):

        if spot_quant is None:
            out[market] = None
        else:
            spot_quant = json.loads(spot_quant)
            out[market] = 10 * LMSRMarketMaker(market, spot_quant['x'], spot_quant['b']).back_spot_value()

    return out


def get_spot_quantities(market: str) -> dict:

    result = redis_db.get(market)

    if result is None:
        raise ResourceNotFoundError

    return json.loads(result)


def get_historical_quantities(market: str) -> dict:

    with redis_db.pipeline() as pipe:

        pipe.get(market + ':xhist')
        pipe.get(market + ':bhist')
        xhist, bhist = pipe.execute()

    if xhist is None:
        raise ResourceNotFoundError

    return {'xhist': json.loads(xhist), 'bhist': json.loads(bhist)}


