import redis
import numpy as np
import json
import os
import logging
from src.redis_utils.arrays import toRedis, fromRedis
from src.lmsr.maker import LMSRMarketMaker
from src.redis_utils.exceptions import ResourceNotFoundError

redis_db = redis.Redis(host='redis', port=6379, db=0)


def get_spot_back_prices(markets: list) -> dict:

    with redis_db.pipeline() as pipe:

        for market in markets:
            pipe.get(market)

        results = pipe.execute()

    out = {}

    for market, x0 in zip(markets, results):

        if x0 is None:
            out[market] = None
        else:
            xs = json.loads(x0)
            x, b = xs['x'], xs['b']
            out[market] = 10 * LMSRMarketMaker(market, x, b).back_spot_value()

    return out





def get_spot_quantity_values(markets: list, quantities: list):

    with redis_db.pipeline() as pipe:

        for market in markets:
            pipe.get(market)

        results = pipe.execute()

    out = {}

    for market, x0, q in zip(markets, results, quantities):

        if market == 'cash':
            out['cash'] = q 
        elif x0 is None:
            out[market] = None
        else:
            xs = json.loads(x0)
            x, b = xs['x'], xs['b']
            out[market] = LMSRMarketMaker(market, x, b).spot_value(q)

    return out


def get_latest_quantities(market: str) -> dict:

    result = redis_db.get(market)

    if result is None:
        raise ResourceNotFoundError

    return json.loads(result)



def get_multiple_latest_quantities(markets: list):

    with redis_db.pipeline() as pipe:

        for market in markets:

            pipe.get(market)

        results = pipe.execute()

    return {market: json.loads(result) for market, result in zip(markets, results)}


def get_multiple_historical_quantities(markets: list):

    with redis_db.pipeline() as pipe:

        for market in markets:

            pipe.get(market + ':xhist')
            pipe.get(market + ':bhist')
        
        results = pipe.execute()

    out = {}

    for i, market in enumerate(markets):
        out[market] = {'xhist': json.loads(results[2 * i]), 'bhist': json.loads(results[2 * i + 1])}
    
    return out


def get_historical_quantities(market: str) -> dict:

    with redis_db.pipeline() as pipe:

        pipe.get(market + ':xhist')
        pipe.get(market + ':bhist')
        xhist, bhist = pipe.execute()

    if xhist is None:
        raise ResourceNotFoundError

    return {'xhist': json.loads(xhist), 'bhist': json.loads(bhist)}


