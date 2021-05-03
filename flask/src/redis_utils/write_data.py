import redis
import numpy as np
import json
import logging
from src.lmsr.maker import LMSRMarketMaker
from src.redis_utils.exceptions import ResourceNotFoundError

redis_db = redis.Redis(host='redis', port=6379, db=0)

def attempt_purchase(uid: str, portfolioId: str, market: str, quantity: list, price: float) -> (bool, float):

    if not redis_db.exists(market):
        raise ResourceNotFoundError

    success = False
    redis_db.watch(market)

    for i in range(1, 101):

        try:

            current = json.loads(redis_db.get(market))
            x0, b = current['x'], current['b']
            maker = LMSRMarketMaker(market, x0, b)
            price_true = maker.price_trade(quantity)

            # exectute trade whether or not the price is as expected
            # revert this back later if they choose to cancel or no response is sent
            current['x'] = (np.array(x0) + np.array(quantity)).tolist()
            redis_db.set(market, json.dumps(current))

            redis_db.unwatch()
            success = True
            break

        except redis.WatchError:
            logging.info('WATCH ERROR; purchase; {uid}; {portfolioId}; {market}; {i}')
            
    if success:

        if round(price, 2) == round(price_true, 2):
            return True, round(price_true, 2)

        else:
            return False, round(price_true, 2)

    else:
        raise redis.WatchError


