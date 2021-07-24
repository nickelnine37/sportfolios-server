import uuid
import redis
import numpy as np
import orjson
import logging
from src.lmsr.classic import LMSRMarketMaker
from src.lmsr.long_short import LongShortMarketMaker
from src.redis_utils.exceptions import ResourceNotFoundError
import time
from rq_scheduler import Scheduler
from datetime import timedelta

redis_db = redis.Redis(host='redis', port=6379, db=0)
scheduler = Scheduler(connection=redis_db)


def make_purchase(purchase_form: dict) -> float:
    """
    Execute a trade given a valid purchase form If the trade is executed successfully, 
    return the settled price. If the trade cannot be processed due to a watch error more 
    than 100 times, raise a watch error. 
    """

    market, quantity, team, long = purchase_form['market'], purchase_form['quantity'], purchase_form['team'], purchase_form['long']

    if not redis_db.exists(market):
        raise ResourceNotFoundError

    success = False
    redis_db.watch(market)

    for i in range(1, 101):

        try:
            current = orjson.loads(redis_db.get(market))

            if team:
                maker = LMSRMarketMaker(market, current['x'], current['b'])
                price = maker.price_trade(quantity)
                current['x'] = (np.array(current['x']) + np.array(quantity)).tolist()
            else:
                maker = LongShortMarketMaker(market, current['N'], current['b'])
                price = maker.price_trade(quantity, long)
                current['N'] += quantity * (-1) ** (~long)
            
            redis_db.set(market, orjson.dumps(current))
            redis_db.unwatch()
            success = True
            break

        except redis.WatchError:
            logging.warning(f'WATCH ERROR; make_purchase; {market}; {i}')
            time.sleep(0.01)

    if success:
        return price

    else:
        raise redis.WatchError


def undo_purchase(purchase_form: dict):
    """
    For a given purchase form, undo the associated purchase
    """

    market, quantity, team, long = purchase_form['market'], purchase_form['quantity'], purchase_form['team'], purchase_form['long']

    if not redis_db.exists(market):
        raise ResourceNotFoundError

    success = False
    redis_db.watch(market)

    for i in range(1, 201):

        try:
            current = orjson.loads(redis_db.get(market))

            if team:
                current['x'] = (np.array(current['x']) - np.array(quantity)).tolist()
            else:
                current['N'] -= quantity * (-1) ** (~long)
            
            redis_db.set(market, orjson.dumps(current))
            redis_db.unwatch()
            success = True
            break

        except redis.WatchError:
            logging.warning(f'WATCH ERROR; undo_purchase; {market}; {i}')
            time.sleep(0.01)

    if success:
        return

    else:
        logging.error(f'WATCH ERROR; Was not able to undo purchase; {quantity}; {market}')
        raise redis.WatchError


def schedule_undo_purchase(purchase_form: dict):
    """
    Schedule for the puchase associated with purchase_form to be cancelled in 60s. Return the 
    cancelID, which is used to store information about the purchase in redis. This entry also
    expires in 60s. 
    """

    cancelId = uuid.uuid4().hex
    purchase_form['job_id'] = scheduler.enqueue_in(timedelta(seconds=60), undo_purchase, purchase_form).id
    redis_db.setex(cancelId, timedelta(seconds=60), value=orjson.dumps(purchase_form))

    return cancelId


def cancel_undo_scheduled_purchase(old_purchase_form: dict):
    """
    Run this function when the user decides they do in fact agree with the new price, 
    therefore there is no need to undo their original purchase
    """

    job_id = old_purchase_form['job_id']

    if job_id in scheduler:
        scheduler.cancel(job_id)

    else:
        logging.info('Job ID not found in scheduler')
        raise ResourceNotFoundError


def undo_scheudlued_purchase_now(old_purchase_form: dict):
    """
    Run this function when the user decides they do in fact agree with the new price, 
    therefore there is no need to undo their original purchase
    """

    job_id = old_purchase_form['job_id']

    if job_id in scheduler:
        scheduler.cancel(job_id)
        undo_purchase(old_purchase_form)
        
    else:
        logging.info('Job ID not found in scheduler')
        raise ResourceNotFoundError