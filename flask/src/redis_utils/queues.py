
import redis
from rq_scheduler import Scheduler
from datetime import timedelta, datetime
from src.redis_utils.write_data import increment_quantity
from src.redis_utils.exceptions import ResourceNotFoundError
import logging
import uuid
import json

redis_q = redis.Redis(host='redis', port=6379, db=0)
redis_db = redis.Redis(host='redis', port=6379, db=0)
scheduler = Scheduler(connection=redis_q)



def schedule_cancellation(uid: str, portfolioId: str, market: str, quantity: list, price: float):

    # schedule cancellation 
    cancelId = uuid.uuid4().hex
    job_id = scheduler.enqueue_in(timedelta(seconds=60), increment_quantity, uid, portfolioId, market, quantity=[-q for q in quantity]).id

    # write cancellation into database, which expires in 60s
    redis_db.setex(cancelId, timedelta(seconds=60), value='+'.join([uid, market, portfolioId, str(quantity), str(price), job_id]))
    logging.info(f'CANCELLATION JOB WRITTEN. EXPIRING IN 60s; schedule_cancellation;{cancelId}:{job_id};{uid};{portfolioId};{market}_{quantity}_{price}')

    return cancelId


def cancel_scheduled_cancellation(cancelId: str, uid: str, portfolioId: str, market: str, quantity: list, price: float):

    if not redis_db.exists(cancelId):
        logging.warning(f'CANCELLATION DOES NOT EXIST; cancel_scheduled_cancellation; {cancelId}')
        raise ResourceNotFoundError

    uid_, market_, portfolioId_, quantity_, price_, job_id = redis_db.get(cancelId).decode().split('+')

    if all([uid_ == uid, market_ == market, portfolioId_ == portfolioId, json.loads(quantity_) == quantity, float(price_) == price]):

        if job_id in scheduler:
            scheduler.cancel(job_id)
            logging.info(f'CANCELLING CANCELLATION JOB; cancel_scheduled_cancellation;{cancelId}:{job_id};{uid};{portfolioId};{market}_{quantity}_{price}')
            return
        else:
            logging.info('Job ID not found in scheduler')
            raise ResourceNotFoundError

    else:
        logging.info(f'Bad form data: {[uid_ == uid, market_ == market, portfolioId_ == portfolioId, json.loads(quantity_) == quantity, float(price_) == price]}')
        raise ValueError


def execute_cancellation_early(cancelId: str, uid: str, portfolioId: str, market: str, quantity: list, price: float):

    if not redis_db.exists(cancelId):
        logging.warning(f'CANCELLATION DOES NOT EXIST; execute_cancellation_early; {cancelId}')
        raise ResourceNotFoundError

    uid_, market_, portfolioId_, quantity_, price_, job_id = redis_db.get(cancelId).decode().split('+')

    if all([uid_ == uid, market_ == market, portfolioId_ == portfolioId, json.loads(quantity_) == quantity, float(price_) == price]):

        if job_id in scheduler:
            scheduler.cancel(job_id)
            increment_quantity(uid, portfolioId, market, quantity=[-q for q in quantity])
            logging.info(f'RUNNING CANCELLATION JOB EARLY; execute_cancellation_early;{cancelId}:{job_id};{uid};{portfolioId};{market}_{quantity}_{price}')
            return
        else:
            logging.info('Job ID not found in scheduler')
            raise ResourceNotFoundError

    else:
        logging.info(f'Bad form data: {[uid_ == uid, market_ == market, portfolioId_ == portfolioId, json.loads(quantity_) == quantity, float(price_) == price]}')
        raise ValueError


