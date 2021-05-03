
import redis
from rq_scheduler import Scheduler
from datetime import timedelta
from src.redis_utils.write_data import increment_quantity
import logging
import uuid

redis_q = redis.Redis(host='redis', port=6379, db=0)
scheduler = Scheduler(connection=redis_q)


# class Cancellation:

#     def __init__(self, uid: str, market: str, portfolioId: str, quantity: list=None, cancelId: str=None, delay: float=60):

#         self.uid = uid
#         self.market = market
#         self.portfolioId = portfolioId
#         self.quantity = quantity

#         if cancelId is None:
#             self.cancelId = uuid.uuid4().hex
#         else:
#             self.cancelId = cancelId

#         self.delay = delay

#     def schedule(self):
        
#         job = scheduler.enqueue_in(timedelta(seconds=self.delay), increment_quantity, uid, portfolioId, market, quantity=[-q for q in quantity])


def schedule_cancellation(uid: str, portfolioId: str, market: str, quantity: list, delay: float=60):
    return scheduler.enqueue_in(timedelta(seconds=delay), increment_quantity, uid, portfolioId, market, quantity=[-q for q in quantity])


def cancel_scheduled_cancellation(job_id: str) -> bool:

    if job_id in scheduler:
        scheduler.cancel(job_id)
        return True
    else:
        return False

