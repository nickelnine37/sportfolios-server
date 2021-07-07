import sys

sys.path.append('/var/www/src')

import os
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

from redis_holdings import RedisJobs
from firebase_markets import FirebaseMarketJobs


if __name__ == '__main__':

    BASE_DIR='/var/www'

    logging.basicConfig(format='%(asctime)s %(threadName)s %(levelname)s %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S',
                        filename=os.path.join(BASE_DIR, 'logs', 'jobs.log'),
                        level=logging.INFO)

    logging.info(str(sys.path))

    scheduler = BlockingScheduler()
    redis_jobs = RedisJobs()
    firebase_market_jobs = FirebaseMarketJobs()

    now = datetime.now()

    scheduler.add_job(redis_jobs.update_all_historical_holdings, trigger='interval', minutes=2, start_date=now, next_run_time=now)
    scheduler.add_job(firebase_market_jobs.update_all_markets, trigger='interval', minutes=60, start_date=now, next_run_time=now)

    scheduler.start()
