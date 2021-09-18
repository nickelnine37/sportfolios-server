import json
import sys
sys.path.append('/var/www/src')

import os
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

from redis_holdings import RedisJobs
from firebase_markets import FirebaseMarketJobs
from firebase_portfolios import FirebasePortfoliosJobs
from trading_bot import TradingBot

import time
import redis

from typing import List

redis_db = redis.Redis(host='redis', port=6379, db=0)


class JobScheduler:

    def __init__(self):

        self.redis_jobs = RedisJobs()
        self.firebase_market_jobs = FirebaseMarketJobs()
        self.firebase_portfolio_jobs = FirebasePortfoliosJobs()
        self.trading_bot = TradingBot(trade_noise=True)


    def get_jobs(self, t: int):
        """
        Return a list of functions that should be run for this specific time
        """

        jobs = []

        # run redis jobs every two minutes
        jobs.append(self.redis_jobs.update_all_historical_holdings)

        # run portfolio jobs on the hour
        if t % 60 == 0:
            jobs.append(self.firebase_portfolio_jobs.update_all_portfolios)

        # run firebasse market jobs on the half hour
        if t % 60 == 30:
            jobs.append(self.firebase_market_jobs.update_all_markets)

        # run trading bot on minute 2 every 10 mins
        if t % 10 == 2:
            jobs.append(self.trading_bot.trade)

        return jobs


    def run_jobs(self):

        t = int(redis_db.get('t'))

        for job in self.get_jobs(t):
            # must catch errors here so time is incremented
            try:
                job(t)
            except Exception as E:
                logging.error(str(E))

        redis_db.incr('t', amount=2)


if __name__ == '__main__':

    time.sleep(30)

    BASE_DIR='/var/www'

    logging.basicConfig(format='%(asctime)s %(threadName)s %(levelname)s %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S',
                        filename=os.path.join(BASE_DIR, 'logs', 'jobs.log'),
                        level=logging.INFO)

    scheduler = BlockingScheduler()
    jobs = JobScheduler()
    now = datetime.now()

    scheduler.add_job(jobs.run_jobs,     trigger='interval', minutes=2, start_date=now, next_run_time=now)

    # disable apsscheduler info logs
    logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
    scheduler.start()

