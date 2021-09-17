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

if __name__ == '__main__':

    time.sleep(30)

    BASE_DIR='/var/www'

    logging.basicConfig(format='%(asctime)s %(threadName)s %(levelname)s %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S',
                        filename=os.path.join(BASE_DIR, 'logs', 'jobs.log'),
                        level=logging.INFO)


    scheduler = BlockingScheduler()

    t0 = 44658

    if True:

        redis_jobs = RedisJobs(t=t0, max_interval=672, add_noise=True)
        firebase_market_jobs = FirebaseMarketJobs(t=t0)
        firebase_portfolio_jobs = FirebasePortfoliosJobs(t=t0)
        trading_bot = TradingBot(t=t0)

        now = datetime.now()
        scheduler.add_job(redis_jobs.update_all_historical_holdings,     trigger='interval', minutes=2, start_date=now, next_run_time=now)
        scheduler.add_job(firebase_market_jobs.update_all_markets,       trigger='interval', minutes=2, start_date=now, next_run_time=now, jitter=120)
        scheduler.add_job(firebase_portfolio_jobs.update_all_portfolios, trigger='interval', minutes=2, start_date=now, next_run_time=now, jitter=120)
        scheduler.add_job(trading_bot.trade,                             trigger='interval', minutes=2, start_date=now, next_run_time=now, jitter=20)

    # disable apsscheduler info logs
    logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
    scheduler.start()

