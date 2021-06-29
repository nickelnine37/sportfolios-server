from datetime import datetime
import os
import time
import json
import redis
import numpy as np
from itertools import groupby
import logging
from apscheduler.schedulers.blocking import BlockingScheduler

BASE_DIR='/var/www'

logging.basicConfig(format='%(asctime)s %(threadName)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    filename=os.path.join(BASE_DIR, 'logs', 'flask.log'),
                    level=logging.INFO)

redis_db = redis.Redis(host='redis', port=6379, db=0)



def get_current_and_hist(markets: list):
    """
    Get the raw (string) curent and historical holdings for a list of markets
    """

    with redis_db.pipeline() as pipe:

        for market in markets:

            pipe.get(market)
            pipe.get(market + ':hist')

        results = pipe.execute()

    return results[::2], results[1::2]


def send_new_hist_ro_redis(all_hist_new: dict):
    """
    Given a new dictionary mapping string market to historical holdings, send this to redis
    """

    with redis_db.pipeline() as pipe:

        for team, hist_new in all_hist_new.items():
            pipe.set(team + ':hist', json.dumps(hist_new))

        pipe.execute()


def update_hist(markets: list, timeframe: str):
    """
    Given a list of markets, and a particular timeframe, grab the current holdings and 
    update the relevant historical holdings. 
    """

    all_current, all_hist = get_current_and_hist(markets)

    hist_new = {}

    for market, current, hist in zip(markets, all_current, all_hist):

        if current is not None:

            current = json.loads(current)
            hist = json.loads(hist)

            hist['x'][timeframe].append(current['x'])
            hist['b'][timeframe].append(current['b'])

            if timeframe == 'M': 
                # delete every other time starting from second time
                if len(hist['b'][timeframe]) == 121:
                    del hist['x'][timeframe][1::2]
                    del hist['b'][timeframe][1::2]
            
            else:
                if len(hist['b'][timeframe]) > 60:
                    del hist['x'][timeframe][0]
                    del hist['b'][timeframe][0]

            hist_new[market] = hist
        
        else:
            logging.error(f'MARKET {market} IS MISSING?')

    send_new_hist_ro_redis(hist_new)


def update_time(timeframe: str):
    """
    Make relevant entry into time log
    """

    hist_times = json.loads(redis_db.get('time'))
    hist_times[timeframe].append(int(time.time()))

    if timeframe == 'M':        
        # delete every other time starting from second time
        if len(hist_times[timeframe]) == 121:
            del hist_times[timeframe][1::2]

    else:
        # delete first time
        if len(hist_times[timeframe]) > 60:
            del hist_times[timeframe][0]

    redis_db.set('time', json.dumps(hist_times))



def update_markets(timeframe: str):
    """
    Update the market for a particular timeframe
    """

    with open('/var/www/data/teams.txt', 'r') as f:
        teams = f.read().splitlines()
    
    update_hist(teams, timeframe)

    with open('/var/www/data/players.txt', 'r') as f:
        all_players = f.read().splitlines()

    # split on league, just so we maintain a reasonable number at a time
    for players in groupby(all_players, key=lambda player: player.split(':')[1]):
        update_hist(players, timeframe)

    update_time(timeframe)


def update_markets_h():
    update_markets('h')

def update_markets_d():
    update_markets('d')

def update_markets_w():
    update_markets('w')

def update_markets_m():
    update_markets('m')

def update_markets_M():
    update_markets('M')


def update_portfolios():
    pass

def update_market_documents():
    pass



scheduler = BlockingScheduler()


scheduler.add_job(update_markets_h, 'interval', minutes=2, start_date=datetime.now())
scheduler.add_job(update_markets_d, 'interval', minutes=24, start_date=datetime.now(), jitter=60)
scheduler.add_job(update_markets_w, 'interval', minutes=168, start_date=datetime.now(), jitter=120)
scheduler.add_job(update_markets_m, 'interval', minutes=672, start_date=datetime.now())
scheduler.add_job(update_markets_M, 'interval', seconds=672, start_date=datetime.now())


print('We should be stating now.....')

scheduler.start()