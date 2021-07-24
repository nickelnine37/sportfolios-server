from itertools import groupby
from os import scandir
import time
from scheduler_utils import Timer, RedisExtractor
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import json
import numpy as np

## TODO: Under the current implementation, because the holdings vectors are not updated at exactly
## the same time as the time log, they may be out of sync when a client makes a request. This will
## only cause an error if they are not the same length. They should pretty much always be the same
## length, as long as we initialise the holdings to have length 60. This is still not great though...


class RedisJobs:
    """
    The purpose of this class is to provide functionality for running the regular job of updating the 
    historical holding vectors. Redis holds to JSON objects for each market. The current holding and
    the historical holding: 

    'market1':      {'x': [1, 2, 3, 4... ], 'b': 100}
    'market1:hist'  {'x': {'h': [[1, 2, 3, 4, ...], 
                                 [2, 3, 4, 5, ...], ...], 
                           'd': [[...], [...], ...]}, ...
                     'b': {'h': [...], ....}}

    At regular intervals, the current holding vector needs to be copied into the historical holding 
    vectors. That is what this class does. 

    """

    def __init__(self, t: int=0, max_interval: int=672, add_noise: bool=False):

        self.redis_extractor = RedisExtractor()
        self.t = t
        self.max_interval = max_interval
        self.max_interval_needs_doubling = False
        self.add_noise = add_noise


    def get_timeframes(self):
        """
        For a particular number of minutes, t, return the time horizons that we are interested in
        """

        out = []

        for tf, interval in zip(['h', 'd', 'w', 'm', 'M'], [2, 24, 168, 672, self.max_interval]):
            if self.t % interval == 0:
                out.append(tf)

        return out

    def update_all_historical_holdings(self):
        """
        Go through all team and player markets and update the relevant holding vectors
        """

        try:

            timeframes = self.get_timeframes()

            with Timer() as timer:
            
                with open('/var/www/data/teams.txt', 'r') as f:
                    teams = f.read().splitlines()
                
                redis_time, python_time = self.update_historical_holdings(teams, timeframes, team=True)
            
                with open('/var/www/data/players.txt', 'r') as f:
                    all_players = f.read().splitlines()

                # split on league, just so we maintain a reasonable number at a time
                for group, players in groupby(all_players, key=lambda player: player.split(':')[1]):
                    rtime, ptime = self.update_historical_holdings(list(players), timeframes, team=False)
                    redis_time += rtime; python_time += ptime
                    
            self.update_time(timeframes)

            if self.max_interval_needs_doubling:
                self.max_interval *= 2
                self.max_interval_needs_doubling = False

            logging.info(f'REDIS HOLDINGS t = {self.t}. Completed update for timeframes {timeframes}. time: {timer.t:.4f}s \t redis time: {redis_time:.4f}s \t python time: {python_time:.4f}s')
            
            self.t += 2

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as E:
            # TODO: SEND EMAIL ALERT
            logging.error(str(E))

    def update_historical_holdings(self, markets: list, timeframes: list, team: bool):
        """
        Given a list of markets, and a particular timeframe, grab the current holdings and 
        update the relevant historical holdings. Return the time taken for redis read and 
        write operations, and the time taken for python operations
        """

        with Timer() as redis1_timer:
            all_current, all_hist = self.redis_extractor.get_current_and_historical_holdings(markets)

        with Timer() as python_timer:

            hist_new = {}

            for market, current, hist in zip(markets, all_current, all_hist):

                if (current is None) or (hist is None):
                    logging.error(f'Cannot update hist {timeframes} holdings for {market}. Redis returned None')

                else:
                    
                    if self.add_noise:
                        if team:
                            current['x'] = np.round(np.array(current['x']) + np.random.normal(loc=np.linspace(0, current['b'] / 10000 , len(current['x'])), scale= current['b'] / 100), 2).tolist()
                        else:
                            current['N'] = float(np.round(current['N'] + np.random.normal(loc=current['b'] / 10000, scale=current['b'] / 100), 2))

                    for timeframe in timeframes:
                        hist = self.get_new_historical_holdings(timeframe, current, hist, team)

                    hist_new[market] = hist

        with Timer() as redis2_timer:
            self.redis_extractor.write_historical_holdings(hist_new)
            if self.add_noise:
                self.redis_extractor.write_current_holdings(markets, all_current)


        
        return redis1_timer.t + redis2_timer.t, python_timer.t


    def get_new_historical_holdings(self, timeframe: str, current: str, hist: dict, team: bool):
        """
        Given a timeframe, a current holdings dictionary and a historical holdings dictionary; make the necessary changes to
        the historical holdings dictionary so that the object is updated. For h, d, w, and m, this means appending
        the newest holding vector to the historical list and deleting the oldest (as long as it's length is greater than 60). 
        For M, this means appending the latest and deleting every other holding vector if the length is greater than 120. 
        """

        if team:
            k = 'x'
        else:
            k = 'N'

        hist[k][timeframe].append(current[k])

        
        hist['b'][timeframe].append(current['b'])

        if timeframe == 'M': 
            if len(hist['b'][timeframe]) > 120:
                del hist[k][timeframe][1::2]
                del hist['b'][timeframe][1::2]
                
        else:
            if len(hist['b'][timeframe]) > 60:
                del hist[k][timeframe][0]
                del hist['b'][timeframe][0]

        return hist


    def update_time(self, timeframes: list):
        """
        Make relevant entries into time log JSON
        """

        hist_times = self.redis_extractor.get_time()
        t = int(time.time())

        for timeframe in timeframes:

            hist_times[timeframe].append(t)

            if timeframe == 'M':        
                if len(hist_times[timeframe]) > 120:
                    del hist_times[timeframe][1::2]
                    self.max_interval_needs_doubling = True

            else:
                if len(hist_times[timeframe]) > 60:
                    del hist_times[timeframe][0]

        self.redis_extractor.set_time(hist_times)
