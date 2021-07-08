from apscheduler.schedulers.blocking import BlockingScheduler
from scheduler_utils import Timer, RedisExtractor, firebase
from lmsr import maker
import logging
import numpy as np
from concurrent.futures import ThreadPoolExecutor


class FirebasePortfoliosJobs:

    def __init__(self):
        
        self.redis_extractor = RedisExtractor()
        self.t = 0
        self.saved_markets = {}

        self.timeframes = ['d', 'w', 'm', 'M']

        times = self.redis_extractor.get_time()
        self.times = {tf: times[tf][0] for tf in self.timeframes}

        self.cpu_time = 0
        self.redis_time = 0

    
    def reset_compute_time(self):
        self.cpu_time = 0
        self.redis_time = 0


    def add_to_saved_markets(self, markets: list):
        """
        For a list of markets, go to redis and fetch the current and historical holdings. Then iterate through this
        and add the following information to self.saved_markets:

            {'market': 
                {'current': {'x': [current_x], 'b': current_b}, 
                 'd':       {'x': [d_x],       'b': d_b}, ... }}

        Only for markets that are not already in self.saved_markets
        """

        # only fetch markets we have not already fetched
        markets = [market for market in markets if market not in self.saved_markets]

        if len(markets) == 0:
            return

        with Timer() as redis_timer:
            currents, historicals = self.redis_extractor.get_current_and_historical_holdings(markets)
        
        self.redis_time += redis_timer.t

        with Timer() as cpu_timer:

            for market, current, historical in zip(markets, currents, historicals): 

                if (current is None) or (historical is None):
                    logging.error(f'Market {market} not found in Redis')
                    continue

                self.saved_markets[market] = {'current': current}

                for timeframe in self.timeframes:

                    self.saved_markets[market][timeframe] = {'x': historical['x'][timeframe][0], 'b': historical['b'][timeframe][0]}

        self.cpu_time += cpu_timer.t


    def get_document_update(self, quantities: dict):
        """
        For a given dictionary of quantities held by the user, calulate the returns at each period. 
        quantities should be indexed by timeframe, and look something like this:
            {'current': {'market1': [1, 2, 3], 
                         'market2': [1, 2, 3], }
             'd':       {'market1': [2, 3, 4], }, 
            }
        Reutnrs something like this: {'returns_h': 0.1, 'returns_w': 0.2, 'returns_m': 0.3, 'returns_M': 0.4, 'current_value': 510 }
        """

        with Timer() as cpu_timer:

            values = {}

            for timeframe in ['current'] + self.timeframes[:-1]:

                value = 0

                cpu_timer.pause()
                self.add_to_saved_markets(quantities[timeframe].keys())
                cpu_timer.resume()
                
                for market in quantities[timeframe]:
                    
                    # it might not be, if it's missing from redis, we'll just ignore??
                    if market in self.saved_markets:

                        x, b = self.saved_markets[market][timeframe]['x'], self.saved_markets[market][timeframe]['b']
                        value += maker.LMSRMarketMaker(market, x, b).spot_value(quantities[timeframe][market])

                values[timeframe] = value

            values['M'] = 500

            doc = {}

            for timeframe in self.timeframes:
                doc[f'returns_{timeframe}'] = values['current'] / values[timeframe] - 1
            
            doc['current_value'] = values['current']

        self.cpu_time += cpu_timer.t

        return doc


    def get_quantities_dict(self, portfolio: dict):
        """
        For a given portfolio, return the quantities dictionary ready for get_document_update
        """

        with Timer() as cpu_timer:

            quantities = {'current': portfolio['holdings']}
            
            # ensure times are sorted ascending so we can breka the loop later
            sorted_history = sorted(portfolio['history'], key=lambda purchase: purchase['time'])

            for timeframe in self.timeframes[:-1]:
                
                holdings = {}
                
                for purchase in sorted_history:

                    market = purchase['market'] 

                    if purchase['time'] <= self.times[timeframe]:
                        if market not in holdings:
                            holdings[market] = np.array(purchase['quantity'])
                        else:
                            holdings[market] += np.array(purchase['quantity'])
                    else:
                        # no need to check any more, its sorted
                        break
                
                if len(holdings) == 0:
                    holdings = {'cash': [500]}

                quantities[timeframe] = holdings

            quantities['M'] = {'cash': [500]}

        self.cpu_time += cpu_timer.t

        return quantities


    def update_all_portfolios(self):

        batches = [firebase.db.batch()]

        with Timer() as timer:

            # no need to do anything fancier here, this should be the most memory efficient way to do it
            # however, there may be some timeout operation???
            for i, doc in enumerate(firebase.portfolios_collection.stream()):
            
                document = self.get_document_update(self.get_quantities_dict(doc.to_dict()))

                if (i % 499) == 0:
                    batches.append(firebase.db.batch())
                    batches[-1].update(firebase.portfolios_collection.document(doc.id), document)
                
            # execute firebase batch commits on seperate threads
            with ThreadPoolExecutor(max_workers=len(batches)) as executor:
                executor.map(lambda batch: batch.commit(), batches)

        
        # set this back to empty
        self.saved_markets = {}

        logging.info(f'FIREBASE PORTFOLIOS t = {self.t}. Completed update. time: {timer.t:.4f}s \t redis time: {self.redis_time:.4f}s \t cpu time: {self.cpu_time:.4f}s \t firebase time: {timer.t - self.redis_time - self.cpu_time:.4f}s')

        self.t += 60
        


