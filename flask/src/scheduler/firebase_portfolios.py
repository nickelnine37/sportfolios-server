import os
from lmsr.classic import LMSRMarketMaker, LMSRMultiMarketMaker
from lmsr.long_short import LongShortMarketMaker, LongShortMultiMarketMaker
from scheduler_utils import Timer, RedisExtractor, firebase
import logging
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from typing import Union


class Market:

    def __init__(self, name: str, current: dict, hist: dict) -> None:
        
        self.current = current
        self.name = name
        self.ths = ['d', 'w', 'm', 'M']

    def get_current_value(self, q: Union[list, float], long: bool=None) -> float:
        raise NotImplementedError

    def get_hist_value(self, q: Union[list, float], long: bool=None) -> np.ndarray:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f'Market({self.name})'


class PlayerMarket(Market):

    def __init__(self, name: str, current: dict, hist: dict) -> None:
        super().__init__(name, current, hist)

        Nts = [hist['N'][th][0] for th in self.ths]
        bts = [hist['b'][th][0] for th in self.ths]

        self.multi_market_maker = LongShortMultiMarketMaker(self.name, Nts, bts)
        self.market_maker = LongShortMarketMaker(self.name, self.current['N'], self.current['b'])

    def get_current_value(self, q: float, long: bool) -> float:
        return q * self.market_maker.spot_value(long=long)

    def get_hist_value(self, q: float, long: bool) -> np.ndarray:
        return q * self.multi_market_maker.spot_value(long=long, aslist=False)


class TeamMarket(Market):

    def __init__(self, name: str, current: dict, hist: dict) -> None:
        super().__init__(name, current, hist)

        xts = [hist['x'][th][0] for th in self.ths]
        bts = [hist['b'][th][0] for th in self.ths]

        self.multi_market_maker = LMSRMultiMarketMaker(self.name, xts, bts)
        self.market_maker = LMSRMarketMaker(self.name, self.current['x'], self.current['b'])

    def get_current_value(self, q: list, long: bool=None) -> float:
        return self.market_maker.spot_value(q)

    def get_hist_value(self, q: list, long: bool=None) -> np.ndarray:
        return self.multi_market_maker.spot_value(q, aslist=False)



class Transaction:

    def __init__(self, 
                 market: Market, 
                 transaction_time: float, 
                 quantity: Union[float, list], 
                 price: float, 
                 hist_times: np.ndarray, 
                 long: bool=None):
        """
        A representation of a transaction that has occurred in the past. The transaction data takes
        the form in which it is stored inside a portfolio. It must have the following properties: 
        'market',  'time', 'quantity' and 'price'. hist_times if the whole time log
        """

        self.market = market
        self.quantity = quantity
        self.price = price
        self.long = long
        self.mask = hist_times > transaction_time

    def get_current_value(self):
        return self.market.get_current_value(self.quantity, self.long) - self.price

    def get_hist_value(self):
        value = self.market.get_hist_value(self.quantity, self.long) - self.price
        value[~self.mask] = 0
        return value

    
class Portfolio:

    def __init__(self, portfolio_dict: dict, market_pool: dict, hist_times: np.ndarray, c0: float=500) -> None:
        
        self.c0 = c0
        self.transactions = []

        for transaction in portfolio_dict['transactions']:
            
            market = transaction['market']

            if 'P' in market:
                if 'L' in market:
                    long = True
                else:
                    long = False
                market = market[:-1]

            else:
                long = None

            self.transactions.append(Transaction(market=market_pool[market], 
                                                 transaction_time=transaction['time'], 
                                                 quantity=transaction['quantity'], 
                                                 price=transaction['price'], 
                                                 hist_times=hist_times, 
                                                 long=long))
        
    def get_current_value(self) -> float:
        return sum(transaction.get_current_value() for transaction in self.transactions) + self.c0

    def get_hist_value(self) -> np.ndarray:
        return sum(transaction.get_hist_value() for transaction in self.transactions) + self.c0

    def get_document_update(self):
        
        current_value = self.get_current_value()
        hist_value = self.get_hist_value()
        hist_returns = current_value / hist_value  - 1

        doc = {'current_value': current_value}

        for th, ret in zip(['d', 'w', 'm', 'M'], hist_returns):
            doc[f'returns_{th}'] = ret

        return doc




class FirebasePortfoliosJobs:

    def __init__(self, t: int=0):
        
        self.redis_extractor = RedisExtractor()
        self.t = t
        self.saved_markets = {}

        self.timeframes = ['d', 'w', 'm', 'M']

        times = self.redis_extractor.get_time()
        self.hist_times = np.array([times[tf][0] for tf in self.timeframes])

        self.cpu_time = 0
        self.redis_time = 0

    def reset_compute_time(self):
        self.cpu_time = 0
        self.redis_time = 0

    def filterLS(self, markets: list):
        out = []
        for market in markets:
            if 'L' in market or 'S' in market:
                out.append(market[:-1])
            else:
                out.append(market)
        return out


    def add_to_saved_markets(self, new_markets: list):
        """
        For a list of markets, go to redis and fetch the current and historical holdings. Then iterate through this
        and add the following information to self.saved_markets:

            {'market': 
                {'current': {'x': [current_x], 'b': current_b}, 
                 'd':       {'x': [d_x],       'b': d_b}, ... }}

        Only for markets that are not already in self.saved_markets
        """

        # only fetch markets we have not already fetched
        new_markets = [market for market in self.filterLS(new_markets) if market not in self.saved_markets]

        if len(new_markets) == 0:
            return

        with Timer() as redis_timer:
            currents, historicals = self.redis_extractor.get_current_and_historical_holdings(new_markets)
        
        self.redis_time += redis_timer.t

        with Timer() as cpu_timer:

            for market, current, historical in zip(new_markets, currents, historicals): 

                if (current is None) or (historical is None):
                    logging.error(f'Market {market} not found in Redis')
                    continue

                if 'T' in market:
                    self.saved_markets[market] = TeamMarket(market, current, historical)
                else:
                    self.saved_markets[market] = PlayerMarket(market, current, historical)
                
        self.cpu_time += cpu_timer.t

 
    def update_all_portfolios(self):

        if self.t % 60 == 0:

            batches = []

            with Timer() as timer:

                # no need to do anything fancier here, this should be the most memory efficient way to do it
                # however, there may be some timeout operation???
                for i, portfolio_doc in enumerate(firebase.portfolios_collection.stream()):
                    
                    portfolio_dict = portfolio_doc.to_dict()
                    markets = [transaction['market'] for transaction in portfolio_dict['transactions']]
                    self.add_to_saved_markets(markets)
                    portfolio = Portfolio(portfolio_dict, self.saved_markets, self.hist_times, 500)
                    document = portfolio.get_document_update()

                    if (i % 499) == 0:
                        batches.append(firebase.db.batch())

                    batches[-1].update(firebase.portfolios_collection.document(portfolio_doc.id), document)
                    
                # execute firebase batch commits on seperate threads
                with ThreadPoolExecutor(max_workers=os.cpu_count() + 4) as executor:
                    executor.map(lambda batch: batch.commit(), batches)

            
            # set this back to empty
            self.saved_markets = {}

            logging.info(f'FIREBASE PORTFOLIOS t = {self.t}. Completed update. time: {timer.t:.4f}s \t redis time: {self.redis_time:.4f}s \t cpu time: {self.cpu_time:.4f}s \t firebase time: {timer.t - self.redis_time - self.cpu_time:.4f}s')

        self.t += 2
        


    



    




# class FirebasePortfoliosJobs:

#     def __init__(self, t: int=0):
        
#         self.redis_extractor = RedisExtractor()
#         self.t = t
#         self.saved_markets = {}

#         self.timeframes = ['d', 'w', 'm', 'M']

#         times = self.redis_extractor.get_time()
#         self.times = {tf: times[tf][0] for tf in self.timeframes}

#         self.cpu_time = 0
#         self.redis_time = 0

    
#     def reset_compute_time(self):
#         self.cpu_time = 0
#         self.redis_time = 0


#     def add_to_saved_markets(self, new_markets: list):
#         """
#         For a list of markets, go to redis and fetch the current and historical holdings. Then iterate through this
#         and add the following information to self.saved_markets:

#             {'market': 
#                 {'current': {'x': [current_x], 'b': current_b}, 
#                  'd':       {'x': [d_x],       'b': d_b}, ... }}

#         Only for markets that are not already in self.saved_markets
#         """

#         # only fetch markets we have not already fetched
#         new_markets = [market for market in new_markets if market not in self.saved_markets and market != 'cash']

#         if len(new_markets) == 0:
#             return

#         with Timer() as redis_timer:
#             currents, historicals = self.redis_extractor.get_current_and_historical_holdings(new_markets)
        
#         self.redis_time += redis_timer.t

#         with Timer() as cpu_timer:

#             for market, current, historical in zip(new_markets, currents, historicals): 

#                 if (current is None) or (historical is None):
#                     logging.error(f'Market {market} not found in Redis')
#                     continue

#                 self.saved_markets[market] = {'current': current}

#                 for timeframe in self.timeframes:
                    
#                     if market[-1] == 'T':
#                         self.saved_markets[market][timeframe] = {'x': historical['x'][timeframe][0], 'b': historical['b'][timeframe][0]}
#                     else:
#                         self.saved_markets[market][timeframe] = {'N': historical['N'][timeframe][0], 'b': historical['b'][timeframe][0]}
                    

#         self.cpu_time += cpu_timer.t


#     def get_document_update(self, quantities: dict):
#         """
#         For a given dictionary of quantities held by the user, calulate the returns at each period. 
#         quantities should be indexed by timeframe, and look something like this:
#             {'current': {'market1': [1, 2, 3], 
#                          'market2': [1, 2, 3], }
#              'd':       {'market1': [2, 3, 4], }, 
#             }
#         Returns something like this: {'returns_h': 0.1, 'returns_w': 0.2, 'returns_m': 0.3, 'returns_M': 0.4, 'current_value': 510 }
#         """

#         with Timer() as cpu_timer:

#             values = {}

#             # exclude maxly - it's value was 500
#             for timeframe in ['current'] + self.timeframes[:-1]:

#                 value = 0

#                 cpu_timer.pause()
#                 self.add_to_saved_markets([market[:-1] if 'P' in market else market for market in quantities[timeframe].keys()])
#                 cpu_timer.resume()
                
#                 for market, q in quantities[timeframe].items():
                    
#                     # it might not be, if it's missing from redis, we'll just ignore??
#                     if market in self.saved_markets or market == 'cash':

#                         if market == 'cash':
#                             value += q    
#                         elif market[-1] == 'T':
#                             x, b = self.saved_markets[market][timeframe]['x'], self.saved_markets[market][timeframe]['b']
#                             value += LMSRMarketMaker(market, x, b).spot_value(q)
#                         else:
#                             N, b = self.saved_markets[market][timeframe]['N'], self.saved_markets[market][timeframe]['b']
#                             if market[-1] == 'L':
#                                 value += q * LongShortMarketMaker(market, N, b).spot_value(long=True)
#                             else:
#                                 value += q * LongShortMarketMaker(market, N, b).spot_value(long=False)
                            
#                 values[timeframe] = value

#             values['M'] = 500

#             doc = {}

#             for timeframe in self.timeframes:
#                 doc[f'returns_{timeframe}'] = values['current'] / values[timeframe] - 1
            
#             doc['current_value'] = values['current']

#         self.cpu_time += cpu_timer.t

#         return doc


#     def get_quantities_dict(self, portfolio: dict):
#         """
#         Find out what markets were held at the beginning of each timeframe, and how much? 

#         """

#         with Timer() as cpu_timer:
            
#             # NOTE: portfolio holdings contain the L/S tag
#             quantities = {'current': portfolio['holdings']}
            
#             # ensure times are sorted ascending so we can breka the loop later
#             sorted_history = sorted(portfolio['history'], key=lambda purchase: purchase['time'])

#             for timeframe in self.timeframes[:-1]:

#                 holdings_at_period_start = {}
                
#                 for purchase in sorted_history:

#                     market = purchase['market']
#                     q = purchase['quantity']

#                     if market[-1] == 'T':
#                         q = np.asarray(q)

#                     if purchase['time'] <= self.times[timeframe]:
#                         if market not in holdings_at_period_start:
#                             holdings_at_period_start[market] = q
#                         else:
#                             holdings_at_period_start[market] += q
#                     else:
#                         break
                
#                 if len(holdings_at_period_start) == 0:
#                     holdings_at_period_start = {'cash': 500}

#                 quantities[timeframe] = holdings_at_period_start

#             quantities['M'] = {'cash': 500}

#         self.cpu_time += cpu_timer.t

#         return quantities


#     def update_all_portfolios(self):

#         if self.t % 60 == 0:

#             batches = []

#             with Timer() as timer:

#                 # no need to do anything fancier here, this should be the most memory efficient way to do it
#                 # however, there may be some timeout operation???
#                 for i, portfolio_doc in enumerate(firebase.portfolios_collection.stream()):
                    
#                     document = self.get_document_update(self.get_quantities_dict(portfolio_doc.to_dict()))

#                     if (i % 499) == 0:
#                         batches.append(firebase.db.batch())

#                     batches[-1].update(firebase.portfolios_collection.document(portfolio_doc.id), document)
                    
#                 # execute firebase batch commits on seperate threads
#                 with ThreadPoolExecutor(max_workers=os.cpu_count() + 4) as executor:
#                     executor.map(lambda batch: batch.commit(), batches)

            
#             # set this back to empty
#             self.saved_markets = {}

#             logging.info(f'FIREBASE PORTFOLIOS t = {self.t}. Completed update. time: {timer.t:.4f}s \t redis time: {self.redis_time:.4f}s \t cpu time: {self.cpu_time:.4f}s \t firebase time: {timer.t - self.redis_time - self.cpu_time:.4f}s')

#         self.t += 2
        

