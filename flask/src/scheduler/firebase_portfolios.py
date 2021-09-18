import os

from numpy.lib.function_base import quantile
from lmsr.classic import LMSRMarketMaker, LMSRMultiMarketMaker
from lmsr.long_short import LongShortMarketMaker, LongShortMultiMarketMaker
from scheduler_utils import Timer, RedisExtractor, firebase
import logging
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from typing import Union


class Market:

    def __init__(self, name: str, current: dict, hist: dict) -> None:
        """
        name:     the market name
        current:  the current holdings dict from redis
        hist:     the historical holdings dict from redis   
        """
        
        # no need to add the whole hist here - we jsut need the start of each interval which is done in the sub-classes
        self.current = current
        self.name = name
        self.ths = ['d', 'w', 'm', 'M']

    def get_current_value(self, q: Union[list, np.ndarray]) -> float:
        """
        Calculate the current value of the quantity vector q
        """
        return self.market_maker.spot_value(q)

    def get_hist_value(self, q: Union[list, np.ndarray]) -> np.ndarray:
        """
        Calculate the value of the quantity vector at each of the relevant timepoints (a vector of length 4)
        """
        return self.multi_market_maker.spot_value(q, aslist=False)

    def __repr__(self) -> str:
        return f'Market({self.name})'


class PlayerMarket(Market):

    def __init__(self, name: str, current: dict, hist: dict) -> None:
        super().__init__(name, current, hist)

        Nts = [hist['N'][th][0] for th in self.ths]
        bts = [hist['b'][th][0] for th in self.ths]

        self.multi_market_maker = LongShortMultiMarketMaker(self.name, Nts, bts)
        self.market_maker = LongShortMarketMaker(self.name, self.current['N'], self.current['b'])

    def __repr__(self) -> str:
        return f'PlayerMarket({self.name})'


class TeamMarket(Market):

    def __init__(self, name: str, current: dict, hist: dict) -> None:
        super().__init__(name, current, hist)

        xts = [hist['x'][th][0] for th in self.ths]
        bts = [hist['b'][th][0] for th in self.ths]

        self.multi_market_maker = LMSRMultiMarketMaker(self.name, xts, bts)
        self.market_maker = LMSRMarketMaker(self.name, self.current['x'], self.current['b'])

    def __repr__(self) -> str:
        return f'TeamMarket({self.name})'


class Transaction:

    def __init__(self, 
                 market: Market, 
                 transaction_time: float, 
                 quantity: Union[list, np.ndarray], 
                 price: float, 
                 hist_times: np.ndarray):
        """
        A representation of a transaction that has occurred in the past. The transaction data takes
        the form in which it is stored inside a portfolio. It must have the following properties: 
        'hist_times' is a numpy array holdings the timestamps of when the intervals started
        """

        self.market = market
        self.quantity = quantity
        self.price = price
        self.mask = hist_times > transaction_time

    def get_current_value(self) -> float:
        return self.market.get_current_value(self.quantity) - self.price

    def get_hist_value(self) -> np.ndarray:
        value = self.market.get_hist_value(self.quantity) - self.price
        value[~self.mask] = 0
        return value


class Holding:

    def __init__(self, market: Market, quantity: Union[list, np.ndarray]) -> None:
        self.market = market
        self.quantity = quantity
        self.value = 0

    def get_value(self):
        self.value = self.market.get_current_value(self.quantity)
        return self.value

    
class Portfolio:

    def __init__(self, portfolio_dict: dict, market_pool: dict, hist_times: np.ndarray, c0: float=500) -> None:
        
        self.c0 = c0
        self.cash = portfolio_dict['cash']
        self.transactions = [Transaction(market=market_pool[transaction['market']], 
                                         transaction_time=transaction['time'], 
                                         quantity=transaction['quantity'], 
                                         price=transaction['price'], 
                                         hist_times=hist_times) for transaction in portfolio_dict['transactions']]

        self.holdings = [Holding(market=market_pool[market_name], quantity=quantity) for market_name, quantity in portfolio_dict['holdings'].items()]
         
        
    def get_current_value(self) -> float:
        return sum(holding.get_value() for holding in self.holdings) + self.cash

    def get_hist_value(self) -> np.ndarray:
        return sum(transaction.get_hist_value() for transaction in self.transactions) + self.c0

    def get_current_values(self):
        return {holding.market.name: holding.value for holding in self.holdings}

    def get_document_update(self):
        
        current_value = self.get_current_value()
        current_values = self.get_current_values()
        hist_value = self.get_hist_value()
        hist_returns = (current_value / hist_value  - 1).reshape(-1).tolist()

        doc = {'current_value': current_value, 'current_values': current_values}

        for th, ret in zip(['d', 'w', 'm', 'M'], hist_returns):
            doc[f'returns_{th}'] = ret

        return doc




class FirebasePortfoliosJobs:

    def __init__(self):
        
        self.redis_extractor = RedisExtractor()
        self.saved_markets = {}

        times = self.redis_extractor.get_time()
        self.hist_times = np.array([times[tf][0] for tf in ['d', 'w', 'm', 'M']])

        self.cpu_time = 0
        self.redis_time = 0

    def reset_compute_time(self):
        self.cpu_time = 0
        self.redis_time = 0

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
        new_markets = [market for market in new_markets if market not in self.saved_markets]

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

 
    def update_all_portfolios(self, t: int):


        # set this back to empty
        self.saved_markets = {}

        batches = [firebase.db.batch()]#

        with Timer() as timer:

            # no need to do anything fancier here, this should be the most memory efficient way to do it
            # however, there may be some timeout operation???
            for i, portfolio_doc in enumerate(firebase.portfolios_collection.stream()):

                portfolio_dict = portfolio_doc.to_dict()
                markets = list(set([transaction['market'] for transaction in portfolio_dict['transactions']]))

                # no markets, just carry on
                if len(markets) == 0:
                    continue

                self.add_to_saved_markets(markets)

                document = Portfolio(portfolio_dict=portfolio_dict,
                                     market_pool=self.saved_markets,
                                     hist_times=self.hist_times,
                                     c0=500).get_document_update()

                if (i % 499) == 498:
                    batches.append(firebase.db.batch())

                batches[-1].update(firebase.portfolios_collection.document(portfolio_doc.id), document)

            # execute firebase batch commits on seperate threads
            with ThreadPoolExecutor(max_workers=os.cpu_count() + 4) as executor:
                executor.map(lambda batch: batch.commit(), batches)

        logging.info(f'FIREBASE PORTFOLIOS t = {t}. Completed update. time: {timer.t:.4f}s \t redis time: {self.redis_time:.4f}s \t cpu time: {self.cpu_time:.4f}s \t firebase time: {timer.t - self.redis_time - self.cpu_time:.4f}s')




    

