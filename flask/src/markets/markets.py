import redis
import json
import os
import logging
from src.lmsr.maker import LMSRMarketMaker, MultiMarketMaker, _MultiMarketMaker
from src.redis_utils.exceptions import ResourceNotFoundError
import numpy as np

redis_db = redis.Redis(host='redis', port=6379, db=0)


class Market: 

    def __init__(self, name: str):

        self.name = name
        self.N = None

        self.x = None
        self.b = None
        self.MM = None

        self.daily_x = None
        self.daily_b = None
        self.daily_MM = None

        self.historical_x = None
        self.historical_b = None
        self.historical_MM = None

    def set_xb(self, x: list, b: float):

        self.x = x
        self.b = b
        if self.N is None:
            self.N = len(x)

        assert isinstance(self.x, list)
        assert self.N > 0
        assert isinstance(self.b, (int, float)) 

        self.MM = LMSRMarketMaker(self.name, self.x, self.b)

    def get_xb(self):
        self.set_xb(**json.loads(redis_db.get(self.name)))

    def set_daily_xb(self, daily_x: dict, daily_b: dict):

        self.daily_x = daily_x
        self.daily_b = daily_b
        if self.N is None:
            self.N = len(next(iter(daily_x.values())))

        assert isinstance(daily_x, dict)
        assert isinstance(daily_b, dict)
        assert daily_x.keys() == daily_b.keys()

        self.daily_MM = MultiMarketMaker(self.name, self.daily_x, self.daily_b)

    def get_daily_xb(self):

        xhist = json.loads(redis_db.get(market.name + ':xhist'))['d']
        bhist = json.loads(redis_db.get(market.name + ':bhist'))['d']
        market.set_daily_xb(xhist, bhist)

        
    def current_back_price(self):
        return self.MM.spot_value(q=10 * np.exp(- np.linspace(0, self.N - 1, self.N) / 6))



    def daily_back_price(self):
        return self.daily_MM.value(q=10 * np.exp(- np.linspace(0, self.N - 1, self.N) / 6))


    def current_holding(self, pipe=None):
        pass

    def historical_holdings(self, pipe=None):
        pass

    
class MarketCollection:

    def __init__(self, names: list):
        self.markets = [Market(name) for name in names]

    def current_back_prices(self):

        prices = {}

        with redis_db.pipeline() as pipe:

            for market in self.markets:
                pipe.get(market.name)

            results = pipe.execute()

        for current_xb, market in zip(results, self.markets):

            market.set_xb(**json.loads(current_xb))
            prices[market.name] = market.current_back_price()

        return prices

    def daily_back_prices(self):

        prices = {}

        with redis_db.pipeline() as pipe:

            for market in self.markets:
                pipe.get(market.name + ':xhist')
                pipe.get(market.name + ':bhist')
                
            results = pipe.execute()

        for i, market in enumerate(self.markets):

            xhist = json.loads(results[2 * i])['d']
            bhist = json.loads(results[2 * i + 1])['d']

            market.set_daily_xb(xhist, bhist)
            prices[market.name] = market.daily_back_price()

        return prices



        
class _Market: 

    def __init__(self, name: str):
        
        if name.split(':')[-1][-1] == 'T':
            self.back_divisor  = 6
        else:
            self.back_divisor  = 3

        self.name = name
        self.N = None

        self.x = None
        self.b = None
        self.MM = None

        self.daily_x = None
        self.daily_b = None
        self.daily_MM = None

        self.historical_x = None
        self.historical_b = None
        self.historical_MM = None

    def set_xb(self, x: list, b: float):

        self.x = x
        self.b = b
        if self.N is None:
            self.N = len(x)

        assert isinstance(self.x, list)
        assert self.N > 0
        assert isinstance(self.b, (int, float)) 

        self.MM = LMSRMarketMaker(self.name, self.x, self.b)

    def get_xb(self):
        self.set_xb(**json.loads(redis_db.get(self.name)))

    def set_daily_xb(self, daily_x: list, daily_b: list):

        assert isinstance(daily_x, list)
        assert isinstance(daily_b, list)

        self.daily_x = daily_x
        self.daily_b = daily_b

        if self.N is None:
            self.N = len(daily_x[0])

        self.daily_MM = _MultiMarketMaker(self.name, self.daily_x, self.daily_b)
        
    def current_back_price(self):
        return self.MM.spot_value(q=10 * np.exp(- np.linspace(0, self.N - 1, self.N)[::-1] / self.back_divisor))


    def daily_back_price(self):
        return self.daily_MM.value(q=10 * np.exp(- np.linspace(0, self.N - 1, self.N)[::-1] / self.back_divisor))


    def current_holding(self, pipe=None):
        pass

    def historical_holdings(self, pipe=None):
        pass

    
class _MarketCollection:

    def __init__(self, names: list):
        self.markets = [_Market(name) for name in names]

    def current_back_prices(self):

        prices = {}

        with redis_db.pipeline() as pipe:

            for market in self.markets:
                pipe.get(market.name)

            results = pipe.execute()

        for current_xb, market in zip(results, self.markets):

            if current_xb is None:
                prices[market.name] = None
                logging.info(f'_MarketCollection.current_back_prices failed for {market.name}')
            else:
                market.set_xb(**json.loads(current_xb))
                prices[market.name] = market.current_back_price()

        return prices

    def daily_back_prices(self):

        prices = {}

        with redis_db.pipeline() as pipe:

            for market in self.markets:
                pipe.get(market.name + ':hist')
                
            results = pipe.execute()

        for hist, market in zip(results, self.markets):

            if hist is None:
                prices[market.name] = None
                logging.info(f'_MarketCollection.daily_back_prices failed for {market.name}')
            else:
                hist = json.loads(hist)
                xhist = hist['x']['d']
                bhist = hist['b']['d']

                market.set_daily_xb(xhist, bhist)
                prices[market.name] = market.daily_back_price()

        return prices



        



