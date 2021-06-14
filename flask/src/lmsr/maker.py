import numpy as np
import logging
import json

class LMSRMarketMaker:

    def __init__(self, asset: str, x0: list, b: float):

        self.asset = asset
        self.x = np.array(x0)
        self.b = b

    def C(self, x: np.ndarray):
        """
        LMSR cost function of a inventory vector x
        """
        xmax = x.max()
        return xmax + self.b * np.log(np.exp((x - xmax) / self.b).sum())

    def price_trade(self, q: np.ndarray):
        """
        The price to make a trade q, taking the inventory vector from x to x + q
        """
        if not isinstance(q, np.ndarray):
            q = np.array(q)
        return self.C(self.x + q) - self.C(self.x)

    def spot_value(self, q: np.ndarray):
        """
        Get the spot value for a quantity vector q
        """
        if not isinstance(q, np.ndarray):
            q = np.array(q)
        xmax = self.x.max()
        return float((q * np.exp((self.x - xmax) / self.b)).sum() / np.exp((self.x - xmax) / self.b).sum())

    def execute_order(self, q: np.ndarray):
        """
        Execute an order for a quantity vector q
        """
        if not isinstance(q, np.ndarray):
            q = np.array(q)
        price = self.price_trade(q)

    def __repr__(self):
        return f'LMSRMarketMaker({self.asset})'


def orders_to_q(orders, hts, dts, wts, mts, Mts):

    out = {}

    q0 = orders[0][1]
    try:
        N = len(q0)
    except TypeError:
        N = 1

    for xh, ts in zip(['h', 'd', 'w', 'm', 'M'], [hts, dts, wts, mts, Mts]):
        
        q_out = np.zeros((len(ts), N))
        
        for t, order in orders:
            
            if isinstance(order, list):
                order = np.array(order)
                
            q_out[np.array(ts) >= t] += order

        if N == 1:
            out[xh] = dict(zip(ts, q_out.reshape(-1)))
        else:
            out[xh] = dict(zip(ts, q_out))
                    
    return out


class MultiMarketMaker:

    def __init__(self, market: str, xhist: dict, bhist: dict):

        self.market = market
        self.xhist = xhist
        self.bhist = bhist  

        self.ts = [int(i) for i in xhist.keys()]
        self.xs = np.array(list(xhist.values()))
        self.bs = np.array(list(bhist.values())).reshape(-1, 1)
        self.xmax = self.xs.max(1).reshape(-1, 1)

        self.T, self.N = self.xs.shape

    def value(self, q: list):
        
        q = np.array(q).reshape(1, -1)
        assert q.shape == (1, self.N)

        return dict(zip(self.ts, (q * np.exp((self.xs - self.xmax) / self.bs)).sum(1) / np.exp((self.xs - self.xmax) / self.bs).sum(1)))


class _MultiMarketMaker:

    def __init__(self, market: str, xhist: list, bhist: list):

        self.market = market
        self.xs = np.array(xhist)
        self.bs = np.array(bhist).reshape(-1, 1)
        self.xmax = self.xs.max(1).reshape(-1, 1)
        self.T, self.N = self.xs.shape

    def value(self, q: list):
        
        q = np.array(q).reshape(1, -1)
        assert q.shape == (1, self.N)

        return ((q * np.exp((self.xs - self.xmax) / self.bs)).sum(1) / np.exp((self.xs - self.xmax) / self.bs).sum(1)).reshape(-1).tolist()



class HistoricalLMSRMarketMaker:

    def __init__(self, market: str, xhist: dict, bhist: dict):
        
        self.market = market
        self.xhist = xhist
        self.bhist = bhist

        self.hts = [int(i) for i in xhist['h'].keys()]
        self.dts = [int(i) for i in xhist['d'].keys()]
        self.wts = [int(i) for i in xhist['w'].keys()]
        self.mts = [int(i) for i in xhist['m'].keys()]
        self.Mts = [int(i) for i in xhist['M'].keys()]
        
    def spot_value(self, orders: list):
        """
        Get the spot value for a quantity vector q
        """

        qhist = orders_to_q(orders, self.hts, self.dts, self.wts, self.mts, self.Mts)
        

        if self.market == 'cash':
            return qhist

        out = {}

        for th in self.xhist.keys():
            
            ts = [int(i) for i in self.xhist[th].keys()]
            xs = np.array(list(self.xhist[th].values()))
            bs = np.array(list(self.bhist[th].values())).reshape(-1, 1)
            qs = np.array(list(qhist[th].values()))
            
            xmax = xs.max(1).reshape(-1, 1)
            
            out[th] = dict(zip(ts, (qs * np.exp((xs - xmax) / bs)).sum(1) / np.exp((xs - xmax) / bs).sum(1)))

        return out

    


    

