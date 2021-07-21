import numpy as np
import logging
import json

class LMSRMarketMaker:

    def __init__(self, asset: str, x0: list, b: float):

        self.asset = asset
        self.x = np.array(x0)
        self.b = b

    def C(self, x: np.ndarray) -> float:
        """
        LMSR cost function of a inventory vector x
        """
        xmax = x.max()
        return xmax + self.b * np.log(np.exp((x - xmax) / self.b).sum())

    def price_trade(self, q: np.ndarray) -> float:
        """
        The price to make a trade q, taking the inventory vector from x to x + q
        """
        if not isinstance(q, np.ndarray):
            q = np.array(q)
        return self.C(self.x + q) - self.C(self.x)

    def spot_value(self, q: np.ndarray) -> float:
        """
        Get the spot value for a quantity vector q
        """
        if not isinstance(q, np.ndarray):
            q = np.array(q)
        xmax = self.x.max()
        return float((q * np.exp((self.x - xmax) / self.b)).sum() / np.exp((self.x - xmax) / self.b).sum())

    def __repr__(self):
        return f'LMSRMarketMaker({self.asset})'


class LongShortMarketMaker:

    def __init__(self, market: str, N: float, b: float):

        self.market = market
        self.N = N
        self.b = b

    def __repr__(self):
        return f'Long_Short_MarketMaker({self.market})'

    def price_trade(self, n: float, long: bool=True) -> float:
        """
        price of going long with n units on player
        """
        if not long:
            return n + self.price_trade(n=-n)

        N = self.N
        b = self.b

        if n == 0:
            return 0

        elif N == 0:
            if n < 0:
                return b * np.log(b * (np.exp(n / b) - 1) / n) 
            else:
                return b * np.log(b * (1 - np.exp(-n / b)) / (n * np.exp(-n / b)))

        elif N < 0:
            if N == -n:
                return b * np.log(N / (b *  (np.exp(N / b) - 1)))
            else:
                return b * np.log(N / (N + n) * (np.exp((N + n) / b) - 1) / (np.exp(N / b) - 1))

        elif N > 0:
            if N == -n:
                return b * np.log(N * np.exp(-N / b) / (b *  (1 - np.exp(-N / b))))
            else:
                return b * np.log(N / (N + n) * (np.exp(n / b) - np.exp(-N / b)) / (1 - np.exp(-N / b)))
        else:
            raise ValueError(f'N ({N}, type {type(N)}) not an acceptable value')

    def spot_value(self, long: bool=True) -> float:
        """
        Instantaneous price of long on player
        """
        if not long:
            return 1 - self.spot_value()

        k = self.N / self.b

        if k == 0:
            return 0.5
        
        if k > 0:
            return ((k - 1) + np.exp(-k)) / (k * (1 - np.exp(-k)))

        else:
            return (np.exp(k) * (k - 1) + 1) / (k * (np.exp(k) - 1))


class LongShortMultiMarketMaker:

    def __init__(self, market: str, N: list, b: list):
        
        self.market = market
        self.N = np.asarray(N)
        self.b = np.asarray(b)

    def __repr__(self):
        return f'Multi_Long_Short_MarketMaker({self.market})'

    def spot_value(self, long: bool=True) -> list:
        """
        instantaneous price history for player over Ns and bs
        """
        
        if not long:
            return 1 - self.spot_value()

        k = self.N / self.b
        
        m0 = k == 0
        mp = k > 0; kp = k[mp]
        mm = k < 0; km = k[mm]
        
        out = np.zeros_like(k)
        
        out[m0] = 0.5
        out[mm] = (np.exp(km) * (km - 1) + 1) / (km * (np.exp(km) - 1))
        out[mp] = ((kp - 1) + np.exp(-kp)) / (kp * (1 - np.exp(-kp)))
        
        return out.tolist()


# def orders_to_q(orders, hts, dts, wts, mts, Mts):

#     out = {}

#     q0 = orders[0][1]
#     try:
#         N = len(q0)
#     except TypeError:
#         N = 1

#     for xh, ts in zip(['h', 'd', 'w', 'm', 'M'], [hts, dts, wts, mts, Mts]):
        
#         q_out = np.zeros((len(ts), N))
        
#         for t, order in orders:
            
#             if isinstance(order, list):
#                 order = np.array(order)
                
#             q_out[np.array(ts) >= t] += order

#         if N == 1:
#             out[xh] = dict(zip(ts, q_out.reshape(-1)))
#         else:
#             out[xh] = dict(zip(ts, q_out))
                    
#     return out



class LMSRMultiMarketMaker:

    def __init__(self, market: str, xhist: list, bhist: list):

        self.market = market
        self.xs = np.array(xhist)
        self.bs = np.array(bhist).reshape(-1, 1)
        self.xmax = self.xs.max(1).reshape(-1, 1)
        self.T, self.N = self.xs.shape

    def value(self, q: list) -> list:
        
        q = np.array(q).reshape(1, -1)
        assert q.shape == (1, self.N)

        return ((q * np.exp((self.xs - self.xmax) / self.bs)).sum(1) / np.exp((self.xs - self.xmax) / self.bs).sum(1)).reshape(-1).tolist()



# class HistoricalLMSRMarketMaker:

#     def __init__(self, market: str, xhist: dict, bhist: dict):
        
#         self.market = market
#         self.xhist = xhist
#         self.bhist = bhist

#         self.hts = [int(i) for i in xhist['h'].keys()]
#         self.dts = [int(i) for i in xhist['d'].keys()]
#         self.wts = [int(i) for i in xhist['w'].keys()]
#         self.mts = [int(i) for i in xhist['m'].keys()]
#         self.Mts = [int(i) for i in xhist['M'].keys()]
        
#     def spot_value(self, orders: list):
#         """
#         Get the spot value for a quantity vector q
#         """

#         qhist = orders_to_q(orders, self.hts, self.dts, self.wts, self.mts, self.Mts)
        

#         if self.market == 'cash':
#             return qhist

#         out = {}

#         for th in self.xhist.keys():
            
#             ts = [int(i) for i in self.xhist[th].keys()]
#             xs = np.array(list(self.xhist[th].values()))
#             bs = np.array(list(self.bhist[th].values())).reshape(-1, 1)
#             qs = np.array(list(qhist[th].values()))
            
#             xmax = xs.max(1).reshape(-1, 1)
            
#             out[th] = dict(zip(ts, (qs * np.exp((xs - xmax) / bs)).sum(1) / np.exp((xs - xmax) / bs).sum(1)))

#         return out

    


    

