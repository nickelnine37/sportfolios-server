import numpy as np
from typing import Union

class LongShortMarketMaker:
    """
    The Long-Shiort market maker, capable of calculating the inst price and cost to trade
    for a given number of longs or shorts
    """

    def __init__(self, market: str, N: float, b: float):

        self.market = market
        self.N = N
        self.b = b
        k = N / b
        
        if k == 0:
            self.long_price = 0.5
        if k > 0:
            self.long_price = ((k - 1) + np.exp(-k)) / (k * (1 - np.exp(-k)))
        else:
            self.long_price = (np.exp(k) * (k - 1) + 1) / (k * (np.exp(k) - 1))

    def __repr__(self):
        return f'LongShortMarketMaker({self.market})'

    def price_trade(self, q: Union[list, np.ndarray]) -> float:
        """
        Price a trade for vector quantity q, where q[0] is the number of longs and q[1] is the number of shorts
        """


        N = self.N
        b = self.b
        
        def f(n):
            """
            price of going long with n units on player
            """

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
                
        return float(f(q[0]) + q[1] + f(-q[1]))

    def spot_value(self, q: Union[list, np.ndarray]) -> float:
        """
        Instantaneous price for vector quantity q, where q[0] is the number of longs and q[1] is the number of shorts
        """

        cMin = min(q)
        cMax = max(q)
        
        if np.argmax(q) == 0:
            return float(cMin + self.long_price * (cMax - cMin))
        else:
            return float(cMax - self.long_price * (cMax - cMin))
        

class LongShortMultiMarketMaker:
    """
    Used to evaluate the value of the long contract over time, with a series 
    of N and bs. 
    """

    def __init__(self, market: str, Ns: Union[list, np.ndarray], bs: Union[list, np.ndarray]):
        
        self.market = market
        Ns = np.asarray(Ns)
        bs = np.asarray(bs)
        ks = Ns / bs
        
        m0 = ks == 0
        mp = ks > 0; kp = ks[mp]
        mm = ks < 0; km = ks[mm]
        
        self.long_price = np.zeros_like(ks)
        
        self.long_price[m0] = 0.5
        self.long_price[mm] = (np.exp(km) * (km - 1) + 1) / (km * (np.exp(km) - 1))
        self.long_price[mp] = ((kp - 1) + np.exp(-kp)) / (kp * (1 - np.exp(-kp)))
        
    def __repr__(self):
        return f'LongShortMultiMarketMaker({self.market})'

    def spot_value(self, q: Union[list, np.ndarray], aslist=True) -> list:
        """
        instantaneous price history for player over Ns and bs
        """
        
        cMin = min(q)
        cMax = max(q)
        
        if np.argmax(q) == 0:
            out = cMin + self.long_price * (cMax - cMin)
        else:
            out = cMax - self.long_price * (cMax - cMin)
        
        if aslist:
            return out.tolist()
        else:
            return out