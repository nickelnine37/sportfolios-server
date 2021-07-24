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

    def __repr__(self):
        return f'LongShortMarketMaker({self.market})'

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

    def spot_value(self, long: bool) -> float:
        """
        Instantaneous price of long on player
        """
        if not long:
            return 1 - self.spot_value(True)

        k = self.N / self.b

        if k == 0:
            return 0.5
        
        if k > 0:
            return ((k - 1) + np.exp(-k)) / (k * (1 - np.exp(-k)))

        else:
            return (np.exp(k) * (k - 1) + 1) / (k * (np.exp(k) - 1))


class LongShortMultiMarketMaker:
    """
    Used to evaluate the value of the long contract over time, with a series 
    of N and bs. 
    """

    def __init__(self, market: str, N: Union[list, np.ndarray], b: Union[list, np.ndarray]):
        
        self.market = market
        self.N = np.asarray(N)
        self.b = np.asarray(b)

    def __repr__(self):
        return f'LongShortMultiMarketMaker({self.market})'

    def spot_value(self, long: bool, aslist: bool=True) -> list:
        """
        instantaneous price history for player over Ns and bs
        """
        
        if not long:
            out = 1 - self.spot_value(True, aslist=False)
            if aslist:  
                return out.tolist()
            else:
                return out
            

        k = self.N / self.b
        
        m0 = k == 0
        mp = k > 0; kp = k[mp]
        mm = k < 0; km = k[mm]
        
        out = np.zeros_like(k)
        
        out[m0] = 0.5
        out[mm] = (np.exp(km) * (km - 1) + 1) / (km * (np.exp(km) - 1))
        out[mp] = ((kp - 1) + np.exp(-kp)) / (kp * (1 - np.exp(-kp)))
        
        if aslist:
            return out.tolist()
        else:
            return out
