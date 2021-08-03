import numpy as np
from typing import Union


class LMSRMarketMaker:
    """
    Regular LMSR market maker for evaluating the instantaneous price and the cost of a trade, 
    for a given single x and b
    """

    def __init__(self, market: str, x: Union[list, np.ndarray], b: float):

        self.asset = market
        self.x = np.asarray(x)
        self.b = b

    def C(self, x: np.ndarray) -> float:
        """
        LMSR cost function of a inventory vector x
        """
        xmax = x.max()
        return xmax + self.b * np.log(np.exp((x - xmax) / self.b).sum())

    def price_trade(self, q: Union[list, np.ndarray]) -> float:
        """
        The price to make a trade q, taking the inventory vector from x to x + q
        """
        return float(self.C(self.x + np.asarray(q)) - self.C(self.x))

    def spot_value(self, q: Union[list, np.ndarray]) -> float:
        """
        Get the spot value for a quantity vector q
        """
        xmax = self.x.max()
        return float((np.asarray(q) * np.exp((self.x - xmax) / self.b)).sum() / np.exp((self.x - xmax) / self.b).sum())

    def __repr__(self):
        return f'LMSRMarketMaker({self.asset})'


class LMSRMultiMarketMaker:
    """
    Used to evaluate the instantaneous market price of a quantity vector q
    over a series of times, each with a potnentially unique x and b. 
    """

    def __init__(self, market: str, xhist: list, bhist: list):

        self.market = market
        self.xs = np.array(xhist)
        self.bs = np.array(bhist).reshape(-1, 1)
        self.xmax = self.xs.max(1).reshape(-1, 1)
        self.T, self.N = self.xs.shape

    def spot_value(self, q: list, aslist: bool=True) -> list:
        
        q = np.array(q).reshape(1, -1)
        assert q.shape == (1, self.N)

        if aslist:
            return ((q * np.exp((self.xs - self.xmax) / self.bs)).sum(1) / np.exp((self.xs - self.xmax) / self.bs).sum(1)).reshape(-1).tolist()
        else:
            return ((q * np.exp((self.xs - self.xmax) / self.bs)).sum(1) / np.exp((self.xs - self.xmax) / self.bs).sum(1)).reshape(-1)


    def __repr__(self):
        return f'LMSRMultiMarketMaker({self.market})'