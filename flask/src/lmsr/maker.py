import numpy as np

from server.utils.io import read_last_line
from server.utils.math_utils import log_sum_exp




class MarketMaker:

    def __init__(self, asset: str):

        self.asset = asset
        info = read_last_line('holdings/' + asset + '.csv').split(',')
        self.b = float(info[-1])
        self.x = np.array([float(n) for n in info[1:-1]])

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
        return self.C(self.x + q) - self.C(self.x)

    def spot_value(self, q: np.ndarray):
        """
        Get the spot value for a quantity vector q
        """
        xmax = self.x.max()
        return (q * np.exp(self.x - xmax)).sum() / np.exp(self.x - xmax).sum()

    def execute_order(self, q: np.ndarray):
        """
        Execute an order for a quantity vector q
        """

        price = self.price_trade(q)













if __name__ == '__main__':

    MarketMaker(asset='P37:12:17426')
    

