import numpy as np

class LMSRMarketMaker:

    def __init__(self, asset: str, x0: np.ndarray, b: float):

        self.asset = asset
        self.x = x0
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
        return self.C(self.x + q) - self.C(self.x)

    def spot_value(self, q: np.ndarray):
        """
        Get the spot value for a quantity vector q
        """
        xmax = self.x.max()
        return (q * np.exp((self.x - xmax) / self.b)).sum() / np.exp((self.x - xmax) / self.b).sum()

    def execute_order(self, q: np.ndarray):
        """
        Execute an order for a quantity vector q
        """
        price = self.price_trade(q)

    def back_spot_value(self):
        return self.spot_value(q=np.exp(- np.linspace(0, 19, 20) / 6))

    def __repr__(self):
        return f'LMSRMarketMaker({self.asset})'


    

