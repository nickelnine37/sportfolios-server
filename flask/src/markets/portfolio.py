# from collections import defaultdict
# from src.redis_utils.get_data import get_spot_quantity_values, get_multiple_historical_quantities
# from src.lmsr.maker import HistoricalLMSRMarketMaker, orders_to_q
# import logging 

# class Portfolio:

#     def __init__(self, portfolioId: str, portfolioDoc: dict):

#         self.markets, self.current_quantities = zip(*portfolioDoc['current'].items())
#         self.values = get_spot_quantity_values(self.markets, self.current_quantities)
#         self.historical_x = get_multiple_historical_quantities(self.markets)
#         self.value = sum(self.values.values())

#         self.orders = defaultdict(list)

#         for order in portfolioDoc['history']:
#             self.orders[order['market']].append((order['time'], order['quantity']))

#     def build_history(self):

#         hist = None

#         for market, orders in self.orders.items():

#             mm = HistoricalLMSRMarketMaker(market, xhist=self.historical_x[market]['xhist'], bhist=self.historical_x[market]['bhist'])

#             if hist is None:
#                 hist = mm.spot_value(self.orders[market])

#             else:
#                 h = mm.spot_value(self.orders[market])
#                 logging.info(str(h))
#                 logging.info(str(hist))
#                 for th in self.historical_x[market]['xhist'].keys():
#                     for t in hist[th].keys():
#                         hist[th][t] += h[th][t] 


#         return hist



