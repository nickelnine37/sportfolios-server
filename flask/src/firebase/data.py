from firebase_admin import firestore
from src.redis_utils.exceptions import ResourceNotFoundError
import numpy as np
import time

db = firestore.client()
portfolios = db.collection(u'portfolios')


def get_portfolio(portfolioId: str) -> dict:
    doc = portfolios.document(portfolioId).get()
    if doc is None:
        return None
    return doc.to_dict()


def check_portfolio(portfolioId: str, uid: str) -> bool:
    portfolio = get_portfolio(portfolioId)
    if portfolio is None:
        return False
    return portfolio['user'] == uid


def make_purchase(uid: str, portfolioId: str, market: str, quantity: list, price: float):
    
    doc = portfolios.document(portfolioId)
    current = doc.get()

    if not current.exists:
        raise ResourceNotFoundError

    current = current.to_dict()

    if market in current['current']:
        doc.update({f'current.{market}': (np.array(current['current'][market]) + np.array(quantity)).tolist()})

    else:
        doc.update({f'current.{market}': quantity})

    doc.update({f'current.cash': firestore.Increment(-price)})

    t = int(time.time()) 

    doc.update({'history': firestore.ArrayUnion([{'market': 'cash', 'quantity': -price,   'time': t}, 
                                                 {'market': market, 'quantity': quantity, 'time': t}])})

