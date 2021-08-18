import unidecode
from firebase_admin import firestore
from src.redis_utils.exceptions import ResourceNotFoundError
import numpy as np
import time
import logging

db = firestore.client()
portfolios = db.collection(u'portfolios')
users = db.collection(u'users')


def get_search_terms(name):
    if name is not None:

        names = name.split() + [name]
        names = names + [unidecode.unidecode(name) for name in names]

        sts = []
        for name in names:
            for i in range(len(name)):
                sts.append(name[:i + 1].lower().strip())

        return list(set(sts))

    else:
        return []


def get_all_search_terms(*names):
    return list(set(sum([get_search_terms(name) for name in names], [])))

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


def add_new_portfolio(uid: str, username: str, name: str, public: bool, description: str):

    logging.info(f'Attempting portfolio add: {uid} {name} {public}')

    new_portfolio = {
      'user': uid,
      'name': name,
      'public': public,
      'username': username,
      'description': description,
      'cash': 500.0,
      'current_value': 500.0,
      'holdings': {},
      'transactions': [],
      'current_values': {},
      'returns_d': 0.0,
      'returns_w': 0.0, 
      'returns_m': 0.0, 
      'returns_M': 0.0, 
      'created': time.time(),
      'active': True,
      'colours': {'cash': '#00bb01'},
      'comments': {},
      'search_terms': get_all_search_terms(name, username),
    }

    timestamp, new_doc = portfolios.add(new_portfolio)
    
    users.document(uid).update({'portfolios':  firestore.ArrayUnion([new_doc.id])})

    return new_doc.id

