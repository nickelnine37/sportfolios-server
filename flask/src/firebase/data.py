from firebase_admin import firestore 

db = firestore.client()
portfolios = db.collection(u'portfolios')


def get_portfolio(portfolioId: str) -> dict:
    return portfolios.document(portfolioId).get().to_dict()


def check_portfolio(portfolioId: str, uid: str) -> bool:
    portfolio = get_portfolio(portfolioId)
    if portfolio is None:
        return False
    return portfolio['user'] == uid

