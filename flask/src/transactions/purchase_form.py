import json
from src.redis_utils.exceptions import ResourceNotFoundError
from firebase_admin import firestore
from src.transactions.make_purchase import cancel_undo_scheduled_purchase, make_purchase, schedule_undo_purchase, undo_purchase, undo_scheudlued_purchase_now
import redis 
import math
import numpy as np
import time

db = firestore.client()
portfolios = db.collection(u'portfolios')
redis_db = redis.Redis(host='redis', port=6379, db=0)


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


def round_decimals_up(number:float, decimals:int=2):
    """
    Returns a value rounded up to a specific number of decimal places.
    """
    
    factor = 10 ** decimals
    
    if int(number * factor) / factor == number:
        return number
        
    return math.ceil(number * factor) / factor


class PurchaseFormError(ValueError):
    pass

class MissingEntriesError(PurchaseFormError):
    pass

class InvalidMarketError(PurchaseFormError):
    pass

class PortfolioError(PurchaseFormError):
    pass

class TransactionError(PurchaseFormError):
    pass



class ConfirmationFormError(ValueError):
    pass

class CancelIdNotFoundError(ConfirmationFormError):
    pass

class ConfirmationTooLateError(ConfirmationFormError):
    pass




def push_transaction_to_firebase(purchse_form: dict) -> None:
    """
    Push the transaction to firebase
    """
    
    doc = portfolios.document(purchse_form['portfolioId'])
    current = doc.get()

    if not current.exists:
        raise ResourceNotFoundError

    current = current.to_dict()

    market = purchse_form['market']
    quantity = purchse_form['quantity']
    price = purchse_form['price']

    # NOTE we can treat quantity as a array regardless of whether it's an array or a float, because .tolist()
    # returns a float when we have a numpy array of a number 

    if market in current['holdings']:
        newQ = np.array(current['holdings'][market]) + np.array(quantity)
        if np.isclose(newQ, 0, atol=1e-3).all():
            doc.update({f'holdings.{market}': firestore.DELETE_FIELD})
        else:
            doc.update({f'holdings.{market}': newQ.tolist()})

    else:
        doc.update({f'holdings.{market}': quantity})

    doc.update({f'holdings.cash': [current['holdings']['cash'][0] - price]})

    t = int(time.time()) 

    doc.update({'history': firestore.ArrayUnion([{'market': 'cash', 'quantity': [-price], 'time': t}, 
                                                    {'market': market, 'quantity': quantity, 'time': t}])})



class PurchaseForm:
    """
    When a user attempts to make a purchase, create a purchase form for them. This 
    handles validation, and other core business logic. 
    """

    def __init__(self, uid: str, post_form: dict):
        """
        Perform basic validation on purchase form
        """

        portfolioId = post_form.get('portfolioId')
        market = post_form.get('market')
        quantity = post_form.get('quantity')
        price = post_form.get('price')
        long = post_form.get('long')
        
        for entry, value in zip(['market', 'portfolioId', 'quantity', 'price'], [market, portfolioId, quantity, price]):
            if value is None:
                raise MissingEntriesError(f'{entry} is missing from the purchase form')

        if market[-1] == 'T':
            team = True
        elif market[-1] == 'P':
            team = False
        else:
            raise InvalidMarketError(f'The market string ({market}) is malformed')

        if not team and long is None:
            raise MissingEntriesError('Player is selected but long is not supplied')
        
        try:
            quantity = json.loads(quantity)
            price = float(price)
            long = json.loads(long)

        except Exception:
            raise PurchaseFormError(f'One of quantity ({quantity}), price ({price}) or long ({long}) is malformed')

        if not check_portfolio(portfolioId, uid):
            raise PortfolioError(f'The portfiolio ID {portfolioId} does not match the user ID {uid}, or cannot be found')

        self.form = {'uid': uid, 
                     'portfolioId': portfolioId, 
                     'market': market, 
                     'quantity': quantity, 
                     'price': price,
                     'team': team,
                     'long': long}


    def prices_consistent(self, price: float):
        """
        Check whether the price calculated by us is consistent with the 
        price supplied by the user, stored in the form 
        """

        profit = round_decimals_up(self.form['price']) - price
        return 0.01 > profit >= 0


    def attempt_purchase(self) -> dict:
        """
        Attempt to make a purchse for the given purchase form. If the transaction takes place, 
        return dict giving information, for example whether the price is as the user expected
        and the cancelID if necessary. 
        """

        try:
            price = make_purchase(self.form)

        except ResourceNotFoundError:
            raise TransactionError(f'The market {self.form["market"]} cannot be found or is invalid')

        except redis.WatchError:
            raise TransactionError(f'There is currently too much trading activity to complete this purchase')

        if self.prices_consistent(price):

            try:
                push_transaction_to_firebase(self.form)
            except:
                undo_purchase(self.form)

            return {'success': True, 'price': price, 'cancelId': None}

        else:
            cancelId = schedule_undo_purchase(self.form)
            return {'success': False, 'price': price, 'cancelId': cancelId}


class ConfirmationForm:

    def __init__(self, uid: str, confirmation_form: dict):

        cancelId = confirmation_form.get('cancelId')
        confirm = confirmation_form.get('confirm')

        for entry, value in zip(['cancelId', 'confirm'], [cancelId, confirm]):
            if value is None:
                raise MissingEntriesError(f'{entry} is missing from the purchase form')

        try:
            self.confirm = json.loads(confirm)
        except:
            raise ConfirmationFormError(f'The value confirm ({confirm}) must be true or false')

        if not redis_db.exists(cancelId):
            raise CancelIdNotFoundError(f'The cancelId {cancelId} was not found in Redis. Either it is malformed, or the order has already been cancelled')

        else:
            self.cancelId = cancelId
            self.old_purchase_form = json.loads(redis_db.get(self.cancelId))

        if self.old_purchase_form['uid'] != uid:
            raise ConfirmationFormError(f'The userID supplied {uid} does not match that of the original order')
        else:
            self.uid = uid

    
    def process_request(self):
        """
        Process the users request to either confirm or redact their order
        """
        
        # the user wants to proceed with the transaction, so we must cancel the undoing
        # an issue may arise if it's already been cancelled, i.e. they were too slow
        if self.confirm:
            try:
                cancel_undo_scheduled_purchase(self.old_purchase_form)
                return 'Order confirmed'
            except ResourceNotFoundError:
                raise ConfirmationTooLateError('This order could not be confirmed as the cancellation has already happened')

        # the user wants us to cancel their original transaction, so lets do it now
        # it might have already happened, which is no problem
        else:
            try:
                undo_scheudlued_purchase_now(self.old_purchase_form)
            except ResourceNotFoundError:
                pass
            return 'Order cancelled' 

