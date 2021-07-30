import os
import logging
from src.firebase.data import add_new_portfolio
from flask import Flask, request, jsonify
import orjson

# LOAD THIS FIRST TO ACCESS FIREBASE STUFF
from src.firebase.authentication import verify_user_token, verify_admin

from src.redis_utils.init_redis_db import init_redis_f
from src.redis_utils.update import  update_b_redis
import src.redis_utils.read_data as read_data
from src.redis_utils.exceptions import ResourceNotFoundError
from src.transactions.purchase_form import ConfirmationForm, ConfirmationFormError, PurchaseForm, PurchaseFormError, TransactionError


BASE_DIR='/var/www'

logging.basicConfig(format='%(asctime)s %(threadName)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    filename=os.path.join(BASE_DIR, 'logs', 'flask.log'),
                    level=logging.INFO)

app = Flask(__name__)


# ----------------------------------------------------------------------------
# ----------------------------- CLIENT ENDPOINTS -----------------------------
# ----------------------------------------------------------------------------

@app.route('/current_holdings', methods=['GET'])
def current_holdings():
    """
    Endpoint for querying the latest quantity vector and liquidity parameter
        * Requires JWT Authorization header.
        * Market should be specified in EITHER 'market' url argument, with a single market id
          e.g. https://engine.sportfolios.co.uk/spot_quantities?market=1:8:18378T
          OR a 'markets' url parameter, with multiple markets specified
          e.g.  https://engine.sportfolios.co.uk/spot_quantities?markets=1:8:18378T,1182:8:18378P
        
    Returns:
        JSON response: e.g. 
                    for market  {'b': 4000, 'x': [1, 2, 3]}
                    for markets {'1:8:18378T': {'b': 4000, 'x': [1, 2, 3]}, '1182:8:18378P': {'b': 2000, 'N': 400}}
    """

    if request.headers.get('Authorization') is None:
        return f'Authorization ID needed', 407

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'GET; current_holdings; unknown; {remote_ip}; fail; {info}')
        return message, code

    market = request.args.get('market')
    markets = request.args.get('markets')

    if market is None and markets is None:
        logging.info(f'GET; current_holdings; {info["user_id"]}; {remote_ip}; fail; No market specified')
        return 'No market specified', 400
 
    elif market is None:
        
        markets_list = list(set([m for m in markets.split(',') if m != '']))
        if len(markets_list) > 100:
            return 'Request has exceeded markets limit. ', 400

        result = read_data.get_multiple_latest_quantities(markets_list)
        logging.info(f'GET; current_holdings; {info["user_id"]}; {remote_ip}; success; {markets}')
        for m, quantity in result.items():
            if quantity is None:
                logging.warn(f'GET; current_holdings; {info["user_id"]}; {remote_ip}; warning; {m} is missing from Redis')
        return jsonify(result), 200

    elif markets is None:

        try:
            result = read_data.get_latest_quantities(market)
            logging.info(f'GET; current_holdings; {info["user_id"]}; {remote_ip}; success; {market}')
            return jsonify(result), 200

        except ResourceNotFoundError:
            logging.warn(f'GET; current_holdings; {info["user_id"]}; {remote_ip}; fail; Unknown market specified {market}')
            return f'Market {market} does not exist', 404

    else:
        return 'market and markets specified', 400



@app.route('/historical_holdings', methods=['GET'])
def historical_holdings():
    """
    Endpoint for querying the historical quantity vector and liquidity parameter
        * Requires JWT Authorization header.
        * Market should be specified in 'market' url argument, with a single market id
        e.g. https://engine.sportfolios.co.uk/historical_quantities?market=T1:8:17420

    Returns:
        JSON response: e.g. {'b': {'h': {t1: 4000, t2: 4000, ...],
                                   'd': {ta: 4000, tb: 4000, ...], ...},
                             'x': {'h': {[1, 2, 3, 4, ...],
                                          [2, 3, 4, 5, ...], ... },
                                   'd': [[4, 5, 6, 7, ...],
                                          [6, 7, 8, 9, ...], ...], ...}
                             }
    """

    if request.headers.get('Authorization') is None:
        return f'Authorization ID needed', 407

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'GET; historical_holdings; unknown; {remote_ip}; fail; {info}')
        return message, code

    market = request.args.get('market')
    markets = request.args.get('markets')

    if market is None and markets is None:
        logging.info(f'GET; historical_holdings; {info["user_id"]}; {remote_ip}; fail; No market specified')
        return 'No market specified', 400

    elif market is None:

        markets_list = list(set([m for m in markets.split(',') if m != '']))
        if len(markets_list) > 100:
            return 'Request has exceeded markets limit. ', 400

        result = read_data.get_multiple_historical_quantities(markets_list)
        logging.info(f'GET; historical_holdings; {info["user_id"]}; {remote_ip}; success; {markets}')
        for m, quantity in result.items():
            if quantity is None:
                logging.warn(f'GET; historical_holdings; {info["user_id"]}; {remote_ip}; warning; {m}:hist is missing from Redis')
        return jsonify(result), 200

    elif markets is None:

        try:
            result = read_data.get_historical_quantities(market)
            logging.info(f'GET; historical_holdings; {info["user_id"]}; {remote_ip}; success; {market}')
            return jsonify(result), 200

        except ResourceNotFoundError:
            logging.info(f'GET; historical_holdings; {info["user_id"]}; {remote_ip}; fail; Unknown market specified {market}')
            return f'Market {market} does not exist', 404

    else:
        return 'market and markets specified', 400



@app.route('/purchase', methods=['POST'])
def purchase():
    """
    Attempt to make a purchase of a certain quantity vector for a certain market

        * Requires JWT Authorization header
        * Request body should contain the following keys:

            portfolioId: the desired portfolio id
            market: the desired market
            quantity: the desired quantity vector
            price: the desired price to 2 dp

    Response contains a cancellation ID. If the user wishes to confirm or cancel an order 
    for a price different to what they specified, they can send back a request and include 
    this id in the post body. (see confirm_order() below)

    Returns:
        if price is agreed,     JSON: {'success': True,  'price': sealed_price, 'cancelId': None}
        if price is not agreed, JSON: {'success': False, 'price': sealed_price, 'cancelId': id}
    """

    if request.headers.get('Authorization') is None:
        return f'Authorization ID needed', 407

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'POST; purcharse; unknown; {remote_ip}; fail; {info}')
        return message, code
    
    try:
        purchase_form = PurchaseForm(info['uid'], request.form)
    except PurchaseFormError as E:
        logging.warning(f'Invalid purchase form: {E}')
        return f'Invalid purchase form: {E}', 400

    try:
        return purchase_form.attempt_purchase(), 200
    except TransactionError as E:
        logging.warning(f'Transaction failed: {E}')
        return f'Transaction failed: {E}', 400


@app.route('/confirm_order', methods=['POST'])
def confirm_order():
    """
    Confirm or cancel order, given that the price was not what was requested. This is a follow-up 
    request, which should 

        * Requires JWT Authorization header
        * Request body should contain the following keys:

            cancelId: the ID of the cancellation
            confirm: true (do not cancel order) or false (continue with order cancellation)

    """

    if request.headers.get('Authorization') is None:
        return f'Authorization ID needed', 407

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'POST; confirm_order; unknown; {remote_ip}; fail; {info}')
        return message, code

    try:
        confirmation_form = ConfirmationForm(info['uid'], request.form)
    except ConfirmationFormError as E:
        logging.warning(f'Invalid confirmation form: {E}')
        return f'Invalid purchase form: {E}', 400
    except PurchaseFormError as E:
        logging.warning(f'Invalid confirmation form: {E}')
        return f'Invalid purchase form: {E}', 400

    try:
        message = confirmation_form.process_request()
        logging.info(f'User request successfully implemented')
        return message, 200
    except ConfirmationFormError as E:
        logging.warning(f'Could not process user request: {E}')
        return f'Unable to process request: {E}', 400



@app.route('/create_portfolio', methods=['POST'])
def create_portfolio():
    """
    Create a new portfolio

        * Requires JWT Authorization header
        * Request body should contain the following keys:

            name: (string) the portfolio name
            public: (bool) whether the portfolio is public

    """

    logging.info('Creating new portfolio')

    if request.headers.get('Authorization') is None:
        return f'Authorization ID needed', 407

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'POST; create_portfolio; unknown; {remote_ip}; fail; {info}')
        return message, code

    uid = info['uid']
    name = request.form.get('name')
    public = request.form.get('public')

    logging.info('Everything fine so far')

    if name is None or public is None:
        return 'Form is missing one of the following: name, public', 400

    try:
        public = orjson.loads(public)
    except:
        return '"public" entry is malformed', 400

    logging.info('almost there')

    try:
        pid = add_new_portfolio(uid, name, public)
        return jsonify({'success': True, 'portfolioId': pid}), 200
    except Exception as E:
        logging.error(E)
        return 'Unable to process request at this time', 400




# ----------------------------------------------------------------------------
# ----------------------------- ADMIN ENDPOINTS ------------------------------
# ----------------------------------------------------------------------------



@app.route('/init_redis', methods=['GET'])
def init_redis():

    if request.headers.get('Authorization') is None:
        return f'Authorization ID needed', 407

    success, message = verify_admin(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not success:
        return message

    init_redis_f()

    return 'Initialised Redis'


@app.route('/update_b', methods=['POST'])
def update_b():

    if request.headers.get('Authorization') is None:
        return f'Authorization ID needed', 407

    success, message = verify_admin(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not success:
        return message

    market = None
    b = None

    if request.method == 'POST':
        for market, b in request.form.items():
            update_b_redis(market, b)

    return f'set {market} b to {b}'





if __name__ == '__main__':

    app.run()


