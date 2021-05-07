import os
import logging
import json
from flask import Flask, request, jsonify
import redis

from src.firebase.authentication import verify_user_token, verify_admin
from src.firebase.data import check_portfolio, make_purchase
from src.database.database import init_db, log_order
from src.redis_utils.init_redis_db import init_redis_f
from src.redis_utils.update import  update_b_redis
from src.redis_utils.get_data import  get_spot_prices, get_spot_quantities, get_historical_quantities
from src.redis_utils.write_data import attempt_purchase
from src.redis_utils.exceptions import ResourceNotFoundError
from src.redis_utils.queues import schedule_cancellation, cancel_scheduled_cancellation, execute_cancellation_early

BASE_DIR='/var/www'

logging.basicConfig(format='%(asctime)s %(threadName)s %(levelname)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    filename=os.path.join(BASE_DIR, 'logs', 'flask.log'),
                    level=logging.INFO)

app = Flask(__name__)
init_db()

@app.route('/spot_prices', methods=['GET'])
def spot_prices():
    """
    Endpoint for querying the latest back spot price for one or more markets.
        * Requires JWT Authorization header.
        * Markets should be specified in 'markets' url argument, with market ids seperated by commas
        e.g. https://engine.sportfolios.co.uk/spot_prices?markets=T1:8:17420,T8:8:17420,T6:8:17420

    Returns:
        JSON response: e.g. {'T1:8:17420': 4.52, 'T8:8:17420': 2.88, 'T6:8:17420': 8.93}
    """

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'GET; spot_prices; unknown; {remote_ip}; fail; {info}')
        return message, code

    markets = request.args.get('markets')

    if markets is None:
        logging.info(f'GET; spot_prices; {info["user_id"]}; {remote_ip}; fail; No market specified')
        return jsonify({}), 200

    result = jsonify(get_spot_prices(markets.split(',')))
    logging.info(f'GET; spot_prices; {info["user_id"]}; {remote_ip}; success; {markets}')

    return result, 200


@app.route('/spot_quantities', methods=['GET'])
def spot_quantities():
    """
    Endpoint for querying the latest quantity vector and liquidity parameter
        * Requires JWT Authorization header.
        * Market should be specified in 'market' url argument, with a single market id
        e.g. https://engine.sportfolios.co.uk/spot_quantities?market=T1:8:17420

    Returns:
        JSON response: e.g. {'b': 4000, 'x': [1, 2, 3, 4, 5, 6, ... , 20]}
    """

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'GET; spot_quantities; unknown; {remote_ip}; fail; {info}')
        return message, code

    market = request.args.get('market')

    if market is None:
        logging.info(f'GET; spot_quantities; {info["user_id"]}; {remote_ip}; fail; No market specified')
        return jsonify({'b': None, 'x': None}), 200

    try:
        result = jsonify(get_spot_quantities(market)), 200
        logging.info(f'GET; spot_quantities; {info["user_id"]}; {remote_ip}; success; {market}')
        return result

    except ResourceNotFoundError:
        logging.info(f'GET; spot_quantities; {info["user_id"]}; {remote_ip}; fail; Unknown market specified {market}')
        return jsonify({'b': None, 'x': None}), 200


@app.route('/historical_quantities', methods=['GET'])
def historical_quantities():
    """
    Endpoint for querying the historical quantity vector and liquidity parameter
        * Requires JWT Authorization header.
        * Market should be specified in 'market' url argument, with a single market id
        e.g. https://engine.sportfolios.co.uk/historical_quantities?market=T1:8:17420

    Returns:
        JSON response: e.g. {'b': {'bh': {t1: 4000, t2: 4000, ...],
                                   'bd': {ta: 4000, tb: 4000, ...], ...},
                             'x': {'xh': {[1, 2, 3, 4, ...],
                                          [2, 3, 4, 5, ...], ... },
                                   'xd': [[4, 5, 6, 7, ...],
                                          [6, 7, 8, 9, ...], ...], ...}
                             }
    """

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'GET; historical_quantities; unknown; {remote_ip}; fail; {info}')
        return message, code

    market = request.args.get('market')

    if market is None:
        logging.info(f'GET; historical_quantities; {info["user_id"]}; {remote_ip}; fail; No market specified')
        return jsonify({'bhist': None, 'xhist': None}), 200

    try:
        result = jsonify(get_historical_quantities(market)), 200
        logging.info(f'GET; historical_quantities; {info["user_id"]}; {remote_ip}; success; {market}')
        return result

    except ResourceNotFoundError:
        logging.info(f'GET; historical_quantities; {info["user_id"]}; {remote_ip}; fail; Unknown market specified {market}')
        return jsonify({'bhist': None, 'xhist': None}), 200


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
        if price is agreed,     JSON: {'success': True,  'price': sealed_price, 'cancellationId': None}
        if price is not agreed, JSON: {'success': False, 'price': sealed_price, 'cancellationId': id..}

    """

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'POST; purcharse; unknown; {remote_ip}; fail; {info}')
        return message, code

    uid = info['uid']
    portfolioId = request.form.get('portfolioId')
    market = request.form.get('market')
    quantity = request.form.get('quantity')
    price = request.form.get('price')
    
    if any([i is None for i in [market, portfolioId, quantity, price]]):
        logging.info(f'POST; purcharse; {uid}; {remote_ip}; fail; Malformed body')
        return 'Malformed body', 400

    quantity = json.loads(quantity)
    price = float(price)

    if not check_portfolio(portfolioId, uid):
        logging.info(f'POST; purcharse; {uid}; {remote_ip}; fail; Invalid portfolio id: {portfolioId}')
        return 'Invalid portfolio ID', 400

    try: 
        success, sealed_price = attempt_purchase(uid, portfolioId, market, quantity, price)
    except redis.WatchError:
        logging.warning(f'WATCH ERROR; purchase; {uid}; {remote_ip}; {portfolioId}; {market}; failed')
        return 'Too much trading activity', 409
    except ResourceNotFoundError:
        logging.info(f'RESOURCE NOT FOUND; purcharse; {uid}; {remote_ip}; {portfolioId}; {market}; failed')
        return f'Market {market} not found'


    if success:
        logging.info(f'POST; purcharse; {uid}; {remote_ip}; success; {portfolioId}_{market}_{quantity}_{sealed_price}')
        log_order(info, market, portfolioId, quantity, sealed_price)
        make_purchase(uid, portfolioId, market, quantity, sealed_price)
        log_order(info, portfolioId, market, quantity, price)
        return jsonify({'success': True, 'price': sealed_price, 'cancellationId': None}), 200

    else:
        cancelId = schedule_cancellation(uid, portfolioId, market, quantity, sealed_price)
        logging.info(f'POST; purcharse; {uid}; {remote_ip}; to be confirmed; {cancelId}_{portfolioId}_{market}_{quantity}_{sealed_price}')
        return jsonify({'success': False, 'price': sealed_price, 'cancelId': cancelId}), 200


@app.route('/confirm_order', methods=['POST'])
def confirm_order():
    """
    Confirm or cancel order, given that the price was not what was requested. This is a follow-up 
    request, which should 

        * Requires JWT Authorization header
        * Request body should contain the following keys:

            cancelId: the ID of the cancellation
            confirm: true (do not cancel order) or false (continue with order cancellation)
            portfolioId: the portfolio id
            market: the market
            quantity: the quantity vector
            price: the price to 2 dp

    """

    authorised, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not authorised:
        message, code = info
        logging.info(f'POST; confirm_order; unknown; {remote_ip}; fail; {info}')
        return message, code

    uid = info['uid']
    cancelId = request.form.get('cancelId')
    confirm = request.form.get('confirm')
    portfolioId = request.form.get('portfolioId')
    market = request.form.get('market')
    quantity = request.form.get('quantity')
    price = request.form.get('price')

    if any([i is None for i in [cancelId, confirm, portfolioId, market, quantity, price]]):
        logging.info(f'POST; confirm_order; {uid}; {remote_ip}; fail; Malformed body')
        return f'Malformed body', 400

    confirm = json.loads(confirm)
    quantity = json.loads(quantity)
    price = float(price)

    if not check_portfolio(portfolioId, uid):
        logging.info(f'POST; purcharse; {uid}; {remote_ip}; fail; Invalid portfolio id: {portfolioId}')
        return 'Invalid portfolio ID', 400

    if confirm is True:
        try:
            cancel_scheduled_cancellation(cancelId, uid, portfolioId, market, quantity, price)
            logging.info(f'POST; confirm_order; {uid}; {remote_ip}; confirmed; {cancelId}_{portfolioId}_{market}_{quantity}_{price}')
            make_purchase(uid, portfolioId, market, quantity, price)
            log_order(info, portfolioId, market, quantity, price)
            return 'Order confirmed'

        except (ValueError, ResourceNotFoundError) as E:
            logging.info(f'POST; confirm_order; {uid}; {remote_ip}; error confirming; {cancelId}_{portfolioId}_{market}_{quantity}_{price}')
            logging.info(f'{E}')
            # return 'Could not confirm order', 400
            raise E
    else:
        try:
            execute_cancellation_early(cancelId, uid, portfolioId, market, quantity, price)
            logging.info(f'POST; confirm_order; {uid}; {remote_ip}; cancelled;  {cancelId}_{portfolioId}_{market}_{quantity}_{price}')
            return 'Order cancelled', 200

        except (ValueError, ResourceNotFoundError):
            logging.info(f'POST; confirm_order; {uid}; {remote_ip}; error running cancelation early;  {cancelId}_{portfolioId}_{market}_{quantity}_{price}')
            return 'Order cancelled', 200 



@app.route('/portfolio_history', methods=['GET'])
def portfolio_history():

    success, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not success:
        error_message, code = info
        return error_message, code
    else:
        return 'Portfolio History'

@app.route('/init_redis', methods=['GET'])
def init_redis():

    success, message = verify_admin(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not success:
        return message

    init_redis_f()

    return 'Initialised Redis'

@app.route('/update_b', methods=['POST'])
def update_b():

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
