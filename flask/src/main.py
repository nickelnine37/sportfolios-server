import os
import sys
import logging
import random
import json

from flask import Flask, request, jsonify
import redis
import firebase_admin
from firebase_admin import credentials, firestore

from src.authentication.authentication import verify_user_token, verify_admin
from src.database.database import init_db, log_price_query
from src.redis_utils.init_redis_db import init_redis_f
from src.redis_utils.update import  update_b_redis
from src.redis_utils.get_data import  get_spot_prices, get_spot_quantities


ADMIN_SDK_KEY = 'sportfolios-431c6-firebase-adminsdk-bq76v-f490ad544c.json'
BASE_DIR='/var/www'

def get_prices(assets: list):
    return {tid: float(f'{20 * random.random():.2f}') for tid in assets}


logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    filename=os.path.join(BASE_DIR, 'logs', 'logfile.log'),
                    level=logging.INFO)

app = Flask(__name__)
init_db()
cred = credentials.Certificate(os.path.join(BASE_DIR, ADMIN_SDK_KEY))
default_app = firebase_admin.initialize_app(cred)
db = firestore.client()

redis_db = redis.Redis(host='redis', port=6379, db=0)


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

    success, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not success:
        logging.info(f'GET; spot_prices; unknown; {remote_ip}; fail; {info}')
        return info

    markets = request.args.get('markets')

    if markets is None:
        logging.info(f'GET; spot_prices; {info["user_id"]}; {remote_ip}; fail; No market specified')
        return jsonify({}), 200

    result = jsonify(get_spot_prices(markets.split(','))), 200
    logging.info(f'GET; spot_prices; {info["user_id"]}; {remote_ip}; success; {markets}')
    return result


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

    success, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not success:
        logging.info(f'GET; spot_quantities; unknown; {remote_ip}; fail; {info}')
        return info

    market = request.args.get('market')

    if market is None:
        logging.info(f'GET; spot_quantities; {info["user_id"]}; {remote_ip}; fail; No market specified')
        return jsonify({'b': None, 'x': None}), 200

    return jsonify(get_spot_quantities(market)), 200


@app.route('/historical_prices', methods=['GET'])
def historical_quantities():

    success, info = verify_user_token(request.headers.get('Authorization'))
    remote_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

    if not success:
        error_message, code = info
        return error_message, code
    else:

        return str(redis_db.hgetall('T68:82:17361'))

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
#
#     app = main()
    app.run()
