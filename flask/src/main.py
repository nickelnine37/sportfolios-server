from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore, auth
import os
import random
import redis
import sys
from src.authentication.authentication import verify_token
from src.database.database import init_db, log_price_query

ADMIN_SDK_KEY = 'sportfolios-431c6-firebase-adminsdk-bq76v-f490ad544c.json'
FILE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR='/var/www'

def get_prices(assets: list):
    return {tid: float(f'{20 * random.random():.2f}') for tid in assets}


# def main():

app = Flask(__name__)
init_db()
cred = credentials.Certificate(os.path.join(BASE_DIR, ADMIN_SDK_KEY))
default_app = firebase_admin.initialize_app(cred)
db = firestore.client()

redis_db = redis.Redis(host='redis', port=6379, db=0)


@app.route("/")
def index():
    return str(os.listdir())

@app.route('/spot_prices', methods=['GET'])
def spot_prices():
    """
    Endpoint for querying the latest spot price for one or more market.
    * Requires JWT 'Authorization' header.
    * Markets should be specified in 'markets' url argument, with market ids seperated by commas
    e.g. https://engine.sportfolios.co.uk/spot_prices?markets=T1:8:17420,T8:8:17420,T6:8:17420
    """

    token = request.headers.get('Authorization')
    success, info = verify_token(token)
    
    print(info)
    redis_db.set('most_recent_request', info['email'])

    if not success:
        error_message, code = info
        return error_message, code
    else:

        markets = request.args.get('markets')

        if markets is None:
            return jsonify({}), 200
        else:
            markets = markets.split(',')
            response = get_prices(markets)
            log_price_query(info, markets)
            return jsonify(response), 200

@app.route('/historical_prices', methods=['GET'])
def historical_prices():

    token = request.headers.get('Authorization')
    success, info = verify_token(token)

    if not success:
        error_message, code = info
        return error_message, code
    else:

        return redis_db.get('most_recent_request')

@app.route('/portfolio_history', methods=['GET'])
def portfolio_history():

    token = request.headers.get('Authorization')
    success, info = verify_token(token)

    if not success:
        error_message, code = info
        return error_message, code
    else:

        return 'Portfolio History'

    # return app


if __name__ == '__main__':
#
#     app = main()
    app.run()