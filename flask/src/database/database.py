import sqlite3
import time
import os

BASE_DIR = '/var/www'


def init_db():

    conn = sqlite3.connect(os.path.join(BASE_DIR, 'database.db'))
    c = conn.cursor()

    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='price_requests' ''')

    if c.fetchone()[0]==1 :
        pass
    else:
        print('price_requests table does not exist. Creating...')
        conn.execute('CREATE TABLE price_requests (username TEXT, email TEXT, uid TEXT, auth_time INT, server_time INT, markets TEXT)')


    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='orders' ''')

    if c.fetchone()[0]==1 :
        pass
    else:
        print('orders table does not exist. Creating...')
        conn.execute('CREATE TABLE orders (username TEXT, email TEXT, uid TEXT, auth_time INT, server_time INT, asset TEXT, portfolioID TEXT, q0 DOUBLE, q1 DOUBLE, q2 DOUBLE, q3 DOUBLE, q4 DOUBLE, q5 DOUBLE, q6 DOUBLE, q7 DOUBLE, q8 DOUBLE, q9 DOUBLE, q10 DOUBLE, q11 DOUBLE, q12 DOUBLE, q13 DOUBLE, q14 DOUBLE, q15 DOUBLE, q16 DOUBLE, q17 DOUBLE, q18 DOUBLE, q19 DOUBLE)')


    conn.close()


def log_price_query(info: dict, markets: list):

    with sqlite3.connect(os.path.join(BASE_DIR, 'database.db')) as con:
        cur = con.cursor()
        cur.execute(
            f"INSERT INTO price_requests (username, email, uid, auth_time, server_time, markets) VALUES (?,?,?,?,?,?)",
            (info['name'], info['email'], info['user_id'], info['auth_time'], int(time.time()), ','.join(markets)))

