import sqlite3
import time
import os
import logging

BASE_DIR = '/var/www'


def init_db():


    with sqlite3.connect(os.path.join(BASE_DIR, 'database.db')) as con:
        cur = con.cursor()

        cur.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='orders' ''')

        if cur.fetchone()[0]==1 :
            pass
        else:
            logging.info('orders table does not exist. Creating...')
            cur.execute('CREATE TABLE orders (username TEXT, email TEXT, uid TEXT, server_time INT,  portfolioId TEXT, market TEXT, quantity TEXT, price DOUBLE)')


def log_order(userInfo: dict, portfolioId: str, market: str, quantity: list, price: float):

    with sqlite3.connect(os.path.join(BASE_DIR, 'database.db')) as con:
        cur = con.cursor()
        cur.execute("INSERT INTO orders (username, email, uid, server_time, portfolioId, market, quantity, price) VALUES (?,?,?,?,?,?,?,?)",
                   (userInfo['name'], userInfo['email'], userInfo['uid'], int(time.time()), portfolioId, market, str(quantity), price))

