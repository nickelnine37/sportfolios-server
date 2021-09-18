import time
# import json
import redis
import orjson
from firebase_admin import credentials, firestore, initialize_app
from typing import Tuple, List

class Timer:
    """
    Convenience class for implementing timed context managers
    """

    def __init__(self):
        self.pause_time = 0
        self.p0 = None

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.t = time.time() - self.t0 - self.pause_time

    def pause(self):
        if self.p0 is None:
            self.p0 = time.time()

    def resume(self):
        if self.p0 is not None:
            self.pause_time += time.time() - self.p0
            self.p0 = None

    


class RedisExtractor:
    """
    Small class to group methods relating to reading and writing from Redis
    """

    def __init__(self):
        self.redis_db = redis.Redis(host='redis', port=6379, db=0)

    def get_current_and_historical_holdings(self, markets: list) -> Tuple[List[dict], List[dict]]:
        """
        Get the raw (string) curent and historical holdings for a list of markets
        """

        with self.redis_db.pipeline() as pipe:

            for market in markets:

                pipe.get(market)
                pipe.get(market + ':hist')

            results = pipe.execute()

        return ([orjson.loads(result) if result is not None else None for result in results[::2]],
                [orjson.loads(result) if result is not None else None for result in results[1::2]])

    def get_current_holdings(self, markets: list):

        with self.redis_db.pipeline() as pipe:

            for market in markets:
                pipe.get(market)

            results = pipe.execute()

        return [orjson.loads(result) if result is not None else None for result in results]

    def write_historical_holdings(self, all_hist_new: dict):
        """
        Given a new dictionary mapping string market to historical holdings dict, send this to redis
        """

        with self.redis_db.pipeline() as pipe:

            for market, hist_new in all_hist_new.items():
                pipe.set(market + ':hist', orjson.dumps(hist_new))

            pipe.execute()

    def write_current_holdings(self, all_current_new: dict) -> None:
        """
        Given a new dictionary mapping string market to current holdings dict, send this to redis
        """

        with self.redis_db.pipeline() as pipe:

            for market, current_new in all_current_new.items():
                pipe.set(market, orjson.dumps(current_new))

            pipe.execute()

    def get_time(self) -> dict:
        return orjson.loads(self.redis_db.get('time'))

    def set_time(self, time_new: dict) -> None:
        self.redis_db.set('time', orjson.dumps(time_new))



class Firebase:
    """
    Singleton class. This is necessary to ensure the app is only initialised once. Use this to access firebase options. 
    The singleton is enforced by using only the global variable "firebase" and never instatiating the Firebase class
    outside of this file. 
    """

    def __init__(self) -> None:
        
        self.default_app = initialize_app(credentials.Certificate('/var/www/sportfolios-431c6-firebase-adminsdk-bq76v-f490ad544c.json'))
        self.db = firestore.client()

        self.teams_collection = self.db.collection(u'teams')
        self.players_collection = self.db.collection(u'players')
        self.portfolios_collection = self.db.collection(u'portfolios')


firebase = Firebase()

