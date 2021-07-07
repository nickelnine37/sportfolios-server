import time
import json
import redis
import orjson

class Timer:
    """
    Convenience class for implementing timed context managers
    """

    def __init__(self):
        pass

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.t = time.time() - self.t0


class RedisExtractor:
    """
    Small class to group methods relating to reading and writing from Redis
    """

    def __init__(self):
        self.redis_db = redis.Redis(host='redis', port=6379, db=0)

    def get_current_and_historical_holdings(self, markets: list):
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

    def send_historical_holdings(self, all_hist_new: dict):
        """
        Given a new dictionary mapping string market to historical holdings dict, send this to redis
        """

        with self.redis_db.pipeline() as pipe:

            for team, hist_new in all_hist_new.items():
                pipe.set(team + ':hist', orjson.dumps(hist_new))

            pipe.execute()

    def get_time(self):
        return orjson.loads(self.redis_db.get('time'))

    def set_time(self, time_new: dict):
        self.redis_db.set('time', orjson.dumps(time_new))
