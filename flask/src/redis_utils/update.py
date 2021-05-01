import redis
import os
from src.redis_utils.arrays import toRedis, fromRedis
import numpy as np
import json

redis_db = redis.Redis(host='redis', port=6379, db=0)

def update_b_redis(market: str, value: str):
    redis_db.hset(market, "b",  float(value))  # set to 67