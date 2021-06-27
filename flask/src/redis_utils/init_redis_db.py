import time

import redis
import os
from src.redis_utils.arrays import toRedis, fromRedis
import numpy as np
import json
import logging

def init_redis_f():

    ## IS EVERYTHING 100% DOUBLES????????? (except time.....!!!!!!)

    redis_db = redis.Redis(host='redis', port=6379, db=0)

    BASE_DIR = '/var/www'

    with open(os.path.join(BASE_DIR, 'data', 'player_hist.json'), 'r')  as f:
        players = json.loads(f.read())

    with open(os.path.join(BASE_DIR, 'data', 'team_hist.json'), 'r')  as f:
        teams = json.loads(f.read())

    with open(os.path.join(BASE_DIR, 'data', 'time.json'), 'r')  as f:
        times = json.loads(f.read())
    
    with redis_db.pipeline() as pipe:

        for player_id, player_hist in players.items():

            x_current = player_hist['x']['h'][-1]
            b_current = player_hist['b']['h'][-1]

            pipe.set(player_id, json.dumps({'x': x_current, 'b': b_current}))
            pipe.set(player_id + ':hist', json.dumps(player_hist))

        for team_id, team_hist in teams.items():

            x_current = team_hist['x']['h'][-1]
            b_current = team_hist['b']['h'][-1]

            pipe.set(team_id, json.dumps({'x': x_current, 'b': b_current}))
            pipe.set(team_id + ':hist', json.dumps(team_hist))

        pipe.set('time', json.dumps(times))

        del players
        del teams

        pipe.execute()


