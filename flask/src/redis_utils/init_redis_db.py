import time

import redis
import os
from src.redis_utils.arrays import toRedis, fromRedis
import numpy as np
import json
import logging

def init_redis_f():

    ## IS EVERYTHING 100% DOUBLES?????????

    redis_db = redis.Redis(host='redis', port=6379, db=0)

    BASE_DIR = '/var/www'

    # with open(os.path.join(BASE_DIR, 'data', 'player_hist.json'), 'r')  as f:
    #     players = json.loads(f.read())

    with open(os.path.join(BASE_DIR, 'data', 'teams_hist.json'), 'r')  as f:
        teams = json.loads(f.read())

    # with open(os.path.join(BASE_DIR, 'data', 'times.json'), 'r')  as f:
    #     times = json.loads(f.read())
    
    with redis_db.pipeline() as pipe:

        # for player in players.keys():

        #     x_current = players[player]['x']['h'][-1]
        #     b_current = players[player]['b']['h'][-1]

        #     pipe.set(player + '_new', json.dumps({'x': x_current, 'b': b_current}))
        #     pipe.set(player + ':hist_new', json.dumps(players[player]))

        for team in teams.keys():

            x_current = teams[team]['x']['h'][-1]
            b_current = teams[team]['b']['h'][-1]

            pipe.set(team + '_new', json.dumps({'x': x_current, 'b': b_current}))
            pipe.set(team + ':hist_new', json.dumps(teams[team]))

        # pipe.set('time', json.dumps(times))

        # del players
        # del teams

        pipe.execute()


