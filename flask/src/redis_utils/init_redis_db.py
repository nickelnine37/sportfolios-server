import time
import redis
import os
import json

def init_redis_f():

    ## IS EVERYTHING 100% DOUBLES????????? (except time.....!!!!!!)

    redis_db = redis.Redis(host='redis', port=6379, db=0)

    BASE_DIR = '/var/www'

    with open(os.path.join(BASE_DIR, 'data', 'players.json'), 'r')  as f:
        players = json.loads(f.read())

    with open(os.path.join(BASE_DIR, 'data', 'teams.json'), 'r')  as f:
        teams = json.loads(f.read())

    # with open(os.path.join(BASE_DIR, 'data', 'time.json'), 'r')  as f:
    #     times = json.loads(f.read())
    
    with redis_db.pipeline() as pipe:

        for player_id, player_Nb in players.items():

            pipe.set(player_id, json.dumps({'N': player_Nb['N'], 'b': player_Nb['b']}))
            pipe.set(player_id + ':hist', json.dumps({'N': {'h': [player_Nb['N']], 'd': [player_Nb['N']], 'w': [player_Nb['N']], 'm': [player_Nb['N']], 'M': [player_Nb['N']]}, 
                                                      'b': {'h': [player_Nb['b']], 'd': [player_Nb['b']], 'w': [player_Nb['b']], 'm': [player_Nb['b']], 'M': [player_Nb['b']]}}))

        for team_id, team_xb in teams.items():

            pipe.set(team_id, json.dumps({'x': team_xb['x'], 'b': team_xb['b']}))
            pipe.set(team_id + ':hist', json.dumps({'x': {'h': [team_xb['x']], 'd': [team_xb['x']], 'w': [team_xb['x']], 'm': [team_xb['x']], 'M': [team_xb['x']]}, 
                                                    'b': {'h': [team_xb['b']], 'd': [team_xb['b']], 'w': [team_xb['b']], 'm': [team_xb['b']], 'M': [team_xb['b']]}}))

        
        t = int(time.time())
        pipe.set('time', json.dumps({'h': [t], 'd': [t], 'w': [t], 'm': [t], 'M': [t]}))

        # del players
        # del teams

        pipe.execute()


