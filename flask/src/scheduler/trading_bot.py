from os import replace
from scheduler_utils import RedisExtractor, Timer
import numpy as np
import logging
from lmsr.long_short import LongShortMarketMaker
from scipy.optimize import brentq
from typing import Union
import time
import orjson
import json


class TradingBot:

    def __init__(self, t: int=0) -> None:
        
        self.B_factor = 0.25
        self.redis_extractor = RedisExtractor()
        self.t = t

    def trade(self):

        if self.t % 10 == 2:

            with open('/var/www/data/player_ms.json') as f:
                player_ms = orjson.loads(f.read())

            with open('/var/www/data/team_ms.json') as f:
                team_ms = orjson.loads(f.read())

            trades = []

            with Timer() as player_timer:
                
                selected_players = np.random.choice(list(player_ms.keys()), size=len(player_ms) // 6, replace=False).tolist()
                player_holdings = self.redis_extractor.get_current_holdings(selected_players)
                
                for player, current_holdings in zip(selected_players, player_holdings):

                    trade = self.optimal_trade_player(player, player_ms[player], current_holdings['N'], current_holdings['b'])
                    if trade['cost'] != 0:
                        current_holdings['N'] += trade['quantity'] * (-1) ** (~trade['long'])
                        trades.append(trade)

            with Timer() as team_timer:
                
                selected_teams = np.random.choice(list(team_ms.keys()), size=len(team_ms) // 6, replace=False).tolist()
                team_holdings = self.redis_extractor.get_current_holdings(selected_teams)

                for team, current_holdings in zip(selected_teams, team_holdings):
                    
                    trade = self.optimal_trade_team(team, team_ms[team], current_holdings['x'], current_holdings['b'])

                    if trade['cost'] != 0:
                        current_holdings['x'] = (np.asarray(current_holdings['x']) + np.asarray(trade['quantity'])).tolist()
                        trades.append(trade)

            with Timer() as redis_timer:
                
                # these should have been edited in-place for memory efficiency
                self.redis_extractor.write_current_holdings(selected_players + selected_teams, player_holdings + team_holdings)

                with open(f'/var/www/logs/trades/{int(time.time())}.json', 'w') as f:
                    json.dump(trades, f)

            logging.info(f'TRADING BOT: t = {self.t}. Player time: {player_timer.t:.4f}. Team time: {team_timer.t:.4f}. Redis time: {redis_timer.t:.4f}')
        
        self.t += 2


    def optimal_trade_team(self, market: str, m: Union[list, np.ndarray], x: Union[list, np.ndarray], b: float):

        try:
            m = np.asarray(m)
            x = np.asarray(x)
            B = self.B_factor * b

            # regular C function
            def C(x_):
                xmax = x_.max()
                return xmax + b * np.log(np.exp((x_ - xmax) / b).sum())

            # regular trading cost function
            def cost(q_):
                return C(x + q_) - C(x)

            def intersects(j):

                # the cost of q_opt when the j smallest dimensions have been set to zero
                def cc(k):
                    q_ = q_opt.copy() + k
                    q_[sorted_dims[:j]] = 0
                    return cost(q_)

                kmax = B - q_opt[j] + C(x) - x[j]

                k_ = brentq(lambda k: cc(k) - B, kmin, kmax)
                q_ = q_opt.copy() + k_
                q_[sorted_dims[:j]] = 0

                return q_
                
            N = len(x)
            assert len(m) == N
            
            q_opt = b * np.log(m) - x
            sorted_dims = np.argsort(q_opt)    
            kmin = -q_opt.max()

            for j in range(N):
                
                q = intersects(j)
                q = np.round(q - q.min(), 2)
                c = cost(q)

                # only make significant trades
                if c < 10:
                    return {'market': market, 'quantity': 0, 'cost': 0, 'long': None}
                                
                if (q >= 0).all():
                    return {'market': market, 'quantity': q.tolist(), 'team': True, 'cost': float(round(c, 2)), 'long': None}
                
        except Exception as E:
            logging.error(f'TRADING BOT failed for {market}: {E}')
            return {'market': market, 'quantity': 0, 'cost': 0, 'long': None}


    def optimal_trade_player(self, market: str, m: float, N: float, b: float):

        assert 0 <= m <= 1

        market_maker = LongShortMarketMaker(market, N, b)

        # the market price already reflects our belief, so make no trade
        if abs(market_maker.spot_value([1, 0]) - m) < 5e-4:
            return {'market': market, 'quantity': 0, 'team': False, 'long': True, 'cost': 0}

        # budget is some multiple of b
        B = self.B_factor * b

        try:
            # how many longs would we need to buy to shift the whole market to our belief?
            n0 = brentq(lambda n: LongShortMarketMaker(market, n, b).spot_value([1, 0]) - m, -40 * b, 40 * b) - N

            # we should buy longs
            if n0 >= 0:
                
                # how mucn would this cost?
                cost = market_maker.price_trade([n0, 0])
                
                # If the cost is greater than our budget, how many units can we buy for our budget?
                if cost > B:
                    n =  brentq(lambda n: market_maker.price_trade([n, 0]) - B, -40 * b, 40 * b)
                    return {'market': market, 'quantity': float(round(n, 2)), 'team': False, 'long': True, 'cost': float(round(B, 2))}

                # else we can safely purchase n0 longs
                else:
                    return {'market': market, 'quantity': float(round(n0, 2)), 'team': False, 'long': True, 'cost': float(round(cost, 2))}
            
            # we should buy shorts
            else:
                
                # how much would it cost to buy -n0 shorts? (-n0 is +ve)
                cost = market_maker.price_trade([0, -n0]) 


                # If the cost is greater than our budget, how many units can we buy for our budget?
                if cost > B:
                    n = brentq(lambda n: market_maker.price_trade([0, n]) - B, -40 * b, 40 * b)
                    return {'market': market, 'quantity': float(round(n, 2)), 'team': False, 'long': False, 'cost': float(round(B, 2))}

                # else we can safely purchase -n0 shorts
                else:
                    return {'market': market, 'quantity': float(round(-n0, 2)), 'team': False, 'long': False, 'cost': float(round(cost, 2))}
            
        except Exception as E:
            logging.error(f'TRADING BOT failed for {market}: {E}')
            return {'market': market, 'quantity': 0, 'team': False, 'long': True, 'cost': 0}
