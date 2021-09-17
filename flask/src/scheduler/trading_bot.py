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


def transform(m):
    random_choice = np.random.uniform(0, 1)
    random_uniform = np.random.uniform(1, 3)
    if random_choice < 0.5:
        def exponential_negative(factor, x):
            return np.exp(factor * ((x[-1] - x) / (x[-1] - x[0]) - 1))
        exponential_dist = exponential_negative(factor=random_uniform,
                                                x=np.arange(start=int(len(m) + 1), stop=1, step=-1))
    else:
        def exponential_positive(factor, x):
            return np.exp(factor * ((x - x[0]) / (x[-1] - x[0]) - 1))
        exponential_dist = exponential_positive(factor=random_uniform,
                                                x=np.arange(start=int(len(m) + 1), stop=1, step=-1))
    return np.asarray((m * exponential_dist) / sum(m * exponential_dist))


class TradingBot:

    def __init__(self, t: int=0, trade_noise: bool=True) -> None:
        """
        Initialise a trading bot. This object will select random markets to make trades in. trade_noise optionally
        adds some random purturbation to the trade probability
        """
        
        self.B_factor = 0.01
        self.trade_noise = trade_noise
        self.noise_level = 0.05
        self.redis_extractor = RedisExtractor()
        self.t = t

    def select_players(self) -> zip:
        """
        Returns a zipped object containing: the slected player market names; the current m; and the current holding
        """

        with open('/var/www/data/player_ms.json') as f:
            player_ms = orjson.loads(f.read())

        n_select = len(player_ms) // 6
        selected_players = np.random.choice(list(player_ms.keys()), size=n_select, replace=False).tolist()
        player_holdings = self.redis_extractor.get_current_holdings(selected_players)

        # add some gaussian noise onto the m-level, but ensure its between 0.005 and 0.995
        if self.trade_noise:
            ms = np.array([player_ms[player] for player in selected_players])
            noise = np.random.normal(loc=0, scale=self.noise_level, size=n_select)
            ms = np.clip(ms + noise, a_min=0.005, a_max=0.995).tolist()
        else:
            ms = [player_ms[player] for player in selected_players]

        return zip(selected_players, ms, player_holdings)

    def select_teams(self) -> zip:
        """
        Returns a zipped object containing: the slected team market names; the current m; and the current holding
        """

        with open('/var/www/data/team_ms.json') as f:
            team_ms = orjson.loads(f.read())

        selected_teams = np.random.choice(list(team_ms.keys()), size=len(team_ms) // 6, replace=False).tolist()
        team_holdings = self.redis_extractor.get_current_holdings(selected_teams)

        if self.trade_noise:
            ms = [transform(np.asarray(team_ms[team])) for team in selected_teams]
            # ms = [np.asarray(team_ms[team]) for team in selected_teams]
        else:
            ms = [team_ms[team] for team in selected_teams]

        return zip(selected_teams, ms, team_holdings)

    def trade_players(self) -> list:
        """
        Select player and make trades. Return dict with trade details
        """

        trades = []
        new_holdings = {}

        for market, current_m, current_holdings in self.select_teams():

            trade = self.optimal_trade_team(market, current_m, current_holdings)

            if trade['cost'] != 0:
                trades.append(trade)
                new_holdings[market] = {'x': (np.asarray(current_holdings['x']) + np.asarray(trade['quantity'])).tolist(), 'b': current_holdings['b']}

        self.redis_extractor.write_current_holdings(new_holdings)

        return trades

    def trade_teams(self) -> list:
        """
        Select teams and make trades. Return dict with trade details
        """

        trades = []
        new_holdings = {}

        for market, current_m, current_holdings in self.select_players():

            trade = self.optimal_trade_player(market, current_m, current_holdings)

            if trade['cost'] != 0:
                trades.append(trade)
                new_holdings[market] = {'N': current_holdings['N'] + trade['quantity'] * (-1) ** (~trade['long']), 'b': current_holdings['b']}

        self.redis_extractor.write_current_holdings(new_holdings)

        return trades

    def trade(self):
        """
        If the time is right, exectute some trades
        """

        if self.t % 10 == 2:

            with Timer() as player_timer:
                team_trades = self.trade_teams()

            with Timer() as team_timer:
                player_trades = self.trade_players()

            with open(f'/var/www/logs/trades/{int(time.time())}.json', 'w') as f:
                json.dump(team_trades + player_trades, f)

            logging.info(f'TRADING BOT: t = {self.t}. Player time: {player_timer.t:.4f}. Team time: {team_timer.t:.4f}')
        
        self.t += 2


    def optimal_trade_team(self, market: str, m: Union[list, np.ndarray], holdings: dict):
        """
        Find the optimal trade for a team, given a probability vector m and the current holdings
        """

        try:
            m = np.asarray(m)
            x = np.asarray(holdings['x'])
            b = holdings['b']
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
                q = np.round(q, 2)
                c = cost(q)

                # only make significant trades
                if c < 10:
                    return {'market': market, 'quantity': 0, 'cost': 0, 'long': None}
                                
                if (q >= 0).all():
                    return {'market': market, 'quantity': q.tolist(), 'team': True, 'cost': float(round(c, 2)), 'long': None}
                
        except Exception as E:
            logging.error(f'TRADING BOT failed for {market}: {E}')
            return {'market': market, 'quantity': 0, 'cost': 0, 'long': None}


    def optimal_trade_player(self, market: str, m: float, holdings: dict):
        """
        Find the optimal trade for a player given their expected finishing fraction m, and the current holdings
        """

        assert 0 <= m <= 1

        N, b = holdings['N'], holdings['b']
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