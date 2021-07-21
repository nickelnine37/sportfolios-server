
import logging
import numpy as np
from itertools import groupby
from concurrent.futures import ThreadPoolExecutor

from lmsr import maker
from scheduler_utils import Timer, RedisExtractor, firebase


class FirebaseMarketJobs:

    def __init__(self, t: int=0):

        self.redis_extractor = RedisExtractor()
        self.t = t


    def get_timeframes(self) -> list:
        """
        For a particular number of minutes, t, return the time horizons that we are interested in
        """

        out = []

        if self.t % 60 == 0:
            out.append('d')
        if (self.t % (60 * 24)) == 0:
            out += ['w', 'm', 'M']

        return out


    def get_long_time_series(self, market: str, q: np.ndarray, current: dict, historical: dict, timeframe: str, team: bool):
        """
        For a particular market, a particular quantity (in this case always the long, which could be different
        lengths), the current market JSON, the historical market JSON and a timeframe, return a time series
        of the quantity value for that particular timeframe
        """

        if team:
            # we only want roughly 30 values in the time series, and definitely the current value
            xs = historical['x'][timeframe][::len(historical['x'][timeframe]) // 30 + 1] + [current['x']]
            bs = historical['b'][timeframe][::len(historical['b'][timeframe]) // 30 + 1] + [current['b']]

            if len(xs) != len(bs):
                logging.error(f'Cannot perform document update for {market}: len(bs) != len(xs)')
                return None

            return maker.LMSRMultiMarketMaker(market, xs, bs).value(q)
        else:
            # we only want roughly 30 values in the time series, and definitely the current value
            Ns = historical['N'][timeframe][::len(historical['N'][timeframe]) // 30 + 1] + [current['N']]
            bs = historical['b'][timeframe][::len(historical['b'][timeframe]) // 30 + 1] + [current['b']]

            if len(Ns) != len(bs):
                logging.error(f'Cannot perform document update for {market}: len(bs) != len(Ns)')
                return None

            return maker.LongShortMultiMarketMaker(market, Ns, bs).spot_value()
        

    def get_document_updates(self, markets: list, timeframes: list, team: bool) -> dict:
        """
        For a given list of markets and timeframes, return a dictionary indexed
        by market that contains the necessary information to update the firebase
        document. This includes the current long price plus the price graph and
        returns for each timeframe given in the timeframes list. 
        """

        assert isinstance(markets, list)
        assert isinstance(timeframes, list)

        if len(timeframes) == 0:
            logging.error('Timeframes is empty!')
            return {}

        all_current, all_hist = self.redis_extractor.get_current_and_historical_holdings(markets)

        # if the first market is a team, they all should be!
        if team:
            n_markets = len(all_current[0]['x'])
            q = np.exp(-np.linspace(0, n_markets - 1, n_markets) / 6)[::-1]
        else:
            q = None

        documents = {}

        for market, current, hist in zip(markets, all_current, all_hist):
            
            if (current is None) or (hist is None):
                logging.error(f'Cannot update market doc {timeframes} for {market}. Redis returned None')
                continue

            documents[market] = {}

            for timeframe in timeframes:

                documents[market][f'long_price_hist.{timeframe}'] = self.get_long_time_series(market, q, current, hist, timeframe, team)
                current_price, oldest_price = documents[market][f'long_price_hist.{timeframe}'][-1], documents[market][f'long_price_hist.{timeframe}'][0]
                documents[market][f'long_price_returns_{timeframe}'] = current_price / oldest_price - 1

            documents[market]['long_price_current'] = current_price

        return documents


    def get_document_batches(self, markets: list, timeframes: list, team: bool):
        """
        For a given list of markets and timeframes, make the necessary updates 
        to the firebase documents. 
        """

        documents = self.get_document_updates(markets, timeframes, team)

        # send documents over in batches
        batches = []

        for i, (market, document) in enumerate(documents.items()):

            if (i % 499) == 0:
                batches.append(firebase.db.batch())
            
            if team:
                batches[-1].update(firebase.teams_collection.document(market), document)
            else:
                batches[-1].update(firebase.players_collection.document(market), document)

        return batches


    def update_all_markets(self):
        """
        Run through all markets and make the necessary updates to firebase
        """

        timeframes = self.get_timeframes()

        all_batches = []

        with Timer() as cpu_timer:

            with Timer() as team_timer:

                with open('/var/www/data/teams.txt', 'r') as f:
                    all_teams = f.read().splitlines()

                # split on league, so all xs have the same length
                for leagueId, teams in groupby(all_teams, key=lambda player: player.split(':')[1]):
                    all_batches += self.get_document_batches(list(teams), timeframes, team=True)

            with Timer() as player_timer:
                
                with open('/var/www/data/players.txt', 'r') as f:
                    all_players = f.read().splitlines()

                # split on league, just so we maintain a reasonable number of markets at a time
                for leagueId, players in groupby(all_players, key=lambda player: player.split(':')[1]):
                    all_batches += self.get_document_batches(list(players), timeframes, team=False)
                    
        with Timer() as firebase_timer:
            
            # execute firebase batch commits on seperate threads
            with ThreadPoolExecutor(max_workers=len(all_batches)) as executor:
                executor.map(lambda batch: batch.commit(), all_batches)

        logging.info(f'FIREBASE MARKETS t = {self.t}. Completed update for timeframes {timeframes}. time: {cpu_timer.t + firebase_timer.t:.4f}s \t team time: {team_timer.t:.4f}s \t player time: {player_timer.t:.4f}s \t cpu time: {cpu_timer.t:.4f}s \t firebase time: {firebase_timer.t:.4f}s')

        self.t += 60
