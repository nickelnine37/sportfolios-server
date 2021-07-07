
from itertools import groupby
import logging

from firebase_admin import credentials, firestore
import firebase_admin
from scheduler_utils import Timer, RedisExtractor
import numpy as np
from lmsr import maker
from concurrent.futures import ThreadPoolExecutor


class FirebaseMarketJobs:

    def __init__(self):

        cred = credentials.Certificate('/var/www/sportfolios-431c6-firebase-adminsdk-bq76v-f490ad544c.json')
        self.default_app = firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        self.teams_collection = self.db.collection(u'teams')
        self.players_collection = self.db.collection(u'players')

        self.redis_extractor = RedisExtractor()
        self.t = 0

    
    def get_timeframes(self):
        """
        For a particular number of minutes, t, return the time horizons that we are interested in
        """

        out = []

        if self.t % 60 == 0:
            out.append('d')
        if (self.t % (60 * 24)) == 0:
            out += ['w', 'm', 'M']

        return out


    def get_long_time_series(self, market: str, q: np.ndarray, current: dict, hist: dict, timeframe: str):
        """
        For a particular market, a particular quantity (in this case always the long, which could be different
        lengths), the current market JSON, the historical market JSON and a timeframe, return a time series
        of the quantity value for that particular timeframe
        """

        xs = hist['x'][timeframe][::len(hist['x'][timeframe]) // 30] + [current['x']]
        bs = hist['b'][timeframe][::len(hist['b'][timeframe]) // 30] + [current['b']]

        if len(xs) != len(bs):
            logging.error(f'Cannot perform document update for {market}: len(bs) != len(xs)')
            return None

        return maker._MultiMarketMaker(market, xs, bs).value(q)
        

    def get_document_updates(self, markets: list, timeframes: list) -> dict:
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

        if markets[0][-1] == 'T':
            q = np.exp(-np.linspace(0, len(all_current[0]['x']) - 1, len(all_current[0]['x'])) / 6)[::-1]
        else:
            q = np.exp(-np.linspace(0, 9, 10) / 3)[::-1]

        documents = {}

        for market, current, hist in zip(markets, all_current, all_hist):
            
            if (current is None) or (hist is None):
                logging.error(f'Cannot update market doc {timeframes} for {market}. Redis returned None')
                continue

            documents[market] = {}

            for timeframe in timeframes:

                documents[market][f'long_price_hist.{timeframe}'] = self.get_long_time_series(market, q, current, hist, timeframe)
                current_price, oldest_price = documents[market][f'long_price_hist.{timeframe}'][-1], documents[market][f'long_price_hist.{timeframe}'][0]
                documents[market][f'long_price_returns_{timeframe}'] = (current_price / oldest_price - 1)

            documents[market]['long_price_current'] = current_price

        return documents


    def get_document_batches(self, markets: list, timeframes: list):
        """
        For a given list of markets and timeframes, make the necessary updates 
        to the firebase documents. 
        """

        info = self.get_document_updates(markets, timeframes)

        # send documents over in batches
        batches = [self.db.batch()]

        for i, (market, document) in enumerate(info.items()):

            if (i % 499) == 0:
                batches.append(self.db.batch())
            
            if market[-1] == 'T':
                batches[-1].update(self.teams_collection.document(market), document)
            else:
                batches[-1].update(self.players_collection.document(market), document)

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
                for _, teams in groupby(all_teams, key=lambda player: player.split(':')[1]):
                    all_batches.append(self.get_document_batches(list(teams), timeframes))

            with Timer() as player_timer:
                
                with open('/var/www/data/players.txt', 'r') as f:
                    all_players = f.read().splitlines()

                # split on league, just so we maintain a reasonable number at a time
                for _, players in groupby(all_players, key=lambda player: player.split(':')[1]):
                    all_batches.append(self.get_document_batches(list(players), timeframes))
                    
        with Timer() as firebase_timer:

            with ThreadPoolExecutor(max_workers=len(all_batches)) as executor:
                executor.map(commit, all_batches)

        logging.info(f'FIREBASE MARKETS t = {self.t}. Completed update for timeframes {timeframes}. time: {cpu_timer.t + firebase_timer.t:.4f}s \t team time: {team_timer.t:.4f}s \t player time: {player_timer.t:.4f}s \t cpu time: {cpu_timer.t:.4f}s \t firebase time: {firebase_timer.t:.4f}s')

        self.t += 60

def commit(batch):
    return batch.commit()