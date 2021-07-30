import redis
import orjson
from src.redis_utils.exceptions import ResourceNotFoundError

redis_db = redis.Redis(host='redis', port=6379, db=0)


def get_latest_quantities(market: str) -> dict:
    """
    Get the latest quantities (x and b or N and b) for a given market. If the market is 
    not found in Redis, raise a ResourceNotFoundError. 
    """

    result = redis_db.get(market)

    if result is None:
        raise ResourceNotFoundError

    return orjson.loads(result)


def get_multiple_latest_quantities(markets: list) -> dict:
    """
    Given a list of markets, return the current quantities as above for each. If a 
    particular market is not found in Redis, its associated entry will be None. No
    error will be raised. 
    """

    with redis_db.pipeline() as pipe:

        for market in markets:

            pipe.get(market)

        results = pipe.execute()

    return {market: orjson.loads(result) if result is not None else None for market, result in zip(markets, results)}


def get_historical_quantities(market: str) -> dict:
    """
    Get the historical quantities associted with a market. This will be a dict
    with two keys, 'data' and 'time'. Data contains the time series quantities
    for x/N and b, with hdwmM for each. If the market is not found, raise a 
    ResourceNotFoundError. 
    """

    with redis_db.pipeline() as pipe:

        pipe.get(market + ':hist')
        pipe.get('time')
        
        data, time = pipe.execute()

    if data is None:
        raise ResourceNotFoundError
    
    time = orjson.loads(time)
    data = orjson.loads(data)

    # ensure lengths are consistent
    for th in ['h', 'd', 'w', 'm', 'M']:
        nt = len(time[th])
        if len(data['b'][th]) != nt:
            data['b'][th] = data['b'][th][:nt]

    return {'data': data, 'time': time}


def get_multiple_historical_quantities(markets: list) -> dict:
    """
    As above, but now 'data' is indexed by market. 
    """

    with redis_db.pipeline() as pipe:

        for market in markets:

            pipe.get(market + ':hist')

        pipe.get('time')
        
        results = pipe.execute()

    time = orjson.loads(results[-1])
    data =  {market: orjson.loads(result) if result is not None else None for market, result in zip(markets, results[:-1])}

    # ensure lengths are consistent
    for th in ['h', 'd', 'w', 'm', 'M']:
        nt = len(time[th])
        for market in markets:
            if len(data[market]['b'][th]) != nt:
                data[market]['b'][th] = data[market]['b'][th][:nt]

    return {'data': data, 'time': time}


# def ensure_historical_data_consistent(hist_data: dict):

#     time_lens = {th: len(array) for th, array in hist_data['time'].items()}

#     if 'b' in hist_data['data']:
#         pass
#     else:


