import json
from typing import Dict

import numpy as np
import pandas as pd
import requests


def make_api_request(
    sub_directory: str,
    parameters: Dict[str, str] = None
):
    """
    Make a grand exchange api query
    """
    osrs_api_endpoint = "https://prices.runescape.wiki/api/v1/osrs"
    header = {'User-Agent': "Time-series forecasting project"}
    url = osrs_api_endpoint + sub_directory
    return requests.get(url, headers=header, params=parameters)


def get_1h_history(timestamp: str):
    """
    Pulls in the 1 hour history for all tradable items
    for a given timestemp
    """
    try:
        parameters = {'timestamp': timestamp}
        response = make_api_request('/1h/', parameters=parameters).json()
        df = pd.DataFrame(response['data']).T
        df.index.name = 'item_id'
        df.reset_index(inplace=True)
        df['datetime'] = pd.to_datetime(timestamp, unit='s')
        df.dropna(inplace=True)
        return df
    except KeyError:
        print(f'Error in running 1h history for timestamp {timestamp}')
        return


def get_all_dates(origin: str = '2022-01-01'):
    """
    Generates a grandexchange frame of all dates to pull grandexchange for
    """
    df = pd.DataFrame({
        'datetime': pd.date_range(
            start=origin,
            end=pd.Timestamp.now().floor('1h') - pd.to_timedelta(1, "d"),
            freq='1h'
        )
    })
    df["timestamp"] = (df['datetime'].astype(np.int64) / 1e9).astype(np.int64)
    return df


def get_item_mapping():
    """
    Returns the item mapping
    """
    content = make_api_request("/mapping/")
    return pd.DataFrame(json.loads(content._content))


if __name__ == '__main__':
    all_dates = get_all_dates().iloc[0:2]
    date_data = []
    for date in all_dates['timestamp'].values:
        date_data.append(get_1h_history(date))
    print(pd.concat(date_data, axis=0))

