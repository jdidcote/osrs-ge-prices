import json
from typing import Dict

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
        content = json.loads(make_api_request('/1h/', parameters=parameters)._content)
        df = pd.DataFrame(content['data']).T
        df.index.name = 'item_id'
        df.reset_index(inplace=True)
        df['datetime'] = pd.to_datetime(timestamp, unit='s')
        df.dropna(inplace=True)
        return df
    except KeyError:
        print(f'Error in running 1h history for timestamp {timestamp}')
        return content


def get_all_dates(origin: str = '2022-01-01'):
    """
    Generates a data frame of all dates to pull data for
    """
    df = pd.DataFrame({
        'datetime': pd.date_range(
            start=origin,
            end=pd.Timestamp.now().floor('1h'),
            freq='1h'
        )
    })
    df["timestamp"] = (df['datetime'].astype(int) / 1e9).astype(int)
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

