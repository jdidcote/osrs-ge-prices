import os
import sqlite3

import pandas as pd

from grand_exchange_api import (
    get_1h_history,
    get_all_dates,
    get_item_mapping
)


def setup_db():
    db_file = 'osrs_ge.sqlite'
    if os.path.isfile():
        return

    con = sqlite3.connect(db_file)

    # Get GE price data
    all_dates = get_all_dates().iloc[0:2]
    date_data = []
    for date in all_dates['timestamp'].values:
        date_data.append(get_1h_history(date))

    pd.concat(date_data, axis=0).to_sql("prices", con)

    # Get item mapping data
    item_mapping = get_item_mapping()

    return


def update_price_data():
    # Get all available dates
    # Check which available dates are not in the DB
    # Append missing dates to DB


if __name__ == '__main__':
    pass