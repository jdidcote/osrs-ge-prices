import os
import sqlite3

import pandas as pd

from grandexchange.wiki_api import (
    get_1h_history,
    get_all_dates,
    get_item_mapping
)


class GrandExchangeDB:
    def __init__(self, update_data: bool = True):
        self.all_dates = get_all_dates()
        self.db_file = '../data/osrs_ge.sqlite'
        self._setup(update_data)
    
    def _setup(self, update_data):
        if not os.path.isfile(self.db_file):
            print("Local DB doesn't exists, creating...")
            self.con = sqlite3.connect(self.db_file)
            self._setup_db()

        self.con = sqlite3.connect(self.db_file)
        if update_data:
            print("Local DB found, checking for updated grandexchange...")
            self.update_price_data()
    
    def _setup_db(self):
        ge_price_list = []
        for date in self.all_dates.iloc[0:2]['timestamp'].values:
            ge_price_list.append(get_1h_history(date))

        ge_price_data = pd.concat(ge_price_list, axis=0)
        ge_price_data.to_sql("prices", self.con)

        item_mapping_data = get_item_mapping()
        item_mapping_data.to_sql('items', self.con)
        print(f"DB set up, {len(self.all_dates)} timesteps saved")
        
    def get_price_data(self):
        query = """
            SELECT *
            FROM PRICES
        """
        prices = pd.read_sql(query, self.con)
        prices['datetime'] = pd.to_datetime(prices['datetime'])
        return prices
    
    def get_item_data(self):
        query = """
            SELECT *
            FROM ITEMS
        """
        return pd.read_sql(query, self.con)

    def get_blacklist_data(self):
        query = """
            SELECT *
            FROM BLACKLIST
        """
        return pd.read_sql(query, self.con)

    def query_db(self, query: str):
        return pd.read_sql(query, self.con)

    def update_price_data(self):
        stored_dates = self.query_db(
            """
            SELECT DISTINCT datetime
            FROM PRICES 
            """
        )
        stored_dates['datetime'] = pd.to_datetime(stored_dates['datetime'])

        # Load all timestamps not found in the db
        missing_timestamps = self.all_dates[(
            ~
            self.all_dates['datetime']
            .isin(
                stored_dates['datetime']
            )
        )].reset_index(drop=True)

        # Remove blacklisted timestamps from missing_timesteps so we don't query the api
        blacklist = self.get_blacklist_data()
        missing_timestamps = missing_timestamps[
            ~missing_timestamps["timestamp"].isin(
                blacklist["timestamp"]
            )
        ]

        # Get response from each row and blacklist if we don't get any data
        histories = []
        for i, row in missing_timestamps.iterrows():
            history = get_1h_history(row["timestamp"])
            if len(history) == 0:
                print(f"Blacklisting {row['datetime']} -- no data found")
                row.to_frame().T.to_sql("blacklist", self.con, if_exists="append")
                continue
            print(f'Progress {i + 1}/{len(missing_timestamps)}')
            histories.append(history)

        if len(histories) == 0:
            print("No new data found")
            return

        ge_price_data = pd.concat(histories, axis=0)
        ge_price_data.to_sql("prices", self.con, if_exists='append')
        print(f"Latest price grandexchange loaded, {len(missing_timestamps)} new timesteps saved")


if __name__ == '__main__':
    db = GrandExchangeDB()
