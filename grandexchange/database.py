import os
import sqlite3

import pandas as pd

from wiki_api import (
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
                
        missing_timestamps = self.all_dates[(
            ~
            self.all_dates['datetime']
            .isin(
                stored_dates['datetime']
            )
        )].timestamp.values
        
        if len(missing_timestamps) > 0:
            ge_price_list = []
            for i, date in enumerate(missing_timestamps):
                print(f'Progress {i}/{len(missing_timestamps)}')
                ge_price_list.append(get_1h_history(date))

            ge_price_data = pd.concat(ge_price_list, axis=0)
            ge_price_data.to_sql("prices", self.con, if_exists='append')
            print(f"Latest price grandexchange loaded, {len(missing_timestamps)} new timesteps saved")
            

if __name__ == '__main__':
    db = GrandExchangeDB()
