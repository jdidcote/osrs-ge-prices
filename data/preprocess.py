from data.database import GrandExchangeDB

import pandas as pd
import numpy as np
from scipy.stats import zscore


def load_price_data() -> pd.DataFrame:
    """
    Loads the main dataset
    @return: pd.DataFrame
    """
    ge = GrandExchangeDB()

    prices = ge.query_db(
        """
        WITH ITEMS_TRADED AS (
        SELECT item_id
                ,AVG(avgHighPrice * highPriceVolume) as amnt_traded_high
        FROM PRICES
        WHERE avgHighPrice IS NOT NULL
        AND highPriceVolume IS NOT NULL
        GROUP BY item_id
        )
        SELECT PRICES.*
            ,ITEMS.name
        FROM PRICES 
        INNER JOIN (
            SELECT DISTINCT item_id
            FROM ITEMS_TRADED
            WHERE amnt_traded_high > 1e6
        ) as SELECTED_ITEMS
        ON PRICES.item_id = SELECTED_ITEMS.item_id
        LEFT JOIN ITEMS
        ON ITEMS.id = PRICES.item_id
        """
    ).drop('index', axis=1)

    # Keep only items which have records for 95% of the selected time period
    n_periods = prices['datetime'].nunique()
    df = prices[['item_id']].value_counts().reset_index(name='count')
    df = df.query(f'count >= {round(n_periods * 0.95)}')

    prices = prices[prices['item_id'].isin(df['item_id'])].copy()
    prices['item_id'] = prices['item_id'].astype(int)
    prices['datetime'] = pd.to_datetime(prices['datetime'])

    # Add single values for price and volume
    prices['price'] = (prices['avgLowPrice'] + prices['avgHighPrice']) / 2
    prices['margin'] = prices['avgHighPrice'] - prices['avgLowPrice']
    prices['volume'] = prices['highPriceVolume'] + prices['lowPriceVolume']
    prices.drop(['avgHighPrice', 'avgLowPrice', 'highPriceVolume', 'lowPriceVolume'], axis=1, inplace=True)

    return prices


def remove_price_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes time-series outliers using zscores
    @param df: prices dataframe
    @return: prices dataframe with outliers removed
    """
    df = df.sort_values(['item_id', 'datetime'])

    df['price_change'] = df.groupby('item_id')['price'].diff()
    df.loc[df['price_change'].isna(), 'price_change'] = 0

    df['zscore'] = df.groupby('item_id')['price_change'].transform(zscore)

    df['anomalous'] = np.where(df['zscore'] > 5, 1, 0)
    return df.drop(['price_change', 'zscore', 'anomalous'], axis=1)


def load_preprocessed_data() -> pd.DataFrame:
    """
    Function to combine all preproccessing steps and return a ready to go data set
    @return: pd.DataFrame or cleaned/preprocessed data ready for modelling
    """
    prices = load_price_data()
    prices = remove_price_outliers(prices)
    return prices


if __name__ == '__main__':
    load_preprocessed_data()
