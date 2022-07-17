from data import GrandExchangeDB

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
            WHERE amnt_traded_high > 10e6
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

    df['anomalous'] = np.where(df['zscore'] > 3, 1, 0)
    df = df[df['anomalous'] == 0]
    return df.drop(['price_change', 'zscore', 'anomalous'], axis=1)


def fill_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add in new rows for missing timestamps by item_id and impute values with mean
    @param df: prices dataframe
    @return:
    """
    df = df.copy()

    # Get all periods in the dataset
    all_periods = df[['datetime']].drop_duplicates().reset_index(drop=True)
    all_items = df[['item_id', 'name']].drop_duplicates().reset_index(drop=True)
    df_all = pd.merge(
        all_periods.assign(key=1),
        all_items.assign(key=1),
        on='key'
    ).drop('key', axis=1)

    # Merge all periods into grandexchange
    df = pd.merge(
        df,
        df_all,
        on=['datetime', 'item_id', 'name'],
        how='right'
    ).sort_values(['item_id', 'datetime'])

    # Impute missing grandexchange using mean
    for col in ['price', 'margin', 'volume']:
        df['avg'] = df.groupby('item_id')[col].transform(lambda x: x.mean())
        df.loc[df[col].isna(), col] = df['avg']
        df.drop('avg', axis=1, inplace=True)

    return df


def resample_timestamp(df: pd.DataFrame, n_hours: int) -> pd.DataFrame:
    """
    Resample prices to n_hours and aggregate numeric columns
    @param df: prices dataframe
    @param n_hours: number of hours to aggregate up to
    @return: aggregated prices dataframe
    """
    if (n_hours <= 1) or not (isinstance(n_hours, int)):
        raise ValueError("n_hours must be greater than 1 and an integer")

    df = df.copy()
    df.set_index(['datetime'], inplace=True)

    df = df.groupby(['item_id', 'name']).resample(f'{n_hours}H').agg({
        'price': 'mean',
        'margin': 'mean',
        'volume': 'sum'
    }).reset_index()

    return df


def load_preprocessed_data(n_hours: int) -> pd.DataFrame:
    """
    Function to combine all preproccessing steps and return a ready to go grandexchange set
    @param n_hours: number of hours to aggregate up to
    @return: pd.DataFrame or cleaned/preprocessed grandexchange ready for modelling
    """
    prices = load_price_data()
    prices = remove_price_outliers(prices)
    prices = fill_missing_data(prices)
    prices = resample_timestamp(prices, n_hours)
    return prices


if __name__ == '__main__':
    load_preprocessed_data(6)
