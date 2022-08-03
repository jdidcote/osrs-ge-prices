from typing import List, Optional

import pandas as pd

from grandexchange.preprocess import load_preprocessed_data


class DataLoader:
    def __init__(self, n_hours_resample: int):
        self.prices = load_preprocessed_data(n_hours_resample)
        self._load_id_key()

    def _load_id_key(self) -> None:
        self.item_id_key = self.prices[["item_id", "name"]].drop_duplicates()

    def filter_time_series(
            self,
            item_ids: Optional[List[int]] = None,
            start_date: Optional[pd.DatetimeIndex] = None,
            end_date: Optional[pd.DatetimeIndex] = None
    ) -> pd.DataFrame:
        """
        Filters main data by a number of parameters for use in dashboard plotting
        """

        if not (start_date is None and end_date is None) and (start_date > end_date):
            raise ValueError("Start date must be before end date")

        masks = {
            "item_ids": lambda df: self.prices["item_id"].isin(item_ids),
            "start_date": lambda df: self.prices.index >= start_date,
            "end_date": lambda df: self.prices.index <= end_date
        }

        df = self.prices.copy()
        for arg, mask in masks.items():
            if eval(arg) is not None:
                df = df[mask(df)]

        return df
