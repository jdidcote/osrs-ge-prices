from typing import List, Optional

import pandas as pd

from grandexchange.preprocess import load_preprocessed_data


class DataLoader:
    def __init__(self, n_hours_resample: int):
        self.df = load_preprocessed_data(n_hours_resample)
        self._load_id_key()

    def _load_id_key(self) -> None:
        self.item_id_key = self.df[["item_id", "name"]].drop_duplicates()

    def aggregate_time_series(
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

        def _item_id_mask(df: pd.DataFrame) -> pd.DataFrame:
            return self.df["item_id"].isin(item_ids)

        def _start_date_mask(df: pd.DataFrame) -> pd.DataFrame:
            return self.df.index >= start_date

        def _end_date_mask(df: pd.DataFrame) -> pd.DataFrame:
            return self.df.index <= end_date

        masks = {
            item_ids: _item_id_mask,
            start_date: _start_date_mask,
            end_date: _end_date_mask
        }

        df = self.df.copy()
        for arg, mask in masks.items():
            if arg is not None:
                df = df[mask(df)]

        return df


if __name__ == '__main__':
    data_loader = DataLoader(7)
    print(data_loader.aggregate_time_series())
