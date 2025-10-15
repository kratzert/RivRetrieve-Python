import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import USAFetcher, constants


class TestUSAFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = USAFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        self.sample_csv = self.test_data_dir / "usa_07374000_discharge_20230101.csv"

        if not self.sample_csv.exists():
            raise FileNotFoundError(f"Sample data not found at {self.sample_csv}")

    def load_sample_data(self):
        df = pd.read_csv(self.sample_csv, index_col=0)
        df.index = pd.to_datetime(df.index)
        return df

    @patch("dataretrieval.nwis.get_dv")
    def test_get_data_discharge(self, mock_get_dv):
        sample_df = self.load_sample_data()
        mock_get_dv.return_value = (
            sample_df,
            MagicMock(),
        )  # get_dv returns df and metadata

        gauge_id = "07374000"
        variable = constants.DISCHARGE
        start_date = "2023-01-01"
        end_date = "2023-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"])
        cfs_to_m3s = 0.0283168466
        expected_values = [
            373000 * cfs_to_m3s,
            373000 * cfs_to_m3s,
            373000 * cfs_to_m3s,
            377000 * cfs_to_m3s,
            382000 * cfs_to_m3s,
        ]
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.DISCHARGE: expected_values,
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        mock_get_dv.assert_called_once()
        mock_args, mock_kwargs = mock_get_dv.call_args
        self.assertEqual(mock_kwargs["sites"], gauge_id)
        self.assertEqual(mock_kwargs["startDT"], start_date)
        self.assertEqual(mock_kwargs["endDT"], end_date)
        self.assertEqual(mock_kwargs["parameterCd"], ["00060"])


if __name__ == "__main__":
    unittest.main()
