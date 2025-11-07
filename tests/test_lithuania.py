import json
import os
import unittest
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import LithuaniaFetcher, constants


class TestLithuaniaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = LithuaniaFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")
        self.gauge_id = "aunuvenu-vms"

    def load_sample_json(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r") as f:
            return json.load(f)

    def mocked_download_data(self, gauge_id, variable, start_date, end_date):
        # This mock function will return the sample data regardless of the date range
        # as we only have one sample file.
        sample_data = self.load_sample_json("lithuania_aunuvenu-vms_2024-01.json")
        return sample_data.get("observations", [])

    @patch("rivretrieve.lithuania.LithuaniaFetcher._download_data")
    def test_get_data_discharge(self, mock_download):
        mock_download.side_effect = self.mocked_download_data

        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2024-01-01"
        end_date = "2024-01-03"

        result_df = self.fetcher.get_data(self.gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"], utc=True),
            constants.DISCHARGE_DAILY_MEAN: [1.96, 1.62, 1.32],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_download.assert_called_once_with(self.gauge_id, variable, "2024-01-01", "2024-01-03")

    @patch("rivretrieve.lithuania.LithuaniaFetcher._download_data")
    def test_get_data_stage(self, mock_download):
        mock_download.side_effect = self.mocked_download_data

        variable = constants.STAGE_DAILY_MEAN
        start_date = "2024-01-29"
        end_date = "2024-01-31"

        result_df = self.fetcher.get_data(self.gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(["2024-01-29", "2024-01-30", "2024-01-31"], utc=True),
            constants.STAGE_DAILY_MEAN: [1.85, 1.76, 1.72],  # Divided by 100
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_download.assert_called_once_with(self.gauge_id, variable, "2024-01-29", "2024-01-31")


if __name__ == "__main__":
    unittest.main()
