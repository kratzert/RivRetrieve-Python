import json
import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import FranceFetcher, constants


class TestFranceFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = FranceFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def load_sample_json(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r") as f:
            return json.load(f)

    @patch("requests.Session.get")
    def test_get_data_discharge(self, mock_get):
        sample_json = self.load_sample_json("france_hubeau_sample.json")

        mock_response = MagicMock()
        mock_response.json.return_value = sample_json
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        gauge_id = "Y1220010"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2023-01-01"
        end_date = "2023-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            constants.DISCHARGE_DAILY_MEAN: [15.0005, 16.0000, 15.5002],  # Divided by 1000
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
