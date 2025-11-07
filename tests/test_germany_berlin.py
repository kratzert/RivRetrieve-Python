import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import GermanyBerlinFetcher, constants


class TestGermanyBerlinFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = GermanyBerlinFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def load_sample_data(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r", encoding="utf-8") as f:
            return f.read()

    @patch("requests.get")
    def test_get_data_discharge(self, mock_get):
        sample_csv = self.load_sample_data("germany_berlin_discharge_sample.csv")

        mock_response = MagicMock()
        mock_response.text = sample_csv
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        gauge_id = "5867601"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2024-01-01"
        end_date = "2024-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            constants.DISCHARGE_DAILY_MEAN: [1.75, 1.75, 2.08],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
