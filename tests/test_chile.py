import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import ChileFetcher, constants


class TestChileFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = ChileFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def load_sample_data(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r") as f:
            return f.read()

    @patch("requests.Session.get")
    def test_get_data_discharge(self, mock_get):
        link_response_content = self.load_sample_data("chile_link_response.html")
        data_response_content = self.load_sample_data("chile_data_sample.csv")

        mock_link_response = MagicMock()
        mock_link_response.text = link_response_content
        mock_link_response.raise_for_status = MagicMock()

        mock_data_response = MagicMock()
        mock_data_response.text = data_response_content
        mock_data_response.raise_for_status = MagicMock()

        def get_side_effect(*args, **kwargs):
            if "request.php" in args[0]:
                return mock_link_response
            elif "test_file.csv" in args[0]:
                return mock_data_response
            raise Exception("Unexpected URL in test")

        mock_get.side_effect = get_side_effect

        gauge_id = "test_gauge"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2022-01-01"
        end_date = "2022-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"]),
            constants.DISCHARGE_DAILY_MEAN: [15.5, 16.0, 15.8],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(mock_get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
