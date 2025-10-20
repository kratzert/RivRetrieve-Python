import json
import os
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import CzechFetcher, constants


class TestCzechFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = CzechFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def load_sample_json(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge(self, mock_session):
        sample_json = self.load_sample_json("czech_data_sample.json")

        mock_response = MagicMock()
        mock_response.json.return_value = sample_json
        mock_response.raise_for_status = MagicMock()
        mock_session.return_value.get.return_value = mock_response

        gauge_id = "0-203-1-016000"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2020-01-01"
        end_date = "2020-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(
                ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]
            ),
            constants.DISCHARGE_DAILY_MEAN: [1.23, 1.45, 1.67, 1.89, 2.01],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.return_value.get.assert_called_once()
        mock_args, mock_kwargs = mock_session.return_value.get.call_args
        self.assertIn(gauge_id, mock_args[0])
        self.assertIn("2020", mock_args[0])
        self.assertIn("DQ", mock_args[0])


if __name__ == "__main__":
    unittest.main()
