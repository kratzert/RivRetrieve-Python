import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import json
from pathlib import Path

from rivretrieve import UKFetcher
from rivretrieve import constants


class TestUKFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = UKFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        self.measures_file = self.test_data_dir / "uk_measures.json"
        self.readings_file = self.test_data_dir / "uk_readings_discharge.json"

        if not self.measures_file.exists():
            raise FileNotFoundError(f"Measures file not found at {self.measures_file}")
        if not self.readings_file.exists():
            raise FileNotFoundError(f"Readings file not found at {self.readings_file}")

    def load_sample_json(self, filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_measures_response = MagicMock()
        mock_measures_response.json.return_value = self.load_sample_json(
            self.measures_file
        )
        mock_measures_response.raise_for_status = MagicMock()

        mock_readings_response = MagicMock()
        mock_readings_response.json.return_value = self.load_sample_json(
            self.readings_file
        )
        mock_readings_response.raise_for_status = MagicMock()

        def mock_get_side_effect(url, *args, **kwargs):
            if "measures?station" in url:
                return mock_measures_response
            elif "readings" in url:
                return mock_readings_response
            return MagicMock()

        mock_session.get.side_effect = mock_get_side_effect

        gauge_id = "http://environment.data.gov.uk/hydrology/id/stations/3c5cba29-2321-4289-a1fd-c355e135f4cb"
        variable = constants.DISCHARGE
        start_date = "2024-01-01"
        end_date = "2024-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        expected_values = [72.777, 99.138, 68.020]  # Values from sample file
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.DISCHARGE: expected_values,
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(
            result_df.reset_index(drop=True), expected_df, check_dtype=False
        )
        self.assertEqual(mock_session.get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
