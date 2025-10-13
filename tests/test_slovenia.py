import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import SloveniaFetcher, constants


class TestSloveniaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SloveniaFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        self.sample_csv = self.test_data_dir / "slovenia_sample.csv"

        if not self.sample_csv.exists():
            raise FileNotFoundError(f"Sample data not found at {self.sample_csv}")

    def load_sample_data(self):
        with open(self.sample_csv, "r", encoding="utf-8") as f:
            return f.read()

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self.load_sample_data()
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        gauge_id = "1020"
        variable = constants.DISCHARGE
        start_date = "1980-01-01"
        end_date = "1980-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["1980-01-01", "1980-01-02", "1980-01-03", "1980-01-04", "1980-01-05"])
        expected_values = [115.0, 119.0, 115.0, 108.0, 108.0]  # Values from sample file
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.DISCHARGE: expected_values,
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df.reset_index(drop=True), expected_df)
        mock_session.get.assert_called_once()
        mock_args, mock_kwargs = mock_session.get.call_args
        self.assertIn("p_postaja=1020", mock_args[0])
        self.assertIn("p_od_leto=1980", mock_args[0])
        self.assertIn("p_do_leto=1980", mock_args[0])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_stage(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self.load_sample_data()
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        gauge_id = "1020"
        variable = constants.STAGE
        start_date = "1980-01-01"
        end_date = "1980-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["1980-01-01", "1980-01-02", "1980-01-03"])
        expected_values = [
            90 / 100,
            92 / 100,
            90 / 100,
        ]  # Values from sample file / 100
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.STAGE: expected_values,
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df.reset_index(drop=True), expected_df)


if __name__ == "__main__":
    unittest.main()
