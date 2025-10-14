import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal
from parameterized import parameterized

from rivretrieve import UKNRFAFetcher, constants


class TestUKNRFAFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = UKNRFAFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"
        self.station_info_file = self.test_data_dir / "uk_nrfa_station_info_sample.json"
        self.discharge_readings_file = self.test_data_dir / "uk_nrfa_1001_discharge_20220101.json"
        self.precip_readings_file = self.test_data_dir / "uk_nrfa_1001_precipitation_20220101.json"

        if not self.station_info_file.exists():
            raise FileNotFoundError(f"Station info file not found at {self.station_info_file}")
        if not self.discharge_readings_file.exists():
            raise FileNotFoundError(f"Discharge readings file not found at {self.discharge_readings_file}")
        if not self.precip_readings_file.exists():
            raise FileNotFoundError(f"Precipitation readings file not found at {self.precip_readings_file}")

    def load_sample_json(self, filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self.load_sample_json(self.station_info_file)
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()
        self.assertFalse(result_df.empty)
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertIn("1001", result_df.index)
        self.assertIn("2001", result_df.index)

        # Check for renamed columns
        self.assertIn(constants.STATION_NAME, result_df.columns)
        self.assertIn(constants.AREA, result_df.columns)
        self.assertIn(constants.LATITUDE, result_df.columns)
        self.assertIn(constants.LONGITUDE, result_df.columns)
        self.assertIn(constants.RIVER, result_df.columns)
        self.assertIn(constants.ALTITUDE, result_df.columns)

        # Check for some original columns that were not renamed
        self.assertIn("grid-reference", result_df.columns)
        self.assertIn("station-level", result_df.columns)
        self.assertIn("opened", result_df.columns)

        station_1001 = result_df.loc["1001"]
        self.assertEqual(station_1001[constants.STATION_NAME], "Wick at Tarroul")
        self.assertEqual(station_1001[constants.AREA], 161.9)
        self.assertEqual(station_1001[constants.RIVER], "Wick")
        self.assertEqual(station_1001[constants.ALTITUDE], 78.8)

    @parameterized.expand(
        [
            (
                constants.DISCHARGE,
                "gdf",
                "uk_nrfa_1001_discharge_20220101.json",
                [1.552, 1.461, 2.035, 7.232, 6.539],
            ),
            (
                constants.CATCHMENT_PRECIPITATION,
                "cdr",
                "uk_nrfa_1001_precipitation_20220101.json",
                [0.2, 1.7, 4.4, 9.1, 0.2],
            ),
        ]
    )
    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data(self, variable, expected_data_type, sample_file, expected_values, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_readings_response = MagicMock()
        mock_readings_response.json.return_value = self.load_sample_json(self.test_data_dir / sample_file)
        mock_readings_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_readings_response

        gauge_id = "1001"
        start_date = "2022-01-01"
        end_date = "2022-01-05"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"])
        expected_data = {constants.TIME_INDEX: expected_dates, variable: expected_values}
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df.reset_index(drop=True), expected_df, check_dtype=False)
        mock_session.get.assert_called_once()
        _, mock_kwargs = mock_session.get.call_args
        params = mock_kwargs["params"]
        self.assertEqual(params["station"], gauge_id)
        self.assertEqual(params["data-type"], expected_data_type)
        self.assertEqual(params["start-date"], "2022-01-01T00:00:00Z")
        self.assertEqual(params["end-date"], "2022-01-05T23:59:59Z")


if __name__ == "__main__":
    unittest.main()
