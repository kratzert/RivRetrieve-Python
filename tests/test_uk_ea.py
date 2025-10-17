import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import UKEAFetcher, constants


class TestUKEAFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = UKEAFetcher()
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
        mock_measures_response.json.return_value = self.load_sample_json(self.measures_file)
        mock_measures_response.raise_for_status = MagicMock()

        mock_readings_response = MagicMock()
        mock_readings_response.json.return_value = self.load_sample_json(self.readings_file)
        mock_readings_response.raise_for_status = MagicMock()

        def mock_get_side_effect(url, *args, **kwargs):
            if "measures?station" in url:
                return mock_measures_response
            elif "readings" in url:
                return mock_readings_response
            return MagicMock()

        mock_session.get.side_effect = mock_get_side_effect

        gauge_id = "http://environment.data.gov.uk/hydrology/id/stations/3c5cba29-2321-4289-a1fd-c355e135f4cb"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2024-01-01"
        end_date = "2024-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
        expected_values = [72.777, 99.138, 68.020]  # Values from sample file
        expected_data = {
            constants.TIME_INDEX: expected_dates,
            constants.DISCHARGE_DAILY_MEAN: expected_values,
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        self.assertEqual(mock_session.get.call_count, 2)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        sample_metadata = {
            "items": [
                {
                    "@id": "http://environment.data.gov.uk/hydrology/id/stations/12345",
                    "notation": "12345",
                    "stationReference": "STREF001",
                    "label": "Test Station 1",
                    "lat": 51.5,
                    "long": -0.1,
                    "riverName": "Test River",
                    "catchmentArea": 1234,
                    "easting": 500000,
                    "northing": 180000,
                    "other_col": "some_value",
                },
                {
                    "@id": "http://environment.data.gov.uk/hydrology/id/stations/67890",
                    "notation": "67890",
                    "stationReference": "STREF002",
                    "label": "Test Station 2",
                    "lat": 52.1,
                    "long": -0.5,
                    "riverName": "Another River",
                    "catchmentArea": 4567,
                    "easting": 480000,
                    "northing": 200000,
                    "other_col": None,  # Match the None for check_like
                },
            ]
        }

        mock_response = MagicMock()
        mock_response.json.return_value = sample_metadata
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()

        expected_data = {
            constants.GAUGE_ID: [
                "12345",
                "67890",
            ],
            "@id": [
                "http://environment.data.gov.uk/hydrology/id/stations/12345",
                "http://environment.data.gov.uk/hydrology/id/stations/67890",
            ],
            "stationReference": ["STREF001", "STREF002"],
            constants.STATION_NAME: ["Test Station 1", "Test Station 2"],
            constants.LATITUDE: [51.5, 52.1],
            constants.LONGITUDE: [-0.1, -0.5],
            constants.RIVER: ["Test River", "Another River"],
            constants.AREA: [1234, 4567],
            "easting": [500000, 480000],
            "northing": [180000, 200000],
            "other_col": ["some_value", None],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.GAUGE_ID)

        assert_frame_equal(result_df, expected_df, check_like=True)
        mock_session.get.assert_called_once()
        mock_args, mock_kwargs = mock_session.get.call_args
        self.assertIn("/hydrology/id/stations.json", mock_args[0])
        self.assertEqual(mock_kwargs["params"], {"_limit": 10000})


if __name__ == "__main__":
    unittest.main()
