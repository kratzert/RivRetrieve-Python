import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import SwitzerlandFetcher, constants


class TestSwitzerlandFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SwitzerlandFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_text(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return f.read()

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("switzerland_metadata_locations.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()

        self.assertEqual(len(result_df), 246)
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertIn("2016", result_df.index)
        self.assertIn("2019", result_df.index)
        self.assertEqual(result_df.loc["2016", constants.STATION_NAME], "Brugg")
        self.assertEqual(result_df.loc["2016", constants.RIVER], "Aare")
        self.assertAlmostEqual(result_df.loc["2016", constants.LATITUDE], 47.4825)
        self.assertAlmostEqual(result_df.loc["2016", constants.LONGITUDE], 8.1949)
        self.assertEqual(result_df.loc["2016", constants.COUNTRY], "Switzerland")
        self.assertEqual(result_df.loc["2016", constants.SOURCE], self.fetcher.SOURCE)
        mock_session.get.assert_called_once()
        args, _ = mock_session.get.call_args
        self.assertIn("/hydro/locations", args[0])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge_uses_archive_and_converts_fallback(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self._load_text("switzerland_2206_discharge_20250101.csv")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="2206",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01"]),
                constants.DISCHARGE_DAILY_MEAN: [14.944444 / 1000.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.post.assert_called_once()
        _, mock_kwargs = mock_session.post.call_args
        self.assertIn("/api/v2/query", mock_session.post.call_args.args[0])
        self.assertIn("Token ", mock_kwargs["headers"]["Authorization"])
        query = mock_kwargs["data"]
        self.assertIn("range(start: 2025-01-01T00:00:00Z, stop: 2025-01-02T00:00:00Z)", query)
        self.assertIn('r["loc"] == "2206"', query)
        self.assertIn('r["_field"] == "flow"', query)
        self.assertIn('r["_field"] == "flow_ls"', query)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_stage_from_archive_payload(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self._load_text("switzerland_2282_stage_20250101.csv")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="2282",
            variable=constants.STAGE_INSTANT,
            start_date="2025-01-01",
            end_date="2025-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 00:10:00"]),
                constants.STAGE_INSTANT: [0.165, 0.165],
            }
        ).set_index(constants.TIME_INDEX)

        self.assertEqual(len(result_df), 141)
        assert_frame_equal(result_df.head(2), expected_df)
        mock_session.post.assert_called_once()
        query = mock_session.post.call_args.kwargs["data"]
        self.assertIn('r["_field"] == "height_abs"', query)
        self.assertIn('r["_field"] == "height"', query)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_temperature(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self._load_text("switzerland_2016_temperature_20200101.csv")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="2016",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2020-01-01",
            end_date="2020-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2020-01-01", "2020-01-02"]),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [6.727014, 6.559091],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.post.assert_called_once()
        query = mock_session.post.call_args.kwargs["data"]
        self.assertIn('r["_field"] == "temperature"', query)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_historical_range_uses_archive_backend(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.text = self._load_text("switzerland_2016_temperature_20200101.csv")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        self.fetcher.get_data(
            gauge_id="2016",
            variable=constants.WATER_TEMPERATURE_INSTANT,
            start_date="2020-01-01",
            end_date="2020-01-02",
        )

        query = mock_session.post.call_args.kwargs["data"]
        self.assertIn("range(start: 2020-01-01T00:00:00Z, stop: 2020-01-03T00:00:00Z)", query)
        self.assertIn('r["loc"] == "2016"', query)
        self.assertIn('r["_field"] == "temperature"', query)


if __name__ == "__main__":
    unittest.main()
