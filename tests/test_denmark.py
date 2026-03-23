import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import DenmarkFetcher, constants


class TestDenmarkFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = DenmarkFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("denmark_stations_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["30000681", "30000682"])
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertEqual(result_df.loc["30000681", constants.STATION_NAME], "Gudenaa, Langaa")
        self.assertEqual(result_df.loc["30000681", constants.RIVER], "Gudenaa")
        self.assertAlmostEqual(result_df.loc["30000681", constants.AREA], 1234.5)
        self.assertEqual(result_df.loc["30000681", constants.COUNTRY], "Denmark")
        self.assertEqual(result_df.loc["30000681", constants.SOURCE], "VandA/H Miljoportal")
        self.assertFalse(pd.isna(result_df.loc["30000681", constants.LATITUDE]))
        self.assertFalse(pd.isna(result_df.loc["30000681", constants.LONGITUDE]))
        mock_session.get.assert_called_once()
        args, _ = mock_session.get.call_args
        self.assertIn("/api/stations", args[0])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("denmark_discharge_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="30000681",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2023-01-01", "2023-01-02"]),
                constants.DISCHARGE_DAILY_MEAN: [1.5, 1.5],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        _, mock_kwargs = mock_session.get.call_args
        self.assertIn("/api/water-flows", mock_session.get.call_args.args[0])
        self.assertEqual(mock_kwargs["params"]["stationId"], "30000681")
        self.assertEqual(mock_kwargs["params"]["from"], "2023-01-01T00:00Z")
        self.assertEqual(mock_kwargs["params"]["to"], "2023-01-02T23:59Z")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_stage(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("denmark_stage_instant_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="30000681",
            variable=constants.STAGE_INSTANT,
            start_date="2023-01-01",
            end_date="2023-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2023-01-01T00:00:00", "2023-01-01T00:10:00"]),
                constants.STAGE_INSTANT: [2.50, 2.55],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        _, mock_kwargs = mock_session.get.call_args
        self.assertIn("/api/water-levels", mock_session.get.call_args.args[0])
        self.assertEqual(mock_kwargs["params"]["stationId"], "30000681")
        self.assertEqual(mock_kwargs["params"]["from"], "2023-01-01T00:00Z")
        self.assertEqual(mock_kwargs["params"]["to"], "2023-01-01T23:59Z")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_empty_response_returns_standardized_empty_dataframe(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="30000681",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        expected_df = pd.DataFrame(
            columns=[constants.DISCHARGE_DAILY_MEAN],
            index=pd.DatetimeIndex([], name=constants.TIME_INDEX),
        )

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        mock_session.get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
