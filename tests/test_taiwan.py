import gzip
import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import TaiwanFetcher, constants


class TestTaiwanFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = TaiwanFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        path = self.test_data_dir / filename
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as f:
                return json.load(f)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("taiwan_flow_station_metadata_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()

        self.assertEqual(len(result_df), 10)
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertIn("1140H099", result_df.index)
        self.assertEqual(result_df.loc["1140H099", constants.STATION_NAME], "思源橋")
        self.assertEqual(result_df.loc["1140H099", constants.RIVER], "北勢溪")
        self.assertAlmostEqual(result_df.loc["1140H099", constants.LATITUDE], 24.961682420969566)
        self.assertAlmostEqual(result_df.loc["1140H099", constants.LONGITUDE], 121.76920614487864)
        self.assertAlmostEqual(result_df.loc["1140H099", constants.ALTITUDE], 259.33)
        self.assertAlmostEqual(result_df.loc["1140H099", constants.AREA], 0.74)
        self.assertEqual(result_df.loc["1140H099", constants.COUNTRY], "Taiwan")
        self.assertEqual(result_df.loc["1140H099", constants.SOURCE], self.fetcher.SOURCE)
        mock_session.get.assert_called_once()
        self.assertIn("/api/v2/9332bd66-0213-4380-a5d5-a43e7be49255", mock_session.get.call_args.args[0])
        self.assertEqual(mock_session.get.call_args.kwargs["params"]["size"], 1000)
        self.assertFalse(mock_session.get.call_args.kwargs["verify"])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("taiwan_1140H099_discharge_daily_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1140H099",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2005-01-07",
            end_date="2005-01-09",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2005-01-07", "2005-01-08", "2005-01-09"]),
                constants.DISCHARGE_DAILY_MEAN: [3.98, 4.46, 4.72],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.post.assert_called_once()
        self.assertIn("/GetStDIDayList", mock_session.post.call_args.args[0])
        self.assertEqual(mock_session.post.call_args.kwargs["data"]["ST_NO"], "1140H099")
        self.assertFalse(mock_session.post.call_args.kwargs["verify"])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_hourly_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("taiwan_1140H099_discharge_hourly_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1140H099",
            variable=constants.DISCHARGE_HOURLY_MEAN,
            start_date="2008-01-01",
            end_date="2008-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    ["2008-01-01 01:00:00", "2008-01-01 02:00:00", "2008-01-01 03:00:00"]
                ),
                constants.DISCHARGE_HOURLY_MEAN: [0.54, 0.67, 0.55],
            }
        ).set_index(constants.TIME_INDEX)

        self.assertGreater(len(result_df), 3)
        assert_frame_equal(result_df.head(3), expected_df)
        mock_session.post.assert_called_once()
        self.assertIn("/GetStDIHourList", mock_session.post.call_args.args[0])
        self.assertEqual(mock_session.post.call_args.kwargs["data"]["ST_NO"], "1140H099")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage_converts_from_zero_point_elevation(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("taiwan_1140H099_stage_daily_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1140H099",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2005-01-06",
            end_date="2005-01-08",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2005-01-06", "2005-01-07", "2005-01-08"]),
                constants.STAGE_DAILY_MEAN: [1.37, 2.13, 2.25],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.post.assert_called_once()
        self.assertIn("/GetStLeDayList", mock_session.post.call_args.args[0])
        self.assertEqual(mock_session.post.call_args.kwargs["data"]["ST_NO"], "1140H099")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_hourly_stage_converts_from_zero_point_elevation(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("taiwan_1140H099_stage_hourly_sample.json.gz")
        mock_response.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1140H099",
            variable=constants.STAGE_HOURLY_MEAN,
            start_date="2006-01-01",
            end_date="2006-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    ["2006-01-01 01:00:00", "2006-01-01 02:00:00", "2006-01-01 03:00:00"]
                ),
                constants.STAGE_HOURLY_MEAN: [3.18, 3.15, 3.15],
            }
        ).set_index(constants.TIME_INDEX)

        self.assertGreater(len(result_df), 3)
        assert_frame_equal(result_df.head(3), expected_df)
        mock_session.post.assert_called_once()
        self.assertIn("/GetStLeHourList", mock_session.post.call_args.args[0])
        self.assertEqual(mock_session.post.call_args.kwargs["data"]["ST_NO"], "1140H099")


if __name__ == "__main__":
    unittest.main()
