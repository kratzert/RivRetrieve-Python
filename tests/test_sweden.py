import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import SwedenFetcher, constants


class TestSwedenFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SwedenFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        metadata_payloads = {
            "/parameter/1.json": self._load_json("sweden_parameter_1_metadata.json"),
            "/parameter/2.json": self._load_json("sweden_parameter_2_metadata.json"),
            "/parameter/3.json": self._load_json("sweden_parameter_3_metadata.json"),
            "/parameter/4.json": self._load_json("sweden_parameter_4_metadata.json"),
            "/parameter/10.json": self._load_json("sweden_parameter_10_metadata.json"),
        }

        def side_effect(url, *args, **kwargs):
            mock_response = MagicMock()
            for path, payload in metadata_payloads.items():
                if path in url:
                    mock_response.json.return_value = payload
                    mock_response.raise_for_status = MagicMock()
                    return mock_response
            raise AssertionError(f"Unexpected metadata URL: {url}")

        mock_session.get.side_effect = side_effect

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["1906", "2357", "80057", "90039"])
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertEqual(result_df.loc["2357", constants.STATION_NAME], "ABISKO")
        self.assertEqual(result_df.loc["2357", constants.RIVER], "TORNEÄLVEN")
        self.assertAlmostEqual(result_df.loc["2357", constants.LATITUDE], 68.1936)
        self.assertAlmostEqual(result_df.loc["2357", constants.LONGITUDE], 19.9859)
        self.assertEqual(result_df.loc["2357", constants.COUNTRY], "Sweden")
        self.assertEqual(result_df.loc["2357", constants.SOURCE], self.fetcher.SOURCE)
        self.assertEqual(result_df.loc["2357", "parameter_id"], 1)
        self.assertTrue(pd.isna(result_df.loc["80057", constants.AREA]))
        self.assertEqual(result_df.loc["1906", constants.STATION_NAME], "BYRVIKSBANKEN")
        self.assertEqual(result_df.loc["90039", constants.STATION_NAME], "Avasundet VM")

        self.assertEqual(mock_session.get.call_count, 5)
        called_urls = [call.args[0] for call in mock_session.get.call_args_list]
        self.assertEqual(
            called_urls,
            [
                f"{self.fetcher.BASE_URL}/parameter/1.json",
                f"{self.fetcher.BASE_URL}/parameter/2.json",
                f"{self.fetcher.BASE_URL}/parameter/3.json",
                f"{self.fetcher.BASE_URL}/parameter/4.json",
                f"{self.fetcher.BASE_URL}/parameter/10.json",
            ],
        )

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("sweden_90072_discharge_daily_mean_20131107.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="90072",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2013-11-07",
            end_date="2013-11-09",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2013-11-07", "2013-11-08", "2013-11-09"]),
                constants.DISCHARGE_DAILY_MEAN: [0.157, 0.161, 0.170],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        args, kwargs = mock_session.get.call_args
        self.assertIn("/parameter/1/station/90072/period/corrected-archive/data.json", args[0])
        self.assertEqual(kwargs["params"]["from"], "2013-11-07T00:00Z")
        self.assertEqual(kwargs["params"]["to"], "2013-11-09T23:59Z")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("sweden_2357_discharge_instant_20260325.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="2357",
            variable=constants.DISCHARGE_INSTANT,
            start_date="2026-03-25",
            end_date="2026-03-25",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    [
                        "2026-03-25 00:00:00",
                        "2026-03-25 00:15:00",
                        "2026-03-25 00:30:00",
                        "2026-03-25 00:45:00",
                    ]
                ),
                constants.DISCHARGE_INSTANT: [18.0, 18.0, 18.0, 18.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        args, kwargs = mock_session.get.call_args
        self.assertIn("/parameter/2/station/2357/period/corrected-archive/data.json", args[0])
        self.assertEqual(kwargs["params"]["from"], "2026-03-25T00:00Z")
        self.assertEqual(kwargs["params"]["to"], "2026-03-25T23:59Z")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_stage_converts_centimeters_to_meters(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("sweden_1906_stage_instant_20260325.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1906",
            variable=constants.STAGE_INSTANT,
            start_date="2026-03-25",
            end_date="2026-03-26",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2026-03-25 00:00:00", "2026-03-26 00:00:00"]),
                constants.STAGE_INSTANT: [161.33, 161.40],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_water_temperature(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("sweden_80107_water_temperature_instant_19930726.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="80107",
            variable=constants.WATER_TEMPERATURE_INSTANT,
            start_date="1993-07-26",
            end_date="1993-07-31",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    [
                        "1993-07-26 00:00:00",
                        "1993-07-27 00:00:00",
                        "1993-07-28 00:00:00",
                        "1993-07-29 00:00:00",
                        "1993-07-30 00:00:00",
                        "1993-07-31 00:00:00",
                    ]
                ),
                constants.WATER_TEMPERATURE_INSTANT: [20.0, 19.0, 19.0, 18.0, 19.0, 19.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_monthly_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("sweden_90039_discharge_monthly_mean_20101101.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="90039",
            variable=constants.DISCHARGE_MONTHLY_MEAN,
            start_date="2010-11-01",
            end_date="2011-01-31",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    ["2010-11-01 00:00:00", "2010-12-01 00:00:00", "2011-01-01 00:00:00"]
                ),
                constants.DISCHARGE_MONTHLY_MEAN: [2.08, 1.12, 0.838],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_empty_payload_returns_standardized_empty_dataframe(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="90072",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2013-11-07",
            end_date="2013-11-09",
        )

        expected_df = pd.DataFrame(
            columns=[constants.DISCHARGE_DAILY_MEAN],
            index=pd.DatetimeIndex([], name=constants.TIME_INDEX),
        )

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        mock_session.get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
