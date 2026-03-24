import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import ThailandFetcher, constants


class TestThailandFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = ThailandFetcher()
        self.test_data_dir = Path(__file__).parent / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _mock_response(payload):
        response = MagicMock()
        response.json.return_value = payload
        response.raise_for_status = MagicMock()
        return response

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata_standardizes_fields(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response(self._load_json("thailand_metadata_sample.json"))

        result_df = self.fetcher.get_metadata()

        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertEqual(list(result_df.index), ["1", "1117894"])
        self.assertEqual(result_df.loc["1", constants.STATION_NAME], "Klong Ladprao Bang Bua Temple")
        self.assertEqual(result_df.loc["1", "station_name_local"], "คลองลาดพร้าว วัดบางบัว")
        self.assertEqual(result_df.loc["1", "station_code"], "BKK021")
        self.assertEqual(result_df.loc["1", "basin"], "Chao Phraya Basin")
        self.assertEqual(result_df.loc["1", "province"], "Bangkok")
        self.assertEqual(result_df.loc["1", "vertical_datum"], "MSL")
        self.assertEqual(result_df.loc["1117894", constants.STATION_NAME], "บ้านขนงพระเหนือ")
        self.assertEqual(result_df.loc["1117894", constants.COUNTRY], "Thailand")
        self.assertEqual(result_df.loc["1117894", constants.SOURCE], self.fetcher.SOURCE)
        mock_session.get.assert_called_once_with(self.fetcher.METADATA_URL, params=None, timeout=60)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage_aggregates_graph_values(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response(self._load_json("thailand_stage_graph_sample.json"))

        result_df = self.fetcher.get_data(
            gauge_id="1",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.STAGE_DAILY_MEAN: [1.3, 1.2],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        mock_session.get.assert_called_once()
        self.assertEqual(mock_session.get.call_args.args[0], self.fetcher.GRAPH_URL)
        self.assertEqual(
            mock_session.get.call_args.kwargs["params"],
            {
                "station_type": "tele_waterlevel",
                "station_id": "1",
                "start_date": "2025-01-01",
                "end_date": "2025-01-02",
            },
        )
        self.assertEqual(mock_session.get.call_args.kwargs["timeout"], 60)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_discharge_drops_null_values(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response(self._load_json("thailand_discharge_graph_sample.json"))

        result_df = self.fetcher.get_data(
            gauge_id="1117894",
            variable=constants.DISCHARGE_INSTANT,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    ["2025-01-01 06:00:00", "2025-01-01 09:00:00", "2025-01-02 06:00:00", "2025-01-02 12:00:00"]
                ),
                constants.DISCHARGE_INSTANT: [2.8, 2.4, 2.6, 2.2],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_long_range_splits_windows_and_combines_payloads(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        payload_2023 = {
            "result": "OK",
            "data": {
                "graph_data": [
                    {"datetime": "2023-01-01 00:00", "value": 1.0, "value_out": None, "discharge": 2.0},
                    {"datetime": "2023-01-02 00:00", "value": 3.0, "value_out": None, "discharge": 4.0},
                ]
            },
        }
        payload_2024 = {
            "result": "OK",
            "data": {
                "graph_data": [
                    {"datetime": "2024-01-01 00:00", "value": 5.0, "value_out": None, "discharge": 6.0},
                    {"datetime": "2024-01-02 00:00", "value": 7.0, "value_out": None, "discharge": 8.0},
                ]
            },
        }
        mock_session.get.side_effect = [self._mock_response(payload_2023), self._mock_response(payload_2024)]

        result_df = self.fetcher.get_data(
            gauge_id="505018",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2023-01-01",
            end_date="2024-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2023-01-01", "2023-01-02", "2024-01-01", "2024-01-02"]),
                constants.DISCHARGE_DAILY_MEAN: [2.0, 4.0, 6.0, 8.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(mock_session.get.call_count, 2)
        first_params = mock_session.get.call_args_list[0].kwargs["params"]
        second_params = mock_session.get.call_args_list[1].kwargs["params"]
        self.assertEqual(first_params["start_date"], "2023-01-01")
        self.assertEqual(first_params["end_date"], "2023-12-31")
        self.assertEqual(second_params["start_date"], "2024-01-01")
        self.assertEqual(second_params["end_date"], "2024-01-02")
        self.assertEqual(mock_session.get.call_args_list[0].args[0], self.fetcher.GRAPH_URL)
        self.assertEqual(mock_session.get.call_args_list[1].args[0], self.fetcher.GRAPH_URL)
        self.assertEqual(mock_session.get.call_args_list[0].kwargs["timeout"], 60)
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["timeout"], 60)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_standardized_empty_frame_for_empty_payload(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response({"result": "OK", "data": {"graph_data": []}})

        result_df = self.fetcher.get_data(
            gauge_id="1",
            variable=constants.STAGE_INSTANT,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(columns=[constants.TIME_INDEX, constants.STAGE_INSTANT]).set_index(
            constants.TIME_INDEX
        )

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)

    def test_unsupported_variable_raises(self):
        with self.assertRaises(ValueError):
            self.fetcher.get_data("1", constants.WATER_TEMPERATURE_DAILY_MEAN)


if __name__ == "__main__":
    unittest.main()
