import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import BelgiumWalloniaFetcher, constants


class TestBelgiumWalloniaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = BelgiumWalloniaFetcher()
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
    def test_get_metadata_filters_supported_stations(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_wallonia_station_list_sample.json")),
            self._mock_response(self._load_json("belgium_wallonia_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_wallonia_stage_ts_map_sample.json")),
        ]

        result_df = self.fetcher.get_metadata()

        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertEqual(set(result_df.index), {"L5442", "8133"})
        self.assertEqual(result_df.loc["L5442", constants.STATION_NAME], "Aiseau")
        self.assertEqual(result_df.loc["L5442", constants.RIVER], "Biesme")
        self.assertAlmostEqual(result_df.loc["L5442", constants.AREA], 77.5)
        self.assertEqual(result_df.loc["L5442", "vertical_datum"], "DNG")
        self.assertEqual(result_df.loc["8133", constants.COUNTRY], "Belgium")
        self.assertEqual(result_df.loc["8133", constants.SOURCE], self.fetcher.SOURCE)
        self.assertEqual(mock_session.get.call_count, 3)
        self.assertEqual(mock_session.get.call_args_list[0].args[0], self.fetcher.BASE_URL)
        self.assertEqual(mock_session.get.call_args_list[0].kwargs["params"]["request"], "getStationList")
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["params"]["timeseriesgroup_id"], "7256919")
        self.assertEqual(mock_session.get.call_args_list[2].kwargs["params"]["timeseriesgroup_id"], "7255151")
        self.assertTrue(all(call.kwargs["timeout"] == 60 for call in mock_session.get.call_args_list))

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_wallonia_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_wallonia_discharge_values_sample.json")),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="L5442",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-05",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]),
                constants.DISCHARGE_DAILY_MEAN: [2.861, 1.848, 1.305, 4.339],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        self.assertEqual(mock_session.get.call_count, 2)
        ts_map_request = mock_session.get.call_args_list[0].kwargs["params"]
        self.assertEqual(ts_map_request["timeseriesgroup_id"], "7256919")
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["params"]["request"], "getTimeseriesValues")
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["timeout"], 60)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_wallonia_stage_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_wallonia_stage_values_sample.json")),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="L5442",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-05",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]),
                constants.STAGE_DAILY_MEAN: [0.714, 0.598, 0.531, 0.864],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        self.assertEqual(mock_session.get.call_args_list[0].kwargs["params"]["timeseriesgroup_id"], "7255151")
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["params"]["request"], "getTimeseriesValues")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_empty_for_unknown_station(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_wallonia_discharge_ts_map_sample.json")),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="missing-station",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-05",
        )

        expected_df = pd.DataFrame(columns=[constants.TIME_INDEX, constants.DISCHARGE_DAILY_MEAN]).set_index(
            constants.TIME_INDEX
        )

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)

    def test_unsupported_variable_raises(self):
        with self.assertRaises(ValueError):
            self.fetcher.get_data("L5442", constants.WATER_TEMPERATURE_DAILY_MEAN)


if __name__ == "__main__":
    unittest.main()
