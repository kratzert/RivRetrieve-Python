import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import BelgiumFlandersFetcher, constants


class TestBelgiumFlandersFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = BelgiumFlandersFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

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
    def test_get_metadata_merges_hic_and_vmm(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_flanders_station_list_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_stage_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_temperature_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_virtual_group_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_station_list_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_stage_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_temperature_ts_map_sample.json")),
        ]

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["L09_16D", "T01_001", "dem04a-1066", "tmp01-1066"])
        self.assertEqual(result_df.loc["dem04a-1066", constants.STATION_NAME], "Zichem")
        self.assertEqual(result_df.loc["dem04a-1066", constants.RIVER], "Demer")
        self.assertAlmostEqual(result_df.loc["dem04a-1066", constants.AREA], 123.45)
        self.assertTrue(pd.isna(result_df.loc["tmp01-1066", constants.AREA]))
        self.assertEqual(result_df.loc["dem04a-1066", "vertical_datum"], "TAW")

        self.assertEqual(result_df.loc["L09_16D", constants.STATION_NAME], "Aalst/Meetpunt")
        self.assertEqual(result_df.loc["L09_16D", constants.RIVER], "Dender")
        self.assertEqual(result_df.loc["L09_16D", "provider"], "vmm")
        self.assertEqual(result_df.loc["L09_16D", constants.SOURCE], "Flemish Environment Agency - VMM")
        self.assertEqual(result_df.loc["tmp01-1066", "provider"], "hic")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_hic_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_flanders_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_discharge_values_sample.json")),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="dem04a-1066",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03"]),
                constants.DISCHARGE_DAILY_MEAN: [21.0, 30.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(mock_session.get.call_args_list[0].kwargs["params"]["timeseriesgroup_id"], "156169")
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["params"]["timeseriesgroup_id"], "192893")
        self.assertEqual(mock_session.get.call_args_list[2].kwargs["params"]["datasource"], 4)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_vmm_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_flanders_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_discharge_values_sample.json")),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="L09_16D",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03"]),
                constants.DISCHARGE_DAILY_MEAN: [12.0, 20.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(mock_session.get.call_args_list[2].kwargs["params"]["datasource"], 1)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_empty_for_unknown_station(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_response(self._load_json("belgium_flanders_discharge_ts_map_sample.json")),
            self._mock_response(self._load_json("belgium_flanders_vmm_discharge_ts_map_sample.json")),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="missing-station",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-03",
        )

        self.assertTrue(result_df.empty)

    def test_unsupported_variable_raises(self):
        with self.assertRaises(ValueError):
            self.fetcher.get_data("dem04a-1066", constants.DISCHARGE_INSTANT)


if __name__ == "__main__":
    unittest.main()
