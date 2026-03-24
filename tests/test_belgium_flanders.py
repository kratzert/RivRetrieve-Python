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

    def _build_response_map(self):
        return {
            ("hic", "getStationList", None, None, None): self._load_json("belgium_flanders_station_list_sample.json"),
            ("hic", "getTimeseriesList", "156169", None, None): self._load_json(
                "belgium_flanders_discharge_ts_map_sample.json"
            ),
            ("hic", "getTimeseriesList", "156162", None, None): self._load_json(
                "belgium_flanders_stage_ts_map_sample.json"
            ),
            ("hic", "getTimeseriesList", "156200", None, None): self._load_json(
                "belgium_flanders_temperature_ts_map_sample.json"
            ),
            ("hic", "getTimeseriesList", "260592", None, None): self._load_json(
                "belgium_flanders_virtual_group_sample.json"
            ),
            ("hic", "getTimeseriesValues", None, "5756010", None): self._load_json(
                "belgium_flanders_discharge_values_sample.json"
            ),
            ("hic", "getTimeseriesValues", None, "5091010", None): self._load_json(
                "belgium_flanders_stage_values_sample.json"
            ),
            ("hic", "getTimeseriesValues", None, "45832010", None): self._load_json(
                "belgium_flanders_temperature_values_sample.json"
            ),
            ("vmm", "getStationList", None, None, None): self._load_json(
                "belgium_flanders_vmm_station_list_sample.json"
            ),
            ("vmm", "getTimeseriesList", "192893", None, None): self._load_json(
                "belgium_flanders_vmm_discharge_ts_map_sample.json"
            ),
            ("vmm", "getTimeseriesList", "192782", None, None): self._load_json(
                "belgium_flanders_vmm_stage_ts_map_sample.json"
            ),
            ("vmm", "getTimeseriesList", "325066", None, None): self._load_json(
                "belgium_flanders_vmm_temperature_ts_map_sample.json"
            ),
            ("vmm", "getTimeseriesValues", None, "69697042", None): self._load_json(
                "belgium_flanders_vmm_discharge_values_sample.json"
            ),
            ("vmm", "getTimeseriesValues", None, "282040042", None): self._load_json(
                "belgium_flanders_vmm_stage_values_sample.json"
            ),
            ("vmm", "getTimeseriesValues", None, "39049042", None): self._load_json(
                "belgium_flanders_vmm_temperature_values_sample.json"
            ),
        }

    def _mock_get_side_effect(self, response_map):
        def side_effect(url, *args, **kwargs):
            params = kwargs["params"]
            provider = "vmm" if "download.waterinfo.be" in url else "hic"
            key = (
                provider,
                params["request"],
                params.get("timeseriesgroup_id"),
                params.get("ts_id"),
                params.get("station_no"),
            )
            payload = response_map.get(key)

            if payload is None:
                raise AssertionError(f"Unexpected request params: {provider} {params}")

            return self._mock_response(payload)

        return side_effect

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata_merges_hic_and_vmm(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["HIS_gnt03a-SF-1066", "L05_408", "L09_16D", "L10_078", "dem04a-1066"])
        self.assertEqual(result_df.loc["dem04a-1066", constants.STATION_NAME], "Zichem")
        self.assertEqual(result_df.loc["dem04a-1066", constants.RIVER], "Demer")
        self.assertTrue(pd.isna(result_df.loc["dem04a-1066", constants.AREA]))
        self.assertEqual(result_df.loc["dem04a-1066", "vertical_datum"], "TAW")
        self.assertEqual(result_df.loc["dem04a-1066", "provider"], "hic")
        self.assertEqual(result_df.loc["L10_078", constants.STATION_NAME], "Meerhout")
        self.assertEqual(result_df.loc["L10_078", constants.RIVER], "Grote Nete")
        self.assertEqual(result_df.loc["L10_078", "provider"], "vmm")
        self.assertEqual(result_df.loc["L10_078", constants.SOURCE], "Flemish Environment Agency - VMM")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_hic_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

        result_df = self.fetcher.get_data(
            gauge_id="dem04a-1066",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-04",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04"]),
                constants.DISCHARGE_DAILY_MEAN: [37.84, 45.95, 36.72],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_hic_stage(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

        result_df = self.fetcher.get_data(
            gauge_id="dem04a-1066",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-04",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04"]),
                constants.STAGE_DAILY_MEAN: [17.93, 18.28, 17.90],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_hic_temperature(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

        result_df = self.fetcher.get_data(
            gauge_id="HIS_gnt03a-SF-1066",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2023-01-01",
            end_date="2023-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [10.469565, 10.543750, 9.975000],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_vmm_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

        result_df = self.fetcher.get_data(
            gauge_id="L10_078",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-04",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04"]),
                constants.DISCHARGE_DAILY_MEAN: [1.95, 2.20, 2.38],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_vmm_stage(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

        result_df = self.fetcher.get_data(
            gauge_id="L09_16D",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-04",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04"]),
                constants.STAGE_DAILY_MEAN: [63.468, 63.396, 63.382],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_vmm_temperature(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

        result_df = self.fetcher.get_data(
            gauge_id="L05_408",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2022-01-01",
            end_date="2022-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2022-01-01", "2022-01-02", "2022-01-03"]),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [9.787609, 9.852917, 9.702500],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_empty_for_unknown_station(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = self._mock_get_side_effect(self._build_response_map())

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
