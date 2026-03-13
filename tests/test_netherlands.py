import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import NetherlandsFetcher, constants


class TestNetherlandsFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = NetherlandsFetcher()
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
    def test_get_metadata_filters_to_supported_discharge_surface_water_stations(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.post.return_value = self._mock_response(self._load_json("netherlands_catalog_sample.json"))

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["borgharen", "lobith"])
        self.assertEqual(result_df.loc["lobith", constants.STATION_NAME], "Lobith")
        self.assertEqual(result_df.loc["lobith", constants.RIVER], "Rijn")
        self.assertAlmostEqual(result_df.loc["lobith", constants.LATITUDE], 51.8621, places=4)
        self.assertAlmostEqual(result_df.loc["lobith", constants.LONGITUDE], 6.1140, places=4)
        self.assertEqual(result_df.loc["lobith", constants.COUNTRY], "Netherlands")
        self.assertEqual(result_df.loc["lobith", constants.SOURCE], self.fetcher.SOURCE)
        self.assertEqual(mock_session.post.call_count, 1)

    def test_available_variables(self):
        self.assertEqual(
            self.fetcher.get_available_variables(),
            (
                constants.DISCHARGE_DAILY_MEAN,
                constants.DISCHARGE_INSTANT,
                constants.STAGE_DAILY_MEAN,
                constants.STAGE_INSTANT,
                constants.WATER_TEMPERATURE_DAILY_MEAN,
                constants.WATER_TEMPERATURE_INSTANT,
            ),
        )

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.post.side_effect = [
            self._mock_response(self._load_json("netherlands_catalog_sample.json")),
            self._mock_response(self._load_json("netherlands_discharge_sample.json")),
            self._mock_response({}),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="lobith",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.DISCHARGE_DAILY_MEAN: [105.0, 130.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(mock_session.post.call_count, 3)
        data_body = mock_session.post.call_args_list[1].kwargs["json"]
        self.assertEqual(data_body["Locatie"]["Code"], "lobith")
        self.assertEqual(data_body["AquoPlusWaarnemingMetadata"]["AquoMetadata"]["Grootheid"]["Code"], "Q")
        self.assertEqual(data_body["AquoPlusWaarnemingMetadata"]["AquoMetadata"]["Eenheid"]["Code"], "m3/s")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.post.side_effect = [
            self._mock_response(self._load_json("netherlands_catalog_sample.json")),
            self._mock_response(self._load_json("netherlands_discharge_sample.json")),
            self._mock_response({}),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="lobith",
            variable=constants.DISCHARGE_INSTANT,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    [
                        "2025-01-01 01:00:00",
                        "2025-01-01 13:00:00",
                        "2025-01-02 01:00:00",
                        "2025-01-02 13:00:00",
                    ]
                ),
                constants.DISCHARGE_INSTANT: [100.0, 110.0, 120.0, 140.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage_converts_cm_to_m(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.post.side_effect = [
            self._mock_response(self._load_json("netherlands_catalog_sample.json")),
            self._mock_response(self._load_json("netherlands_stage_sample.json")),
            self._mock_response({}),
            self._mock_response({}),
            self._mock_response({}),
            self._mock_response({}),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="lobith",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01"]),
                constants.STAGE_DAILY_MEAN: [1.3],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        first_stage_body = mock_session.post.call_args_list[1].kwargs["json"]
        self.assertEqual(first_stage_body["AquoPlusWaarnemingMetadata"]["AquoMetadata"]["Grootheid"]["Code"], "WATHTE")
        self.assertEqual(first_stage_body["AquoPlusWaarnemingMetadata"]["AquoMetadata"]["Hoedanigheid"]["Code"], "NAP")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_temperature_filters_provider_sentinel_values(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.post.side_effect = [
            self._mock_response(self._load_json("netherlands_catalog_sample.json")),
            self._mock_response(self._load_json("netherlands_temperature_sample.json")),
        ]

        result_df = self.fetcher.get_data(
            gauge_id="borgharen",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01"]),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [12.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    def test_unsupported_variable_raises(self):
        with self.assertRaises(ValueError):
            self.fetcher.get_data("lobith", constants.DISCHARGE_HOURLY_MEAN)


if __name__ == "__main__":
    unittest.main()
