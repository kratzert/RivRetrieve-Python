import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import EstoniaFetcher, constants


class TestEstoniaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = EstoniaFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as file_handle:
            return json.load(file_handle)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata_uses_estmodel_json_and_geojson(self, mock_requests_retry_session):
        mock_session = MagicMock()
        mock_requests_retry_session.return_value = mock_session

        stations_response = MagicMock()
        stations_response.json.return_value = self._load_json("estonia_metadata_stations.json")
        stations_response.raise_for_status = MagicMock()

        geojson_response = MagicMock()
        geojson_response.json.return_value = self._load_json("estonia_metadata_stations.geojson")
        geojson_response.raise_for_status = MagicMock()

        mock_session.get.side_effect = [stations_response, geojson_response]

        result_df = self.fetcher.get_metadata()

        self.assertEqual(len(result_df), 59)
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertIn("SJA8821000", result_df.index)
        self.assertEqual(result_df.loc["SJA8821000", constants.STATION_NAME], "Ahja jõgi: Ahja")
        self.assertEqual(result_df.loc["SJA8821000", constants.RIVER], "Ahja jõgi")
        self.assertEqual(result_df.loc["SJA8821000", "location"], "Ahja")
        self.assertAlmostEqual(result_df.loc["SJA8821000", constants.AREA], 896.51)
        self.assertAlmostEqual(result_df.loc["SJA8821000", constants.LATITUDE], 58.2094013)
        self.assertAlmostEqual(result_df.loc["SJA8821000", constants.LONGITUDE], 27.1125992)
        self.assertEqual(result_df.loc["SJA8821000", constants.COUNTRY], self.fetcher.COUNTRY)
        self.assertEqual(result_df.loc["SJA8821000", constants.SOURCE], self.fetcher.SOURCE)
        self.assertEqual(mock_session.get.call_count, 2)
        self.assertEqual(mock_session.get.call_args_list[0].args[0], self.fetcher.STATIONS_URL)
        self.assertEqual(mock_session.get.call_args_list[1].args[0], self.fetcher.GEOJSON_URL)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge_daily_mean(self, mock_requests_retry_session):
        mock_session = MagicMock()
        mock_requests_retry_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("estonia_SJA8821000_discharge_daily_mean_2024.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="SJA8821000",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2024-01-01",
            end_date="2024-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                constants.DISCHARGE_DAILY_MEAN: [6.89, 6.77, 6.65],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        self.assertEqual(
            mock_session.get.call_args.args[0],
            self.fetcher.MEASUREMENTS_URL.format(gauge_id="SJA8821000"),
        )
        self.assertEqual(
            mock_session.get.call_args.kwargs["params"],
            {"parameter": "Q", "type": "MEAN", "start-year": 2024, "end-year": 2024},
        )

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_stage_daily_max(self, mock_requests_retry_session):
        mock_session = MagicMock()
        mock_requests_retry_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("estonia_SJA8821000_stage_daily_max_2024.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="SJA8821000",
            variable=constants.STAGE_DAILY_MAX,
            start_date="2024-01-01",
            end_date="2024-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                constants.STAGE_DAILY_MAX: [1.45, 1.45, 1.43],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        self.assertEqual(
            mock_session.get.call_args.kwargs["params"],
            {"parameter": "H", "type": "MAXIMUM", "start-year": 2024, "end-year": 2024},
        )

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_water_temperature_daily_mean(self, mock_requests_retry_session):
        mock_session = MagicMock()
        mock_requests_retry_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("estonia_SJA8821000_water_temperature_daily_mean_2024.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="SJA8821000",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2024-01-01",
            end_date="2024-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [0.3, 0.3, 0.3],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        self.assertEqual(
            mock_session.get.call_args.kwargs["params"],
            {"parameter": "T", "type": "MEAN", "start-year": 2024, "end-year": 2024},
        )


if __name__ == "__main__":
    unittest.main()
