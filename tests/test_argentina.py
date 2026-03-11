import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import ArgentinaFetcher, constants


class TestArgentinaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = ArgentinaFetcher()

    def test_get_cached_metadata_standardizes_columns(self):
        metadata = self.fetcher.get_cached_metadata()

        self.assertEqual(metadata.index.name, constants.GAUGE_ID)
        for column in [
            constants.STATION_NAME,
            constants.RIVER,
            constants.LATITUDE,
            constants.LONGITUDE,
            constants.COUNTRY,
            constants.SOURCE,
            constants.ALTITUDE,
            constants.AREA,
        ]:
            self.assertIn(column, metadata.columns)

        self.assertTrue((metadata[constants.COUNTRY] == "Argentina").all())
        self.assertTrue((metadata[constants.SOURCE] == "INA Alerta").all())

    @patch("rivretrieve.argentina.ArgentinaFetcher._fetch_station_details")
    @patch("rivretrieve.argentina.ArgentinaFetcher._fetch_series_index")
    def test_get_metadata_uses_series_index_and_station_details(self, mock_series_index, mock_station_details):
        mock_series_index.return_value = pd.DataFrame(
            [
                {
                    constants.GAUGE_ID: "100",
                    constants.STATION_NAME: "Rosario",
                    constants.RIVER: "Parana",
                    constants.LATITUDE: -32.95,
                    constants.LONGITUDE: -60.66,
                    "series_id": 111,
                    "proc_id": 1,
                    "var_id": 40,
                    "unit": "m3/s",
                },
                {
                    constants.GAUGE_ID: "100",
                    constants.STATION_NAME: "Rosario",
                    constants.RIVER: "Parana",
                    constants.LATITUDE: -32.95,
                    constants.LONGITUDE: -60.66,
                    "series_id": 112,
                    "proc_id": 1,
                    "var_id": 40,
                    "unit": "m3/s",
                },
                {
                    constants.GAUGE_ID: "200",
                    constants.STATION_NAME: "Parana",
                    constants.RIVER: "Parana",
                    constants.LATITUDE: -31.74,
                    constants.LONGITUDE: -60.52,
                    "series_id": 211,
                    "proc_id": 1,
                    "var_id": 40,
                    "unit": "m3/s",
                },
            ]
        )
        mock_station_details.side_effect = [
            {constants.ALTITUDE: 12.0, constants.AREA: 3.5},
            {constants.ALTITUDE: np.nan, constants.AREA: 8.0},
        ]

        result = self.fetcher.get_metadata()

        expected = pd.DataFrame(
            {
                constants.GAUGE_ID: ["100", "200"],
                constants.STATION_NAME: ["Rosario", "Parana"],
                constants.RIVER: ["Parana", "Parana"],
                constants.LATITUDE: [-32.95, -31.74],
                constants.LONGITUDE: [-60.66, -60.52],
                constants.COUNTRY: ["Argentina", "Argentina"],
                constants.SOURCE: ["INA Alerta", "INA Alerta"],
                constants.ALTITUDE: [12.0, np.nan],
                constants.AREA: [3.5, 8.0],
                "series_id": [111, 211],
                "proc_id": [1, 1],
                "var_id": [40, 40],
                "unit": ["m3/s", "m3/s"],
            }
        ).set_index(constants.GAUGE_ID)

        assert_frame_equal(result[expected.columns], expected)
        self.assertEqual(mock_station_details.call_count, 2)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_downloads_and_parses_csvless_series(self, mock_retry_session):
        session = MagicMock()
        mock_retry_session.return_value = session

        series_response = MagicMock()
        series_response.json.return_value = {
            "features": [
                {
                    "properties": {
                        "estacion_id": 100,
                        "id": 999,
                        "nombre": "Rosario",
                        "rio": "Parana",
                        "proc_id": 1,
                        "var_id": 40,
                        "unidad": "m3/s",
                    },
                    "geometry": {"coordinates": [-60.66, -32.95]},
                }
            ]
        }
        series_response.raise_for_status = MagicMock()

        data_response = MagicMock()
        data_response.text = "2024-01-01,2024-01-02,10.5\n2024-01-02,2024-01-03,11.5\n"
        data_response.raise_for_status = MagicMock()

        session.get.side_effect = [series_response, data_response]

        result = self.fetcher.get_data(
            gauge_id="100",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2024-01-01",
            end_date="2024-01-02",
        )

        expected = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2024-01-01", "2024-01-02"]),
                constants.DISCHARGE_DAILY_MEAN: [10.5, 11.5],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result, expected)
        self.assertEqual(session.get.call_count, 2)

    @patch("rivretrieve.argentina.ArgentinaFetcher._download_data")
    @patch("rivretrieve.argentina.ArgentinaFetcher._fetch_series_index")
    def test_get_data_for_station_8(self, mock_series_index, mock_download_data):
        mock_series_index.return_value = pd.DataFrame(
            [
                {
                    constants.GAUGE_ID: "8",
                    constants.STATION_NAME: "Andresito",
                    constants.RIVER: "IGUAZU",
                    constants.LATITUDE: -25.5833333333333,
                    constants.LONGITUDE: -53.9833333333333,
                    "series_id": 800040,
                    "proc_id": 1,
                    "var_id": 40,
                    "unit": "m3/s",
                }
            ]
        )
        mock_download_data.return_value = (
            "1990-01-01,1990-01-02,15.0\n1990-01-02,1990-01-03,15.5\n1990-01-03,1990-01-04,16.0\n"
        )

        result = self.fetcher.get_data(
            gauge_id="8",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="1990-01-01",
            end_date="1990-01-03",
        )

        expected = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["1990-01-01", "1990-01-02", "1990-01-03"]),
                constants.DISCHARGE_DAILY_MEAN: [15.0, 15.5, 16.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result, expected)
        mock_download_data.assert_called_once_with(
            "8",
            constants.DISCHARGE_DAILY_MEAN,
            "1990-01-01",
            "1990-01-03",
        )


if __name__ == "__main__":
    unittest.main()
