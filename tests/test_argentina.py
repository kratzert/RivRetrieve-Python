import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import ArgentinaFetcher, constants


class TestArgentinaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = ArgentinaFetcher()
        self.test_data_dir = Path(__file__).parent / "test_data"

    def _load_json(self, filename):
        with (self.test_data_dir / filename).open("r", encoding="utf-8") as file_handle:
            return json.load(file_handle)

    def _load_text(self, filename):
        return (self.test_data_dir / filename).read_text(encoding="utf-8")

    @staticmethod
    def _build_response(*, json_data=None, text=""):
        response = MagicMock()
        response.json.return_value = json_data
        response.text = text
        response.raise_for_status = MagicMock()
        return response

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

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata_uses_raw_series_and_station_payloads(self, mock_requests_retry_session):
        session = MagicMock()
        mock_requests_retry_session.return_value = session

        def mock_get(url, *args, **kwargs):
            if url.endswith("/obs/puntual/series"):
                return self._build_response(json_data=self._load_json("argentina_8_series_index.json"))
            if url.endswith("/obs/puntual/estaciones/8"):
                return self._build_response(json_data=self._load_json("argentina_8_metadata.json"))
            raise AssertionError(f"Unexpected URL: {url}")

        session.get.side_effect = mock_get

        result = self.fetcher.get_metadata()

        expected = pd.DataFrame(
            {
                constants.GAUGE_ID: ["8"],
                constants.STATION_NAME: ["Andresito"],
                constants.RIVER: ["IGUAZU"],
                constants.LATITUDE: [-25.5833333333333],
                constants.LONGITUDE: [-53.9833333333333],
                constants.COUNTRY: ["Argentina"],
                constants.SOURCE: ["INA Alerta"],
                constants.ALTITUDE: [float("nan")],
                constants.AREA: [66551.6507338408],
                "series_id": [26605],
                "proc_id": [1],
                "var_id": [40],
                "unit": [None],
            }
        ).set_index(constants.GAUGE_ID)

        self.assertEqual(result.index.name, constants.GAUGE_ID)
        self.assertIn("series_id", result.columns)
        self.assertIn("proc_id", result.columns)
        self.assertEqual(session.get.call_count, 2)
        assert_frame_equal(result[expected.columns], expected, check_dtype=False)

        first_call = session.get.call_args_list[0]
        self.assertEqual(first_call.kwargs["params"]["format"], "geojson")
        self.assertEqual(first_call.kwargs["params"]["var_id"], 40)
        self.assertEqual(first_call.kwargs["params"]["GeneralCategory"], "Hydrology")
        self.assertEqual(first_call.kwargs["params"]["data_availability"], "h")

        second_call = session.get.call_args_list[1]
        self.assertEqual(second_call.kwargs["params"]["format"], "json")
        self.assertEqual(second_call.kwargs["params"]["get_drainage_basin"], "true")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_downloads_and_parses_csvless_series(self, mock_requests_retry_session):
        session = MagicMock()
        mock_requests_retry_session.return_value = session

        def mock_get(url, *args, **kwargs):
            if url.endswith("/obs/puntual/series"):
                return self._build_response(json_data=self._load_json("argentina_8_series_index.json"))
            if url.endswith("/getObservaciones"):
                return self._build_response(text=self._load_text("argentina_8_discharge_20060101_20060103.csvless"))
            raise AssertionError(f"Unexpected URL: {url}")

        session.get.side_effect = mock_get

        result = self.fetcher.get_data(
            gauge_id="8",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2006-01-01",
            end_date="2006-01-03",
        )

        expected = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2006-01-01", "2006-01-02", "2006-01-03"]),
                constants.DISCHARGE_DAILY_MEAN: [638.79, 557.84, 517.93],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result, expected, check_dtype=False)
        self.assertEqual(result.index.name, constants.TIME_INDEX)
        self.assertEqual(session.get.call_count, 2)

        first_call = session.get.call_args_list[0]
        self.assertEqual(first_call.kwargs["params"]["format"], "geojson")
        self.assertEqual(first_call.kwargs["params"]["var_id"], 40)

        second_call = session.get.call_args_list[1]
        self.assertEqual(second_call.kwargs["params"]["series_id"], 26605)
        self.assertEqual(second_call.kwargs["params"]["timestart"], "2006-01-01")
        self.assertEqual(second_call.kwargs["params"]["timeend"], "2006-01-04")
        self.assertEqual(second_call.kwargs["params"]["format"], "csvless")
        self.assertEqual(second_call.kwargs["params"]["no_id"], "true")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_standardized_empty_frame_for_empty_payload(self, mock_requests_retry_session):
        session = MagicMock()
        mock_requests_retry_session.return_value = session

        def mock_get(url, *args, **kwargs):
            if url.endswith("/obs/puntual/series"):
                return self._build_response(json_data=self._load_json("argentina_8_series_index.json"))
            if url.endswith("/getObservaciones"):
                return self._build_response(
                    text=self._load_text("argentina_8_discharge_empty_20051228_20051230.csvless")
                )
            raise AssertionError(f"Unexpected URL: {url}")

        session.get.side_effect = mock_get

        result = self.fetcher.get_data(
            gauge_id="8",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2005-12-28",
            end_date="2005-12-30",
        )

        expected = pd.DataFrame(columns=[constants.TIME_INDEX, constants.DISCHARGE_DAILY_MEAN]).set_index(
            constants.TIME_INDEX
        )

        assert_frame_equal(result, expected, check_dtype=False)
        self.assertEqual(result.index.name, constants.TIME_INDEX)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_standardized_empty_frame_for_missing_station(self, mock_requests_retry_session):
        session = MagicMock()
        mock_requests_retry_session.return_value = session
        session.get.return_value = self._build_response(json_data=self._load_json("argentina_8_series_index.json"))

        result = self.fetcher.get_data(
            gauge_id="999999",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2006-01-01",
            end_date="2006-01-03",
        )

        expected = pd.DataFrame(columns=[constants.TIME_INDEX, constants.DISCHARGE_DAILY_MEAN]).set_index(
            constants.TIME_INDEX
        )

        assert_frame_equal(result, expected, check_dtype=False)
        self.assertEqual(result.index.name, constants.TIME_INDEX)
        session.get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
