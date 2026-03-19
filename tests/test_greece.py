import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import GreeceFetcher, constants


class TestGreeceFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = GreeceFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_text(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _json_response(payload):
        response = MagicMock()
        response.json.return_value = payload
        response.raise_for_status = MagicMock()
        return response

    @staticmethod
    def _text_response(payload):
        response = MagicMock()
        response.text = payload
        response.raise_for_status = MagicMock()
        return response

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata_merges_stage_water_level_and_discharge_queries(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        self.fetcher = GreeceFetcher()

        stage_page_1 = self._load_json("greece_stage_search_page1_sample.json")
        stage_page_2 = self._load_json("greece_stage_search_page2_sample.json")
        level_payload = self._load_json("greece_level_search_sample.json")
        discharge_payload = self._load_json("greece_discharge_search_sample.json")

        def side_effect(url, params=None, timeout=60):
            if url == f"{self.fetcher.BASE_URL}/stations/" and params == {"q": "ts_only: variable:stage"}:
                return self._json_response(stage_page_1)
            if url == stage_page_1["next"] and params is None:
                return self._json_response(stage_page_2)
            if url == f"{self.fetcher.BASE_URL}/stations/" and params == {"q": "ts_only: variable:level"}:
                return self._json_response(level_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/" and params == {"q": "ts_only: variable:discharge"}:
                return self._json_response(discharge_payload)
            if url == f"{self.fetcher.BASE_URL}/organizations/11/" and params is None:
                return self._json_response({"id": 11, "name": "Hellenic Centre for Marine Research"})
            if url == f"{self.fetcher.BASE_URL}/organizations/17/" and params is None:
                return self._json_response({"id": 17, "name": "National Observatory of Athens"})
            if url == f"{self.fetcher.BASE_URL}/organizations/25/" and params is None:
                return self._json_response({"id": 25, "name": "Mandra Project"})
            raise AssertionError(f"Unexpected request: url={url}, params={params}")

        mock_session.get.side_effect = side_effect

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["1356", "1458", "1534", "28082", "8424"])
        self.assertEqual(result_df.loc["1458", constants.STATION_NAME], "Ανθήλη")
        self.assertAlmostEqual(result_df.loc["1458", constants.LATITUDE], 38.856109)
        self.assertAlmostEqual(result_df.loc["1458", constants.LONGITUDE], 22.466853)
        self.assertEqual(result_df.loc["1356", "owner_name"], "National Observatory of Athens")
        self.assertEqual(result_df.loc["8424", "owner_name"], "Mandra Project")
        self.assertEqual(result_df.loc["28082", constants.SOURCE], self.fetcher.SOURCE)
        self.assertEqual(result_df.loc["28082", constants.COUNTRY], "Greece")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage_normalizes_water_level_and_converts_cm(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        self.fetcher = GreeceFetcher()

        groups_payload = self._load_json("greece_station_8424_groups_sample.json")
        timeseries_payload = self._load_json("greece_water_level_timeseries_sample.json")
        data_payload = self._load_text("greece_stage_data_sample.csv")

        def side_effect(url, params=None, timeout=60):
            if url == f"{self.fetcher.BASE_URL}/stations/8424/timeseriesgroups/" and params is None:
                return self._json_response(groups_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/8424/timeseriesgroups/881/timeseries/" and params is None:
                return self._json_response(timeseries_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/8424/timeseriesgroups/881/timeseries/10306/data/":
                self.assertEqual(
                    params,
                    {"fmt": "csv", "start_date": "2025-01-01", "end_date": "2025-01-02"},
                )
                return self._text_response(data_payload)
            if url == f"{self.fetcher.BASE_URL}/units/2/" and params is None:
                return self._json_response({"id": 2, "symbol": "cm"})
            raise AssertionError(f"Unexpected request: url={url}, params={params}")

        mock_session.get.side_effect = side_effect

        result_df = self.fetcher.get_data(
            gauge_id="8424",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.STAGE_DAILY_MEAN: [1.25, 1.40],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_discharge_prefers_initial_subdaily_series(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        self.fetcher = GreeceFetcher()

        groups_payload = {
            "count": 1,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 6,
                    "last_modified": "2019-12-24T22:43:59.004465+02:00",
                    "name": "",
                    "hidden": False,
                    "precision": 2,
                    "remarks": "",
                    "gentity": 1458,
                    "variable": 2,
                    "unit_of_measurement": 18,
                }
            ],
        }
        timeseries_payload = {
            "count": 3,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 10105,
                    "type": "Initial",
                    "last_modified": "2020-11-18T16:36:51.279025+02:00",
                    "time_step": "",
                    "name": "",
                    "publicly_available": True,
                    "timeseries_group": 6,
                },
                {
                    "id": 10106,
                    "type": "Aggregated",
                    "last_modified": "2020-11-18T16:38:15.700871+02:00",
                    "time_step": "1h",
                    "name": "Mean",
                    "publicly_available": True,
                    "timeseries_group": 6,
                },
                {
                    "id": 10115,
                    "type": "Aggregated",
                    "last_modified": "2020-12-01T21:57:52.001775+02:00",
                    "time_step": "1D",
                    "name": "Mean",
                    "publicly_available": True,
                    "timeseries_group": 6,
                },
            ],
        }
        data_payload = "2025-01-01 00:00,30.44,\n2025-01-01 01:00,30.77,\n"

        def side_effect(url, params=None, timeout=60):
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/" and params is None:
                return self._json_response(groups_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/" and params is None:
                return self._json_response(timeseries_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/10105/data/":
                return self._text_response(data_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/10106/data/":
                return self._text_response("")
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/10115/data/":
                return self._text_response("")
            if url == f"{self.fetcher.BASE_URL}/units/18/" and params is None:
                return self._json_response({"id": 18, "symbol": "m³/s"})
            raise AssertionError(f"Unexpected request: url={url}, params={params}")

        mock_session.get.side_effect = side_effect

        result_df = self.fetcher.get_data(
            gauge_id="1458",
            variable=constants.DISCHARGE_INSTANT,
            start_date="2025-01-01",
            end_date="2025-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 01:00:00"]),
                constants.DISCHARGE_INSTANT: [30.44, 30.77],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage_falls_back_when_primary_group_is_empty(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        self.fetcher = GreeceFetcher()

        groups_payload = {
            "count": 2,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 12,
                    "last_modified": "2025-10-22T16:19:48.103896+03:00",
                    "name": "Mean stage",
                    "hidden": False,
                    "precision": 2,
                    "remarks": "",
                    "gentity": 9999,
                    "variable": 14,
                    "unit_of_measurement": 6,
                },
                {
                    "id": 13,
                    "last_modified": "2024-10-22T16:19:48.103896+03:00",
                    "name": "Stage",
                    "hidden": False,
                    "precision": 2,
                    "remarks": "",
                    "gentity": 9999,
                    "variable": 14,
                    "unit_of_measurement": 6,
                },
            ],
        }
        primary_timeseries_payload = {
            "count": 1,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 200,
                    "type": "Aggregated",
                    "last_modified": "2025-10-22T16:19:48.103896+03:00",
                    "time_step": "1D",
                    "name": "Mean",
                    "publicly_available": True,
                    "timeseries_group": 12,
                }
            ],
        }
        fallback_timeseries_payload = {
            "count": 1,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 201,
                    "type": "Initial",
                    "last_modified": "2024-10-22T16:19:48.103896+03:00",
                    "time_step": "15min",
                    "name": "",
                    "publicly_available": True,
                    "timeseries_group": 13,
                }
            ],
        }

        def side_effect(url, params=None, timeout=60):
            if url == f"{self.fetcher.BASE_URL}/stations/9999/timeseriesgroups/" and params is None:
                return self._json_response(groups_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/9999/timeseriesgroups/12/timeseries/" and params is None:
                return self._json_response(primary_timeseries_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/9999/timeseriesgroups/13/timeseries/" and params is None:
                return self._json_response(fallback_timeseries_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/9999/timeseriesgroups/12/timeseries/200/data/":
                return self._text_response("")
            if url == f"{self.fetcher.BASE_URL}/stations/9999/timeseriesgroups/13/timeseries/201/data/":
                return self._text_response("2025-01-01 00:00,1.00,\n2025-01-01 12:00,3.00,\n")
            if url == f"{self.fetcher.BASE_URL}/units/6/" and params is None:
                return self._json_response({"id": 6, "symbol": "m"})
            raise AssertionError(f"Unexpected request: url={url}, params={params}")

        mock_session.get.side_effect = side_effect

        result_df = self.fetcher.get_data(
            gauge_id="9999",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01"]),
                constants.STAGE_DAILY_MEAN: [2.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge_falls_back_from_empty_aggregated_series_to_initial(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        self.fetcher = GreeceFetcher()

        groups_payload = {
            "count": 1,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 6,
                    "last_modified": "2020-12-01T21:57:52.001775+02:00",
                    "name": "",
                    "hidden": False,
                    "precision": 2,
                    "remarks": "",
                    "gentity": 1458,
                    "variable": 2,
                    "unit_of_measurement": 18,
                }
            ],
        }
        timeseries_payload = {
            "count": 3,
            "next": None,
            "previous": None,
            "results": [
                {
                    "id": 10105,
                    "type": "Initial",
                    "last_modified": "2020-11-18T16:36:51.279025+02:00",
                    "time_step": "",
                    "name": "",
                    "publicly_available": True,
                    "timeseries_group": 6,
                },
                {
                    "id": 10106,
                    "type": "Aggregated",
                    "last_modified": "2020-11-18T16:38:15.700871+02:00",
                    "time_step": "1h",
                    "name": "Mean",
                    "publicly_available": True,
                    "timeseries_group": 6,
                },
                {
                    "id": 10115,
                    "type": "Aggregated",
                    "last_modified": "2020-12-01T21:57:52.001775+02:00",
                    "time_step": "1D",
                    "name": "Mean",
                    "publicly_available": True,
                    "timeseries_group": 6,
                },
            ],
        }

        def side_effect(url, params=None, timeout=60):
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/" and params is None:
                return self._json_response(groups_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/" and params is None:
                return self._json_response(timeseries_payload)
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/10115/data/":
                return self._text_response("")
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/10105/data/":
                return self._text_response(
                    "2024-04-14 07:01,15.30,\n"
                    "2024-04-14 08:01,15.31,\n"
                    "2024-04-15 07:01,14.30,\n"
                    "2024-04-15 08:01,14.70,\n"
                )
            if url == f"{self.fetcher.BASE_URL}/stations/1458/timeseriesgroups/6/timeseries/10106/data/":
                return self._text_response("")
            if url == f"{self.fetcher.BASE_URL}/units/18/" and params is None:
                return self._json_response({"id": 18, "symbol": "m³/s"})
            raise AssertionError(f"Unexpected request: url={url}, params={params}")

        mock_session.get.side_effect = side_effect

        result_df = self.fetcher.get_data(
            gauge_id="1458",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2024-04-14",
            end_date="2024-04-15",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2024-04-14", "2024-04-15"]),
                constants.DISCHARGE_DAILY_MEAN: [15.305, 14.5],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    def test_available_variables(self):
        self.assertEqual(
            self.fetcher.get_available_variables(),
            (
                constants.DISCHARGE_DAILY_MEAN,
                constants.DISCHARGE_INSTANT,
                constants.STAGE_DAILY_MEAN,
                constants.STAGE_INSTANT,
            ),
        )

    def test_unsupported_variable_raises(self):
        with self.assertRaises(ValueError):
            self.fetcher.get_data("1458", constants.WATER_TEMPERATURE_DAILY_MEAN)


if __name__ == "__main__":
    unittest.main()
