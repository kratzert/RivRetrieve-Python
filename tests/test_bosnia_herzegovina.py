import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

import rivretrieve.constants as constants
from rivretrieve.bosnia_herzegovina import BosniaHerzegovinaFetcher


class TestBosniaHerzegovinaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = BosniaHerzegovinaFetcher()
        self.test_data_dir = Path(__file__).parent / "test_data"

    def _load_json(self, filename):
        with (self.test_data_dir / filename).open("r", encoding="utf-8") as file_handle:
            return json.load(file_handle)

    def _load_bytes(self, filename):
        return (self.test_data_dir / filename).read_bytes()

    @staticmethod
    def _build_response(status_code=200, content=b"", json_data=None):
        response = MagicMock()
        response.status_code = status_code
        response.content = content
        response.json.return_value = json_data
        response.raise_for_status = MagicMock()
        return response

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = self._build_response(
            json_data=self._load_json("bosnia_herzegovina_metadata_sample.json")
        )
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()

        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertEqual(list(result_df.index), ["4510", "4121"])
        self.assertEqual(result_df.loc["4510", constants.STATION_NAME], "HS Kaloševići")
        self.assertEqual(result_df.loc["4510", constants.RIVER], "Usora")
        self.assertAlmostEqual(result_df.loc["4510", constants.LATITUDE], 44.64680728070949)
        self.assertAlmostEqual(result_df.loc["4510", constants.LONGITUDE], 17.90406242892678)
        self.assertIn("metadata_station_carteasting", result_df.columns)
        self.assertIn("catchment", result_df.columns)
        self.assertEqual(result_df.loc["4510", constants.COUNTRY], "Bosnia and Herzegovina")
        self.assertEqual(result_df.loc["4510", constants.SOURCE], "vodostaji.voda.ba")
        self.assertAlmostEqual(result_df.loc["4121", constants.AREA], 123.4)
        mock_session.get.assert_called_once_with(self.fetcher.METADATA_URL, timeout=30)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_discharge_detects_station_group(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        missing_response = self._build_response(status_code=404)
        success_response = self._build_response(
            status_code=200,
            content=self._load_bytes("bosnia_herzegovina_4510_discharge_20250323.xlsx"),
        )
        mock_session.get.side_effect = [missing_response, missing_response, missing_response, success_response]

        result_df = self.fetcher.get_data(
            gauge_id="4510",
            variable=constants.DISCHARGE_INSTANT,
            start_date="2025-03-23",
            end_date="2025-03-23",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.date_range("2025-03-23 00:00:00", periods=24, freq="h"),
                constants.DISCHARGE_INSTANT: [
                    8.304,
                    7.958,
                    8.105,
                    8.007,
                    7.909,
                    7.762,
                    7.958,
                    7.665,
                    7.713,
                    8.205,
                    8.007,
                    7.328,
                    7.860,
                    8.105,
                    7.568,
                    7.811,
                    7.958,
                    7.762,
                    7.665,
                    7.280,
                    7.568,
                    7.472,
                    7.472,
                    7.280,
                ],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        self.assertEqual(result_df.attrs["station_group"], 4)
        self.assertEqual(mock_session.get.call_count, 4)
        self.assertIn("/1/4510/Q/Q_1Y.xlsx", mock_session.get.call_args_list[0].args[0])
        self.assertIn("/4/4510/Q/Q_1Y.xlsx", mock_session.get.call_args_list[3].args[0])
        self.assertTrue(all(call.kwargs["timeout"] == 20 for call in mock_session.get.call_args_list))

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage_converts_centimeters_to_meters(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        missing_response = self._build_response(status_code=404)
        success_response = self._build_response(
            status_code=200,
            content=self._load_bytes("bosnia_herzegovina_4510_stage_20250323.xlsx"),
        )
        mock_session.get.side_effect = [missing_response, missing_response, missing_response, success_response]

        result_df = self.fetcher.get_data(
            gauge_id="4510",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-03-23",
            end_date="2025-03-24",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-03-23", "2025-03-24"]),
                constants.STAGE_DAILY_MEAN: [0.8113333333333334, 0.9504166666666667],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        self.assertIn("/4/4510/H/H_1Y.xlsx", mock_session.get.call_args_list[3].args[0])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_standardized_empty_frame_for_empty_temperature_workbook(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        missing_response = self._build_response(status_code=404)
        success_response = self._build_response(
            status_code=200,
            content=self._load_bytes("bosnia_herzegovina_4510_water_temperature_20250323.xlsx"),
        )
        mock_session.get.side_effect = [missing_response, missing_response, missing_response, success_response]

        result_df = self.fetcher.get_data(
            gauge_id="4510",
            variable=constants.WATER_TEMPERATURE_INSTANT,
            start_date="2025-03-23",
            end_date="2025-03-23",
        )

        expected_df = pd.DataFrame(
            columns=[constants.TIME_INDEX, constants.WATER_TEMPERATURE_INSTANT]
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        self.assertIn("/4/4510/WT/Tvode_1Y.xlsx", mock_session.get.call_args_list[3].args[0])


if __name__ == "__main__":
    unittest.main()
