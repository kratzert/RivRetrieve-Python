import json
import os
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
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("bosnia_herzegovina_metadata_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["4510", "4121"])
        self.assertEqual(result_df.loc["4510", constants.STATION_NAME], "HS Kaloševići")
        self.assertEqual(result_df.loc["4510", constants.RIVER], "Usora")
        self.assertAlmostEqual(result_df.loc["4510", constants.LATITUDE], 44.64680728070949)
        self.assertAlmostEqual(result_df.loc["4510", constants.LONGITUDE], 17.90406242892678)
        self.assertEqual(result_df.loc["4510", constants.COUNTRY], "Bosnia and Herzegovina")
        self.assertEqual(result_df.loc["4510", constants.SOURCE], "vodostaji.voda.ba")
        self.assertAlmostEqual(result_df.loc["4121", constants.AREA], 123.4)

    @patch("pandas.read_excel")
    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_instant_discharge_detects_station_group(self, mock_requests_session, mock_read_excel):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_read_excel.return_value = pd.DataFrame(
            {
                constants.TIME_INDEX: [
                    "01.01.2025 00:00",
                    "01.01.2025 01:00",
                    "01.01.2025 02:00",
                    "02.01.2025 00:00",
                ],
                constants.DISCHARGE_INSTANT: [1.0, 2.0, 3.0, 4.0],
            }
        )

        missing_response = MagicMock(status_code=404, content=b"")
        success_response = MagicMock(status_code=200, content=b"fake-xlsx-content")
        mock_session.get.side_effect = [missing_response, missing_response, success_response]

        result_df = self.fetcher.get_data(
            gauge_id="4510",
            variable=constants.DISCHARGE_INSTANT,
            start_date="2025-01-01",
            end_date="2025-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    ["2025-01-01 00:00:00", "2025-01-01 01:00:00", "2025-01-01 02:00:00"]
                ),
                constants.DISCHARGE_INSTANT: [1.0, 2.0, 3.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.attrs["station_group"], 3)
        self.assertEqual(mock_session.get.call_count, 3)
        self.assertIn("/1/4510/Q/Q_1Y.xlsx", mock_session.get.call_args_list[0].args[0])
        self.assertIn("/3/4510/Q/Q_1Y.xlsx", mock_session.get.call_args_list[2].args[0])

    @patch("pandas.read_excel")
    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_temperature(self, mock_requests_session, mock_read_excel):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_read_excel.return_value = pd.DataFrame(
            {
                constants.TIME_INDEX: [
                    "01.01.2025 00:00",
                    "01.01.2025 12:00",
                    "02.01.2025 00:00",
                    "02.01.2025 12:00",
                ],
                constants.WATER_TEMPERATURE_DAILY_MEAN: [10.0, 11.0, 12.0, 12.0],
            }
        )

        success_response = MagicMock(status_code=200, content=b"fake-xlsx-content")
        mock_session.get.return_value = success_response

        result_df = self.fetcher.get_data(
            gauge_id="4510",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [10.5, 12.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)


if __name__ == "__main__":
    unittest.main()
