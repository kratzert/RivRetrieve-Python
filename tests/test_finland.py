import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import FinlandFetcher, constants


class TestFinlandFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = FinlandFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("finland_metadata_sample.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["897", "1900", "3094"])
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertEqual(result_df.loc["897", constants.STATION_NAME], "Juankoski")
        self.assertEqual(result_df.loc["897", constants.RIVER], "Hiitolanjoki")
        self.assertEqual(result_df.loc["897", self.fetcher.SUPPORTED_VARIABLE_COLUMN], constants.DISCHARGE_DAILY_MEAN)
        self.assertEqual(result_df.loc["1900", self.fetcher.SUPPORTED_VARIABLE_COLUMN], constants.STAGE_DAILY_MEAN)
        self.assertEqual(
            result_df.loc["3094", self.fetcher.SUPPORTED_VARIABLE_COLUMN],
            constants.WATER_TEMPERATURE_DAILY_MEAN,
        )
        self.assertEqual(result_df.loc["897", constants.COUNTRY], "Finland")
        self.assertEqual(result_df.loc["897", constants.SOURCE], self.fetcher.SOURCE)
        self.assertAlmostEqual(result_df.loc["897", constants.LATITUDE], 61.4348852475, places=5)
        self.assertAlmostEqual(result_df.loc["897", constants.LONGITUDE], 29.3429991331, places=5)
        mock_session.get.assert_called_once()
        first_call = mock_session.get.call_args
        self.assertIn("/odata/Paikka", first_call.args[0])
        self.assertEqual(first_call.kwargs["params"]["$top"], 500)
        self.assertIn("Suure_Id eq 1", first_call.kwargs["params"]["$filter"])
        self.assertIn("Suure_Id eq 2", first_call.kwargs["params"]["$filter"])
        self.assertIn("Suure_Id eq 11", first_call.kwargs["params"]["$filter"])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("finland_897_discharge_20260325_20260327.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="897",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2026-03-25",
            end_date="2026-03-27",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2026-03-25", "2026-03-26", "2026-03-27"]),
                constants.DISCHARGE_DAILY_MEAN: [6.02, 6.02, 6.02],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        self.assertIn("/odata/Virtaama", mock_session.get.call_args.args[0])
        self.assertEqual(mock_session.get.call_args.kwargs["params"]["$orderby"], "Aika asc")
        self.assertIn("Paikka_Id eq 897", mock_session.get.call_args.kwargs["params"]["$filter"])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage_converts_centimeters_to_meters(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("finland_1900_stage_20250101_20250103.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1900",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
                constants.STAGE_DAILY_MEAN: [3.29, 3.31, 3.30],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        self.assertIn("/odata/Vedenkorkeus", mock_session.get.call_args.args[0])
        self.assertIn("Paikka_Id eq 1900", mock_session.get.call_args.kwargs["params"]["$filter"])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_temperature(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("finland_3094_temperature_20251228_20260101.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="3094",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2025-12-28",
            end_date="2026-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    ["2025-12-28", "2025-12-29", "2025-12-30", "2025-12-31", "2026-01-01"]
                ),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [2.0, 1.6, 1.3, 0.3, 0.2],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.get.assert_called_once()
        self.assertIn("/odata/LampoPintavesi", mock_session.get.call_args.args[0])
        self.assertIn("Paikka_Id eq 3094", mock_session.get.call_args.kwargs["params"]["$filter"])

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_empty_response_returns_standardized_empty_dataframe(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("finland_empty_response.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="973",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-05",
        )

        expected_df = pd.DataFrame(
            columns=[constants.DISCHARGE_DAILY_MEAN],
            index=pd.DatetimeIndex([], name=constants.TIME_INDEX),
        )

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        mock_session.get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
