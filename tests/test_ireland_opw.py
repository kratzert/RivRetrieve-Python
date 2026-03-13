import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import IrelandOPWFetcher, constants


class TestIrelandOPWFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = IrelandOPWFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _mock_response(payload):
        response = MagicMock()
        response.json.return_value = payload
        response.raise_for_status = MagicMock()
        response.status_code = 200
        return response

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata_parses_and_filters_station_list(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response(self._load_json("ireland_opw_metadata_sample.json"))

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["18113", "19001"])
        self.assertEqual(result_df.loc["18113", constants.STATION_NAME], "Ahane Br")
        self.assertEqual(result_df.loc["18113", constants.RIVER], "OWENTARAGLIN")
        self.assertAlmostEqual(result_df.loc["18113", constants.AREA], 76.82)
        self.assertAlmostEqual(result_df.loc["18113", constants.ALTITUDE], 108.98)
        self.assertEqual(result_df.loc["18113", "vertical_datum"], "Malin Head OSGM15")
        self.assertEqual(result_df.loc["19001", constants.RIVER], "BARROW")
        self.assertEqual(result_df.loc["19001", constants.COUNTRY], "Ireland")
        self.assertEqual(result_df.loc["19001", constants.SOURCE], self.fetcher.SOURCE)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_stage_uses_single_series_payload(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response(self._load_json("ireland_opw_stage_sample.json"))

        result_df = self.fetcher.get_data(
            gauge_id="1234",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.STAGE_DAILY_MEAN: [1.23, 1.45],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        request_url = mock_session.get.call_args.args[0]
        self.assertIn("/01234/S/year.json", request_url)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge_selects_daily_mean_series(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response(self._load_json("ireland_opw_discharge_sample.json"))

        result_df = self.fetcher.get_data(
            gauge_id="19001",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.DISCHARGE_DAILY_MEAN: [10.5, 11.5],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_temperature_accepts_padded_ids(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_response(self._load_json("ireland_opw_temperature_sample.json"))

        result_df = self.fetcher.get_data(
            gauge_id="19001",
            variable=constants.WATER_TEMPERATURE_DAILY_MEAN,
            start_date="2025-01-02",
            end_date="2025-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-02", "2025-01-03"]),
                constants.WATER_TEMPERATURE_DAILY_MEAN: [8.5, 9.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)

    def test_unsupported_variable_raises(self):
        with self.assertRaises(ValueError):
            self.fetcher.get_data("19001", constants.STAGE_INSTANT)


if __name__ == "__main__":
    unittest.main()
