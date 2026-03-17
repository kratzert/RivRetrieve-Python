import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import ItalyToscanyFetcher, constants


class TestItalyToscanyFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = ItalyToscanyFetcher()
        self.test_data_dir = Path(os.path.dirname(__file__)) / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_text(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def _mock_json_response(payload):
        response = MagicMock()
        response.json.return_value = payload
        response.raise_for_status = MagicMock()
        return response

    @staticmethod
    def _mock_text_response(payload):
        response = MagicMock()
        response.text = payload
        response.raise_for_status = MagicMock()
        return response

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata_merges_wfs_and_station_table(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.side_effect = [
            self._mock_json_response(self._load_json("italy_toscany_metadata_sample.json")),
            self._mock_text_response(self._load_text("italy_toscany_station_list_sample.js")),
        ]

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["TOS01004005", "TOS01004007", "TOS01004379"])
        self.assertEqual(result_df.loc["TOS01004005", constants.STATION_NAME], "Carrara")
        self.assertEqual(result_df.loc["TOS01004005", constants.RIVER], "Carrione")
        self.assertEqual(result_df.loc["TOS01004005", "basin"], "Carrione")
        self.assertEqual(result_df.loc["TOS01004005", "hydro_zone"], "V")
        self.assertEqual(result_df.loc["TOS01004005", "municipality"], "Carrara")
        self.assertAlmostEqual(result_df.loc["TOS01004005", constants.LATITUDE], 44.082, places=3)
        self.assertAlmostEqual(result_df.loc["TOS01004005", constants.LONGITUDE], 10.103, places=3)
        self.assertAlmostEqual(result_df.loc["TOS01004005", "zero_idrometrico"], 95.69, places=2)
        self.assertEqual(result_df.loc["TOS01004005", constants.COUNTRY], "Italy")
        self.assertEqual(result_df.loc["TOS01004005", constants.SOURCE], self.fetcher.SOURCE)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = self._mock_text_response(self._load_text("italy_toscany_stage_sample.csv"))
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="TOS02004365",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.STAGE_DAILY_MEAN: [1.45, 1.47],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        params = mock_session.get.call_args.kwargs["params"]
        self.assertEqual(params["IDST"], "idro_l")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = self._mock_text_response(self._load_text("italy_toscany_discharge_sample.csv"))
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="TOS02004365",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-03",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2025-01-01", "2025-01-02"]),
                constants.DISCHARGE_DAILY_MEAN: [130.309, 109.667],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        params = mock_session.get.call_args.kwargs["params"]
        self.assertEqual(params["IDST"], "idro_p")

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_returns_empty_when_archive_has_no_table(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session
        mock_session.get.return_value = self._mock_text_response("no data available")

        result_df = self.fetcher.get_data(
            gauge_id="missing-station",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2025-01-01",
            end_date="2025-01-03",
        )

        self.assertTrue(result_df.empty)

    def test_unsupported_variable_raises(self):
        with self.assertRaises(ValueError):
            self.fetcher.get_data("TOS02004365", constants.WATER_TEMPERATURE_DAILY_MEAN)


if __name__ == "__main__":
    unittest.main()
