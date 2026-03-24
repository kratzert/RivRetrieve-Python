import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import SouthKoreaFetcher, constants


class TestSouthKoreaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SouthKoreaFetcher()
        self.test_data_dir = Path(__file__).parent / "test_data"

    def _load_json(self, filename):
        with open(self.test_data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_metadata(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        station_list_response = MagicMock()
        station_list_response.json.return_value = self._load_json("southkorea_station_list_sample.json")
        station_list_response.raise_for_status = MagicMock()

        station_info_response = MagicMock()
        station_info_response.json.return_value = self._load_json("southkorea_station_info_1001602_sample.json")
        station_info_response.raise_for_status = MagicMock()

        mock_session.get.side_effect = [station_list_response, station_info_response]

        result_df = self.fetcher.get_metadata()

        self.assertEqual(list(result_df.index), ["1001602"])
        self.assertEqual(result_df.index.name, constants.GAUGE_ID)
        self.assertEqual(result_df.loc["1001602", constants.STATION_NAME], "Pyeongchanggun(Songjeonggyo)")
        self.assertEqual(result_df.loc["1001602", constants.RIVER], "오대천")
        self.assertAlmostEqual(result_df.loc["1001602", constants.LATITUDE], 37.62416666666667)
        self.assertAlmostEqual(result_df.loc["1001602", constants.LONGITUDE], 128.5511111111111)
        self.assertAlmostEqual(result_df.loc["1001602", constants.AREA], 229.84)
        self.assertEqual(result_df.loc["1001602", constants.COUNTRY], "South Korea")
        self.assertEqual(result_df.loc["1001602", constants.SOURCE], "WAMIS Open API")
        self.assertEqual(mock_session.get.call_count, 2)
        self.assertIn("wl_dubwlobs", mock_session.get.call_args_list[0].args[0])
        self.assertIn("wl_obsinfo", mock_session.get.call_args_list[1].args[0])
        self.assertEqual(mock_session.get.call_args_list[0].kwargs["params"], {"output": "json"})
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["params"], {"obscd": "1001602", "output": "json"})
        self.assertEqual(mock_session.get.call_args_list[0].kwargs["timeout"], 30)
        self.assertEqual(mock_session.get.call_args_list[1].kwargs["timeout"], 10)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_discharge(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("southkorea_discharge_1001602_20230101_20230103.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1001602",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2023-01-01", "2023-01-02"], utc=True),
                constants.DISCHARGE_DAILY_MEAN: [1.5, 1.6],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        mock_session.get.assert_called_once()
        _, mock_kwargs = mock_session.get.call_args
        self.assertIn("flw_dtdata", mock_session.get.call_args.args[0])
        self.assertEqual(mock_kwargs["params"]["obscd"], "1001602")
        self.assertEqual(mock_kwargs["params"]["startdt"], "20230101")
        self.assertEqual(mock_kwargs["params"]["enddt"], "20230102")
        self.assertEqual(mock_kwargs["timeout"], 30)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_daily_stage(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("southkorea_stage_daily_1001602_20230101_20230103.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1001602",
            variable=constants.STAGE_DAILY_MEAN,
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(["2023-01-01", "2023-01-02"], utc=True),
                constants.STAGE_DAILY_MEAN: [2.5, 2.55],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        mock_session.get.assert_called_once()
        self.assertIn("wl_dtdata", mock_session.get.call_args.args[0])
        self.assertEqual(mock_session.get.call_args.kwargs["params"]["startdt"], "20230101")
        self.assertEqual(mock_session.get.call_args.kwargs["params"]["enddt"], "20230102")
        self.assertEqual(mock_session.get.call_args.kwargs["timeout"], 30)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_hourly_stage_includes_full_end_day(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = self._load_json("southkorea_stage_hourly_1001602_20230101.json")
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1001602",
            variable=constants.STAGE_HOURLY_MEAN,
            start_date="2023-01-01",
            end_date="2023-01-01",
        )

        expected_df = pd.DataFrame(
            {
                constants.TIME_INDEX: pd.to_datetime(
                    ["2023-01-01 00:00", "2023-01-01 01:00", "2023-01-01 23:00"], utc=True
                ),
                constants.STAGE_HOURLY_MEAN: [2.5, 2.55, 3.0],
            }
        ).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        mock_session.get.assert_called_once()
        _, mock_kwargs = mock_session.get.call_args
        self.assertIn("wl_hrdata", mock_session.get.call_args.args[0])
        self.assertEqual(mock_kwargs["params"]["startdt"], "20230101")
        self.assertEqual(mock_kwargs["params"]["enddt"], "20230101")
        self.assertEqual(mock_kwargs["timeout"], 30)

    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_empty_response_returns_standardized_empty_dataframe(self, mock_requests_session):
        mock_session = MagicMock()
        mock_requests_session.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = {"result": {"code": "success"}, "list": []}
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response

        result_df = self.fetcher.get_data(
            gauge_id="1001602",
            variable=constants.DISCHARGE_DAILY_MEAN,
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        expected_df = pd.DataFrame(
            columns=[constants.DISCHARGE_DAILY_MEAN],
            index=pd.DatetimeIndex([], name=constants.TIME_INDEX, tz="UTC"),
        )

        assert_frame_equal(result_df, expected_df, check_dtype=False)
        self.assertEqual(result_df.index.name, constants.TIME_INDEX)
        mock_session.get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
