import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
from rivretrieve import BrazilFetcher, constants
from datetime import datetime

class TestBrazilFetcher(unittest.TestCase):

    def setUp(self):
        self.fetcher = BrazilFetcher(username="testuser", password="testpass")

    @patch("rivretrieve.brazil.BrazilFetcher._get_token")
    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_discharge(self, mock_session, mock_get_token):
        mock_get_token.return_value = "fake_token"
        mock_response = MagicMock()
        mock_json = [
            {
                "Data_Hora_Dado": "2024-01-01 00:00:00.0",
                "Vazao_01": "10.0", "Vazao_02": "11.0", "Vazao_03": "12.0",
                # ... add other days up to 31, some can be None
                "Vazao_31": None
            }
        ]
        mock_response.json.return_value = mock_json
        mock_response.raise_for_status = MagicMock()
        mock_session.return_value.get.return_value = mock_response

        gauge_id = "12345678"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2024-01-01"
        end_date = "2024-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            constants.DISCHARGE_DAILY_MEAN: [10.0, 11.0, 12.0],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.return_value.get.assert_called_once()

    @patch("rivretrieve.brazil.BrazilFetcher._get_token")
    @patch("rivretrieve.utils.requests_retry_session")
    def test_get_data_stage(self, mock_session, mock_get_token):
        mock_get_token.return_value = "fake_token"
        mock_response = MagicMock()
        mock_json = [
            {
                "Data_Hora_Dado": "2024-02-01 00:00:00.0",
                "Cota_01": "150", "Cota_02": "155", 
                # ... add other days up to 29
                "Cota_29": "160"
            }
        ]
        mock_response.json.return_value = mock_json
        mock_response.raise_for_status = MagicMock()
        mock_session.return_value.get.return_value = mock_response

        gauge_id = "12345678"
        variable = constants.STAGE_DAILY_MEAN
        start_date = "2024-02-01"
        end_date = "2024-02-02"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(["2024-02-01", "2024-02-02"]),
            constants.STAGE_DAILY_MEAN: [1.50, 1.55], # Converted to meters
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.TIME_INDEX)

        assert_frame_equal(result_df, expected_df)
        mock_session.return_value.get.assert_called_once()

if __name__ == "__main__":
    unittest.main()
