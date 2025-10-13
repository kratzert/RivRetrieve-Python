import json
import os
import unittest
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from rivretrieve import AustraliaFetcher, constants


class TestAustraliaFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = AustraliaFetcher()
        self.test_data_dir = os.path.join(os.path.dirname(__file__), "test_data")

    def load_sample_data(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r") as f:
            return f.read()

    def load_sample_json(self, filename):
        with open(os.path.join(self.test_data_dir, filename), "r") as f:
            return json.load(f)

    @patch("rivretrieve.australia.AustraliaFetcher._make_bom_request")
    def test_get_data_discharge(self, mock_make_bom_request):
        sample_json = self.load_sample_json("australia_getTimeseriesList_sample.json")
        sample_csv = self.load_sample_data("australia_sample.csv")

        def bom_request_side_effect(params):
            if params.get("request") == "getTimeseriesList":
                return sample_json
            elif params.get("request") == "getTimeseriesValues":
                return sample_csv
            return None

        mock_make_bom_request.side_effect = bom_request_side_effect

        gauge_id = "405212"
        variable = constants.DISCHARGE
        start_date = "2010-01-01"
        end_date = "2010-01-03"

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)

        expected_data = {
            constants.TIME_INDEX: pd.to_datetime(
                ["2010-01-01", "2010-01-02", "2010-01-03"]
            ),
            constants.DISCHARGE: [0.000, 3.710, 3.211],
        }
        expected_df = pd.DataFrame(expected_data)

        assert_frame_equal(result_df, expected_df)
        self.assertEqual(mock_make_bom_request.call_count, 2)


if __name__ == "__main__":
    unittest.main()
