import json
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from rivretrieve import NorwayFetcher, constants


class TestNorwayFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = NorwayFetcher()
        self.base_path = "usr/local/google/home/kratzert/Projects/RivRetrieve/RivRetrieve-Python/tests/test_data"

    def mocked_requests_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code

            def json(self):
                return self.json_data

            def raise_for_status(self):
                if self.status_code != 200:
                    raise Exception(f"Status code {self.status_code}")

        if "Stations" in args[0]:
            if "Active=0" in args[0]:
                with open(
                    "/usr/local/google/home/kratzert/Projects/RivRetrieve/RivRetrieve-Python/tests/test_data/norway_metadata_active_0.json",
                    "r",
                ) as f:
                    data = json.load(f)
                return MockResponse({"data": [data]}, 200)
            elif "Active=1" in args[0]:
                with open(
                    "/usr/local/google/home/kratzert/Projects/RivRetrieve/RivRetrieve-Python/tests/test_data/norway_metadata_active_1.json",
                    "r",
                ) as f:
                    data = json.load(f)
                return MockResponse({"data": [data]}, 200)
        elif "Observations" in args[0]:
            with open(
                "/usr/local/google/home/kratzert/Projects/RivRetrieve/RivRetrieve-Python/tests/test_data/norway_discharge_sample.json",
                "r",
            ) as f:
                data = json.load(f)
            return MockResponse(data, 200)

        return MockResponse(None, 404)

    @patch("requests.Session.get", side_effect=mocked_requests_get)
    def test_get_metadata(self, mock_get):
        expected_data = {
            constants.GAUGE_ID: ["1.10.0", "1.15.0"],
            constants.STATION_NAME: ["Skotberg bru", "Femsjø"],
            constants.LATITUDE: [59.20959, 59.13015],
            constants.LONGITUDE: [11.69155, 11.48516],
            constants.ALTITUDE: [114, 81],
            constants.RIVER: ["Haldenvassdraget", "Haldenvassdraget"],
            "councilNumber": ["3124", "3101"],
            "councilName": ["Aremark", "Halden"],
            "countyName": ["Østfold", "Østfold"],
            "drainageBasinKey": [5, 7],
            "hierarchy": ["Haldenvassdraget", "Haldenvassdraget"],
            "lakeArea": [0.0, 10.73],
            "lakeName": ["", "Femsjøen"],
            "lakeNo": [0, 316],
            "regineNo": ["001.C6", "001.B10"],
            "reservoirNo": ["", "78"],
            "reservoirName": ["", "FEMSJØEN"],
            "stationTypeName": ["", "Privat, pålagt"],
            "stationStatusName": ["Nedlagt", "Aktiv"],
            constants.AREA: [1255.46, 1514.61],
            "drainageBasinAreaNorway": [1253.817, 1495.678],
            "gradient1085": [0.25, 0.29],
            "gradientBasin": [None, None],
            "gradientRiver": [1.93, 0.91],
            "heightMinimum": [92, 79],
            "heightHypso10": [129, 122],
            "heightHypso20": [141, 138],
            "heightHypso30": [160, 153],
            "heightHypso40": [177, 169],
            "heightHypso50": [194, 184],
            "heightHypso60": [211, 201],
            "heightHypso70": [232, 220],
            "heightHypso80": [259, 246],
            "heightHypso90": [288, 282],
            "heightMaximum": [406, 407],
            "lengthKmBasin": [95.98, 104.61],
            "lengthKmRiver": [120.81, 144.79],
            "percentAgricul": [12.25, 11.16],
            "percentBog": [4.07, 3.96],
            "percentEffBog": [np.nan, np.nan],
            "percentEffLake": [3.12, 3.34],
            "percentForest": [73.62, 74.41],
            "percentGlacier": [0, 0],
            "percentLake": [8.5, 8.85],
            "percentMountain": [0, 0],
            "percentUrban": [0.52, 0.46],
            "utmZoneGravi": [np.nan, np.nan],
            "utmEastGravi": [np.nan, np.nan],
            "utmNorthGravi": [np.nan, np.nan],
            "utmZoneInlet": [np.nan, np.nan],
            "utmEastInlet": [np.nan, np.nan],
            "utmNorthInlet": [np.nan, np.nan],
            "utmZoneOutlet": [np.nan, np.nan],
            "utmEastOutlet": [np.nan, np.nan],
            "utmNorthOutlet": [np.nan, np.nan],
            "annualRunoff": [570.07, 677.66],
            "specificDischarge": [14.39, 14.18],
            "regulationArea": [1155.26, 1512.44],
            "areaReservoirs": [71.31, 98.51],
            "volumeReservoirs": [82.8, 119.3],
            "regulationPartReservoirs": [0.15, 0.18],
            "transferAreaIn": [0.0, 178.99],
            "transferAreaOut": [0, 0],
            "reservoirAreaIn": [0.0, 4.64],
            "reservoirAreaOut": [0.0, 10.83],
            "reservoirVolumeIn": [0.0, 17.5],
            "reservoirVolumeOut": [0.0, 11.2],
            "remainingArea": [0, 0],
            "numberReservoirs": [15, 19],
            "firstYearRegulation": [1942, 1924],
            "catchmentRegTypeName": ["regulert m/magasinregulering", "Regulert m/magasinregulering og overføringer"],
            "owner": ["", "HALDENVASSDRAGETS BRUKSEIERFORENING"],
            "qNumberOfYears": [None, 84.0],
            "qStartYear": [None, 1940.0],
            "qEndYear": [None, 2024.0],
            "qm": [np.nan, np.nan],
            "q5": [np.nan, np.nan],
            "q10": [np.nan, np.nan],
            "q20": [np.nan, np.nan],
            "q50": [np.nan, np.nan],
            "hm": [None, 79.5405],
            "h5": [None, 79.6792],
            "h10": [None, 79.8333],
            "h20": [None, 80.0008],
            "h50": [None, 80.2505],
            "culQm": [np.nan, np.nan],
            "culQ5": [np.nan, np.nan],
            "culQ10": [np.nan, np.nan],
            "culQ20": [np.nan, np.nan],
            "culQ50": [np.nan, np.nan],
            "culHm": [None, 79.5405],
            "culH5": [None, 79.6792],
            "culH10": [None, 79.8333],
            "culH20": [None, 80.0008],
            "culH50": [None, 80.2505],
            "utmEast_Z33": [311128, 298887],
            "utmNorth_Z33": [6568077, 6559841],
            "seriesList": [
                [
                    {
                        "parameterName": "Vannstand",
                        "parameter": 1000,
                        "versionNo": 1,
                        "unit": "m",
                        "serieFrom": "1851-11-16T00:00:00",
                        "serieTo": "1853-12-30T00:00:00",
                        "resolutionList": [
                            {
                                "resTime": 0,
                                "method": "Instantaneous",
                                "timeOffset": 0,
                                "dataFromTime": "1851-11-16T11:00:00Z",
                                "dataToTime": "1853-12-30T11:00:00Z",
                            },
                            {
                                "resTime": 1440,
                                "method": "Mean",
                                "timeOffset": 0,
                                "dataFromTime": "1851-11-16T11:00:00Z",
                                "dataToTime": "1853-12-30T11:00:00Z",
                            },
                        ],
                    }
                ],
                [
                    {
                        "parameterName": "Vannstand",
                        "parameter": 1000,
                        "versionNo": 2,
                        "unit": "m",
                        "serieFrom": "1939-07-01T00:00:00",
                        "serieTo": None,
                        "resolutionList": [
                            {
                                "resTime": 0,
                                "method": "Instantaneous",
                                "timeOffset": 0,
                                "dataFromTime": "1939-07-01T11:00:00Z",
                                "dataToTime": "2025-10-24T12:00:00Z",
                            },
                            {
                                "resTime": 60,
                                "method": "Mean",
                                "timeOffset": 0,
                                "dataFromTime": "2020-12-13T12:00:00Z",
                                "dataToTime": "2025-10-24T12:00:00Z",
                            },
                            {
                                "resTime": 1440,
                                "method": "Mean",
                                "timeOffset": 0,
                                "dataFromTime": "1939-07-01T11:00:00Z",
                                "dataToTime": "2025-10-23T11:00:00Z",
                            },
                        ],
                    },
                    {
                        "parameterName": "Magasinvolum",
                        "parameter": 1004,
                        "versionNo": 2,
                        "unit": "millioner m³",
                        "serieFrom": None,
                        "serieTo": None,
                        "resolutionList": [
                            {
                                "resTime": 0,
                                "method": "Instantaneous",
                                "timeOffset": 0,
                                "dataFromTime": "1939-07-01T11:00:00Z",
                                "dataToTime": "2025-10-24T12:00:00Z",
                            },
                            {
                                "resTime": 60,
                                "method": "Mean",
                                "timeOffset": 0,
                                "dataFromTime": "2020-12-13T12:00:00Z",
                                "dataToTime": "2025-10-24T12:00:00Z",
                            },
                            {
                                "resTime": 1440,
                                "method": "Mean",
                                "timeOffset": 0,
                                "dataFromTime": "1939-07-01T11:00:00Z",
                                "dataToTime": "2025-10-23T11:00:00Z",
                            },
                        ],
                    },
                ],
            ],
        }
        expected_df = pd.DataFrame(expected_data).set_index(constants.GAUGE_ID)

        result_df = self.fetcher.get_metadata()
        # Sort columns for consistent comparison
        result_df = result_df.sort_index(axis=1)
        expected_df = expected_df.sort_index(axis=1)

        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("requests.Session.get", side_effect=mocked_requests_get)
    def test_get_data(self, mock_get):
        gauge_id = "100.1.0"
        variable = constants.DISCHARGE_DAILY_MEAN
        start_date = "2024-01-01"
        end_date = "2024-01-05"

        expected_index = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"])
        expected_data = {variable: [2.291431, 1.937256, 1.772134, 1.675695]}
        expected_df = pd.DataFrame(expected_data, index=expected_index)
        expected_df.index.name = constants.TIME_INDEX

        result_df = self.fetcher.get_data(gauge_id, variable, start_date, end_date)
        pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


if __name__ == "__main__":
    unittest.main()
