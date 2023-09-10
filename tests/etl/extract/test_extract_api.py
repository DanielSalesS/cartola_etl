import json
import shutil
import unittest
from unittest.mock import patch, Mock
from tests.utils_test.utils import get_test_staging_area_path
from cartola_etl.etl.extract.api import (
    BaseApiDataExtractor,
    FixedApiDataExtractor,
    DynamicApiDataExtractor,
)


fixed_data_endpoints = {
    "fixed_data": "fixed_data_endpoint",
}

dynamic_data_endpoints = {
    "dynamic_data": "dynamic_data_endpoint",
}


class TestBaseApiDataExtractor(unittest.TestCase):
    def tearDown(self):
        path = get_test_staging_area_path()
        shutil.rmtree(path)

    @patch("cartola_etl.etl.extract.api.requests.get")
    def test_get_data_success(self, mock_get):
        # Config
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test_data"}
        mock_get.return_value = mock_response

        endpoint = "fake_endpoint"

        # Run
        base_extract = BaseApiDataExtractor(endpoint)
        data = base_extract.get_data()

        # Check
        self.assertEqual(data, {"data": "test_data"})

    @patch("cartola_etl.etl.extract.api.requests.get")
    def test_get_data_failure(self, mock_get):
        # Config
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        endpoint = "fake_endpoint"

        # Run
        base_extract = BaseApiDataExtractor(endpoint)

        # Check
        with self.assertRaises(Exception):
            base_extract.get_data()

    @patch("cartola_etl.etl.extract.api.get_staging_area_path")
    def test_make_standard_data_path(self, mock_get_staging_area_path):
        # Config
        test_staging_area_path = get_test_staging_area_path()
        folder_name = "folder_name"
        mock_get_staging_area_path.return_value = test_staging_area_path

        # Run
        base_extract = BaseApiDataExtractor("fake_endpoint")
        result_path = base_extract.make_statandard_data_path(folder_name)

        # Check
        expected_path = test_staging_area_path.joinpath(folder_name)
        self.assertEqual(result_path, expected_path)


class TestFixedApiDataExtractor(unittest.TestCase):
    def tearDown(self):
        path = get_test_staging_area_path()
        shutil.rmtree(path)

    @patch("cartola_etl.etl.extract.api.requests.get")
    @patch("cartola_etl.etl.extract.api.get_staging_area_path")
    @patch("cartola_etl.etl.extract.api.fixed_data_endpoints", new=fixed_data_endpoints)
    def test_save_data(self, mock_staging_area_path, mock_requests_get):
        # Config
        endpoint_name = "fixed_data"
        mock_data = {"key": "value"}

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_data
        mock_requests_get.return_value = mock_response

        test_staging_area_path = get_test_staging_area_path()
        mock_staging_area_path.return_value = test_staging_area_path

        # Run
        extractor = FixedApiDataExtractor(endpoint_name)
        extractor.execute()

        # Check
        folder_name = f"fixed_data"
        file_name = f"{endpoint_name}.json"
        expected_path = test_staging_area_path.joinpath(folder_name, file_name)

        with open(expected_path, "r") as file:
            saved_data = json.load(file)

        self.assertEqual(saved_data, mock_data)


class TestDynamicApiDataExtractor(unittest.TestCase):
    def tearDown(self):
        path = get_test_staging_area_path()
        shutil.rmtree(path)

    @patch("cartola_etl.etl.extract.api.requests.get")
    @patch("cartola_etl.etl.extract.api.get_current_round")
    @patch("cartola_etl.etl.extract.api.get_staging_area_path")
    @patch("cartola_etl.etl.extract.api.dynamic_data_endpoints", new=dynamic_data_endpoints)
    def test_save_data(self, mock_staging_area_path, mock_get_current_round, mock_requests_get):
        # Config
        round_number = 1
        round_number_str = str(round_number).zfill(2)
        endpoint_name = "dynamic_data"
        mock_data = {"key": "value"}

        test_staging_area_path = get_test_staging_area_path()
        mock_get_current_round.return_value = round_number_str
        mock_staging_area_path.return_value = test_staging_area_path

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_data
        mock_requests_get.return_value = mock_response

        # Run
        extractor = DynamicApiDataExtractor(endpoint_name)
        extractor.execute()

        # Check
        folder_name = f"rodada{round_number_str}"
        file_name = f"{endpoint_name}.json"
        expected_path = test_staging_area_path.joinpath(folder_name, file_name)
        print(expected_path)

        with open(expected_path, "r") as file:
            saved_data = json.load(file)

        self.assertEqual(saved_data, mock_data)


if __name__ == "__main__":
    unittest.main()
