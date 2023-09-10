import json
import shutil
import unittest
from unittest.mock import patch, Mock
from tests.utils_test.utils import get_root_test_path, get_test_staging_area_path


class TestUtils(unittest.TestCase):
    @patch("cartola_etl.utils.get_project_root")
    def test_get_project_root(self, mock_get_project_root):
        # Config
        expected_path = get_root_test_path()
        mock_get_project_root.return_value = expected_path

        # Run
        from cartola_etl.utils import get_project_root

        result_path = get_project_root()

        # Check
        self.assertEqual(result_path, expected_path)

    @patch("cartola_etl.utils.get_staging_area_path")
    def test_get_staging_area_path(self, mock_get_staging_area_path):
        # Config
        expected_path = get_test_staging_area_path()
        mock_get_staging_area_path.return_value = expected_path

        # Run
        from cartola_etl.utils import get_staging_area_path

        result_path = get_staging_area_path()

        # Check
        self.assertEqual(result_path, expected_path)

    @patch("cartola_etl.utils.utils.requests.get")
    def test_get_current_round(self, mock_requests_get):
        # Config
        expected_round = 1
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"rodada_atual": 1}
        mock_requests_get.return_value = mock_response

        # Run
        from cartola_etl.utils import get_current_round

        result_round = get_current_round()

        # Check
        self.assertEqual(result_round, expected_round)


if __name__ == "__main__":
    unittest.main()
