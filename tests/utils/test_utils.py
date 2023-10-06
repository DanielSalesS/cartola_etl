import json
import shutil
import unittest
from unittest.mock import patch, Mock
from cartola_etl.database.connection import ConnectionManager
from cartola_etl.utils import get_last_round_in_database
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

    @patch.object(ConnectionManager, "connect")
    def test_get_last_round_in_database(self, mock_connect):
        # Config
        expected_data = 10

        query = "SELECT MAX(rodada_id) FROM fact_pontuacoes"

        mock_connection = mock_connect.return_value.__enter__.return_value
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (
            10,
        )

        # Run
        result_data = get_last_round_in_database()

        # Check
        mock_connection.cursor.assert_called_once_with()
        mock_cursor.execute.assert_called_once_with(query)
        mock_cursor.fetchone.assert_called_once_with()
        self.assertEqual(result_data, expected_data)


if __name__ == "__main__":
    unittest.main()
