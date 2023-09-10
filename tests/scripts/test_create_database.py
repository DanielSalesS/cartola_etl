import unittest
from unittest.mock import Mock, patch
from cartola_etl.scripts.create_database import main


NAME_DATAWAREHOUSE = "test_db"


class TestDatabaseCreation(unittest.TestCase):
    @patch("cartola_etl.scripts.create_database.ConnectionManager")
    @patch("cartola_etl.scripts.create_database.NAME_DATAWAREHOUSE", new=NAME_DATAWAREHOUSE)
    def test_database_creation(self, mock_connection_manager):
        # Config
        mock_connection = Mock()
        mock_cursor = Mock()

        mock_connection_manager.return_value.connect.return_value.__enter__.return_value = (
            mock_connection
        )
        mock_connection.cursor.return_value = mock_cursor

        expected_query = f"CREATE DATABASE IF NOT EXISTS {NAME_DATAWAREHOUSE}"
        
        # Run
        main()

        # Check
        
        mock_cursor.execute.assert_called_once_with(expected_query)
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
