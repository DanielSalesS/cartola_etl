import unittest
from unittest.mock import patch
from cartola_etl.database.connection import ConnectionManager
from cartola_etl.etl.load.base_load import BaseLoad


class TestBaseLoad(unittest.TestCase):
    @patch.object(ConnectionManager, "connect")
    def test_load_multiple_data(self, mock_connect):
        # Config
        data = [("v1", 25), ("v2", 30)]
        query = "INSERT INTO TABLE (column1, column2) VALUES (%s, %s)"

        mock_connection = mock_connect.return_value.__enter__.return_value
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

        # Run
        base_load = BaseLoad()
        base_load.load_multiple_data(data, query)

        # Check
        mock_cursor.executemany.assert_called_once_with(query, data)
        mock_connection.commit.assert_called_once()

    @patch.object(ConnectionManager, "connect")
    def test_load_single_data(self, mock_connect):
        # Config
        data = ("v", 25)
        query = "INSERT INTO TABLE (name, age) VALUES (%s, %s)"

        mock_connection = mock_connect.return_value.__enter__.return_value
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

        # Run
        base_load = BaseLoad()
        base_load.load_single_data(data, query)

        # Check
        mock_cursor.execute.assert_called_once_with(query, data)
        mock_connection.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
