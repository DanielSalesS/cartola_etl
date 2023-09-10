import unittest
from unittest.mock import Mock, patch
from cartola_etl.scripts.create_tables import main
from cartola_etl.utils import get_project_root


class TestCreateTables(unittest.TestCase):
    def setUp(self):
        self.expected_queries = []

        root_path = get_project_root()
        sql_path = root_path.joinpath("./sql/create_tables.sql")

        with open(sql_path, "r") as sql_file:
            sql_script = sql_file.read()

        self.expected_queries = [
            query for query in sql_script.split(";") if query.strip() != ""
        ]

    @patch("cartola_etl.scripts.create_tables.ConnectionManager")
    def test_script(self, mock_connection_manager):
        # Config
        mock_connection = Mock()
        mock_cursor = Mock()

        mock_connection_manager.return_value.connect.return_value.__enter__.return_value = (
            mock_connection
        )
        mock_connection.cursor.return_value = mock_cursor

        # Run
        main()

        # Check
        for query in self.expected_queries:
            mock_cursor.execute.assert_any_call(query)

        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
