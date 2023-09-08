import unittest
from unittest.mock import patch, Mock
import mysql.connector
from cartola_etl.database.connection import ConnectionManager


class TestConnectionManager(unittest.TestCase):
    @patch("mysql.connector.connect")
    def test_connect(self, mock_connect):
        manager = ConnectionManager("host", "user", "password", "database", "1234")

        connection_mock = Mock(spec=mysql.connector.connection.MySQLConnection)
        mock_connect.return_value = connection_mock

        connection = manager.connect()

        mock_connect.assert_called_once_with(
            host="host",
            user="user",
            password="password",
            database="database",
            port="1234",
        )

        self.assertEqual(connection, connection_mock)

    @patch("mysql.connector.connect")
    def test_close_connection(self, mock_connect):
        manager = ConnectionManager("host", "user", "password", "database", "1234")

        connection_mock = Mock(spec=mysql.connector.connection.MySQLConnection)
        mock_connect.return_value = connection_mock

        manager.connect()

        manager.close_connection()

        connection_mock.close.assert_called_once()

        self.assertIsNone(manager.connection)


if __name__ == "__main__":
    unittest.main()
