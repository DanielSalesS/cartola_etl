import unittest
from unittest.mock import patch, MagicMock
import os

test_dw_db_config = {
    "NAME_DATAWAREHOUSE": "DWtest",
    "USERNAME_DATAWAREHOUSE": "username_test",
    "PASSWORD_DATAWAREHOUSE": "password_test",
    "HOST_DATAWAREHOUSE": "localhost",
    "PORT_DATAWAREHOUSE": "5432",
}


class TestDataWarehouseDBConfig(unittest.TestCase):
    @patch.dict(os.environ, test_dw_db_config)
    def setUp(self):
        from cartola_etl.config.database_config import datawarehouse_db_config

        self.config = datawarehouse_db_config

    def test_host_datawarehouse(self):
        self.assertEqual(self.config["host"], test_dw_db_config["HOST_DATAWAREHOUSE"])

    def test_username_datawarehouse(self):
        self.assertEqual(
            self.config["username"], test_dw_db_config["USERNAME_DATAWAREHOUSE"]
        )

    def test_password_datawarehouse(self):
        self.assertEqual(
            self.config["password"], test_dw_db_config["PASSWORD_DATAWAREHOUSE"]
        )

    def test_name_datawarehouse(self):
        self.assertEqual(
            self.config["database"], test_dw_db_config["NAME_DATAWAREHOUSE"]
        )

    def test_port_datawarehouse(self):
        self.assertEqual(self.config["port"], test_dw_db_config["PORT_DATAWAREHOUSE"])


if __name__ == "__main__":
    unittest.main()
