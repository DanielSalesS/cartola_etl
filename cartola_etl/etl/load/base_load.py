from cartola_etl.database.connection import ConnectionManager
from cartola_etl.config.database_config import datawarehouse_db_config


class BaseLoad:
    def __init__(self):
        self.conn_manager = ConnectionManager(**datawarehouse_db_config)

    def load_multiple_data(self, data, query):
        with self.conn_manager.connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, data)
                connection.commit()

    def load_single_data(self, data, query):
        with self.conn_manager.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, data)
                connection.commit()
