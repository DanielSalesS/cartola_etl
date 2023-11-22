from cartola_etl.database.connection import ConnectionManager
from cartola_etl.config.database_config import datawarehouse_db_config


class BaseLoad:
    """
    Base class for loading data into the database.

    This class provides a common interface for loading data into the database.

    Attributes:
        conn_manager (ConnectionManager): The connection manager object.
    """
    def __init__(self):
        """
        Initializes a new instance of the class.

        Returns:
            None
        """
        self.conn_manager = ConnectionManager(**datawarehouse_db_config)

    def load_multiple_data(self, data, query):
        """
        Loads multiple rows of data into the database.

        Executes an executemany operation to load multiple rows of data into the 
        database using the provided SQL query and data.

        Args:
            data (list): List containing the data to be loaded into the database. 
            query (str): SQL query to execute for loading the data into the database.

        Returns:
            None
        """
        with self.conn_manager.connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, data)
                connection.commit()

    def load_single_data(self, data, query):
        """
        Loads a single row of data into the database.

        Executes an execute operation to load a single row of data into the
        database using the provided SQL query and data.

        Args:
            data (tuple): Tuple containing the data to be loaded into the database.
            query (str): SQL query to execute for loading the data into the database.
        
        Returns:
            None
        """
        with self.conn_manager.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, data)
                connection.commit()
