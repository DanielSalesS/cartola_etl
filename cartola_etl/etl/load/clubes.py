from cartola_etl.etl.load.base_load import BaseLoad


class ClubesLoad(BaseLoad):
    """
    Loads data into the 'dim_clube' table.

    This class provides a common interface for loading data into the 'dim_clube' table.

    Attributes:
        conn_manager (ConnectionManager): The connection manager object.
    """
    def load_data(self, data):
        """
        Loads data into the 'dim_clube' table.

        Executes a SQL query to insert data into the 'dim_clube' table.

        Args:
            data (list): A list of tuples containing the data to be loaded into the
            'dim_clube' table.

        Returns:
            None
        """
        query = """
            INSERT IGNORE INTO dim_clube (clube_id, nome_clube, abreviacao)
            VALUES (%s, %s, %s)
        """

        self.load_multiple_data(data, query)
