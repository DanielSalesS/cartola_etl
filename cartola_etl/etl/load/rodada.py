from cartola_etl.etl.load.base_load import BaseLoad


class RodadaLoad(BaseLoad):
    """
    Loads data into the 'dim_rodada' table.

    This class provides a common interface for loading data into the
    'dim_rodada' table.

    Attributes:
        conn_manager (ConnectionManager): The connection manager object.
    """
    def load_data(self, data):
        """
        Loads data into the 'dim_rodada' table.

        Executes a SQL query to insert data into the 'dim_rodada' table.

        Args:
            data (list): A list of tuples containing the data to be loaded into the
            'dim_rodada' table.
        
        Returns:
            None
        """
        query = """
            INSERT IGNORE INTO dim_rodada (rodada_id, inicio, fim, times_escalados, media_cartoletas, media_pontos)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        self.load_single_data(data, query)
