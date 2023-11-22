from cartola_etl.etl.load.base_load import BaseLoad


class MembrosEquipesLoad(BaseLoad):
    """
    Loads data into the 'dim_membros_equipe' table.

    This class provides a common interface for loading data into the 
    'dim_membros_equipe' table.

    Attributes:
        conn_manager (ConnectionManager): The connection manager object.
    """
    def load_data(self, data):
        """
        Loads data into the 'dim_membro_equipe' table.

        Executes a SQL query to insert data into the 'dim_membros_equipe' table.

        Args:
            data (list): A list of tuples containing the data to be loaded into the
            'dim_membros_equipe' table.
        
        Returns:
            None
        """
        query = """
            INSERT IGNORE INTO dim_membro_equipe (membro_equipe_id, nome_membro, posicao)
            VALUES (%s, %s, %s)
        """

        self.load_multiple_data(data, query)
