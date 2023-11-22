from cartola_etl.database.connection import ConnectionManager
from cartola_etl.config.database_config import datawarehouse_db_config


class ScoutsDatabaseExtractor:
    """
    Extracts data from the data warehouse database.

    The class provides methods to extract data from the 'fact_pontuacoes' table within
    the data warehouse database.

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

    def extract_data(self):
        """
        Extracts data from the data warehouse database.

        Executes a SQL query to extract data from the 'fact_pontuacoes' table 
        within the data warehouse database.
        
        Returns:
            list: A list of tuples containing the extracted data from the database.
        """
        query = """
            SELECT 
                membro_equipe_id, 
                SUM(COALESCE(desarmes, 0)) AS total_desarmes,
                SUM(COALESCE(falta_cometida, 0)) AS total_falta_cometida,
                SUM(COALESCE(gol_contra, 0)) AS total_gol_contra,
                SUM(COALESCE(cartao_amarelo, 0)) AS total_cartao_amarelo,
                SUM(COALESCE(cartao_vermelho, 0)) AS total_cartao_vermelho,
                SUM(COALESCE(jogo_sem_sofrer_gol, 0)) AS total_jogo_sem_sofrer_gol,
                SUM(COALESCE(defesa_dificil, 0)) AS total_defesa_dificil,
                SUM(COALESCE(defesa_penalti, 0)) AS total_defesa_penalti,
                SUM(COALESCE(gol_sofrido, 0)) AS total_gol_sofrido,
                SUM(COALESCE(penalti_cometido, 0)) AS total_penalti_cometido,
                SUM(COALESCE(falta_sofrida, 0)) AS total_falta_sofrida,
                SUM(COALESCE(assistencia, 0)) AS total_assistencia,
                SUM(COALESCE(finalizacao_trave, 0)) AS total_finalizacao_trave,
                SUM(COALESCE(finalizacao_defendida, 0)) AS total_finalizacao_defendida,
                SUM(COALESCE(finalizacao_fora, 0)) AS total_finalizacao_fora,
                SUM(COALESCE(gols, 0)) AS total_gols,
                SUM(COALESCE(impedimento, 0)) AS total_impedimento,
                SUM(COALESCE(penalti_perdido, 0)) AS total_penalti_perdido,
                SUM(COALESCE(penalti_sofrido, 0)) AS total_penalti_sofrido,
                SUM(COALESCE(vitoria_ponto_extra, 0)) AS total_vitoria_ponto_extra
            FROM fact_pontuacoes
            GROUP BY membro_equipe_id;
        """

        with self.conn_manager.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()

        return data
