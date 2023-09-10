from cartola_etl.database.connection import ConnectionManager
from cartola_etl.config.database_config import datawarehouse_db_config
from cartola_etl.config.database_config import NAME_DATAWAREHOUSE
from mysql.connector import Error


def main():
    try:
        temp_db_config = datawarehouse_db_config.copy()
        temp_db_config["database"] = None
        conn_manager = ConnectionManager(**temp_db_config)

        with conn_manager.connect() as connection:
            cursor = connection.cursor()

            create_db_query = f"CREATE DATABASE IF NOT EXISTS {NAME_DATAWAREHOUSE}"

            cursor.execute(create_db_query)

            connection.commit()

            print(f"Banco de dados criado com sucesso!")

    except Error as e:
        print(f"Erro de banco de dados: {e}")
        connection.rollback()
    except Exception as e:
        print(f"Erro: {e}")
        connection.rollback()
    finally:
        cursor.close()


if __name__ == "__main__":
    main()
