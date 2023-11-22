from cartola_etl.database.connection import ConnectionManager
from cartola_etl.config.database_config import datawarehouse_db_config
from cartola_etl.config.database_config import NAME_DATAWAREHOUSE
from mysql.connector import Error


def main():
    """
    Creates a database if it does not already exist.

    Tries to establish a connection to the database server using the provided 
    data warehouse configuration. If successful, attempts to create a new database 
    named 'NAME_DATAWAREHOUSE' if it does not already exist.

    Returns:
        None

    Raises:
        Error: If there is an error during the database operations.
        Exception: If any other exception occurs.
    """
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
