from cartola_etl.database.connection import ConnectionManager
from cartola_etl.config.database_config import datawarehouse_db_config
from cartola_etl.utils.utils import get_project_root
from mysql.connector import Error


def main():
    """Executes SQL commands to create tables in the data warehouse.

    Attempts to establish a connection to the data warehouse database using the provided 
    configuration. Reads the SQL script from the 'create_tables.sql' file and executes 
    each command found in the script sequentially.

    Returns:
        None
        
    Raises:
        Error: If there is an error during the execution of SQL commands.
        Exception: If any other exception occurs.
    """
    try:
        conn_manager = ConnectionManager(**datawarehouse_db_config)

        root_path = get_project_root()
        sql_path = root_path.joinpath("./sql/create_tables.sql")

        with conn_manager.connect() as connection:
            cursor = connection.cursor()

            with open(sql_path, "r") as sql_file:
                sql_script = sql_file.read()

                sql_commands = sql_script.split(";")
                for command in sql_commands:
                    print(command)

                for command in sql_commands:
                    try:
                        if command.strip() != "":
                            cursor.execute(command)
                            print("Comando executado com sucesso!")
                    except Error as e:
                        print(f"Erro ao executar comando: {e}")

            connection.commit()

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
   

