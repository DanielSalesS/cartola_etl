import json
from pathlib import Path
import requests
from cartola_etl.config.etl_config import (
    dynamic_data_endpoints,
    ROOT_STAGING_AREA_PATH,
    BASE_YEAR,
)
from cartola_etl.config.database_config import datawarehouse_db_config
from cartola_etl.database.connection import ConnectionManager


def get_project_root():
    """
    Returns the root path of the project.

    Returns:
        Path: Root path of the project.
    """
    return Path(__file__).parent.parent


def get_staging_area_path():
    """
    Retrieves the path to the staging area.

    Constructs the path to the staging area where raw data is stored for processing.

    Returns:
        Path: Path to the staging area.
    """
    root_staging_area_path = ROOT_STAGING_AREA_PATH or get_project_root()
    path = Path(root_staging_area_path)
    staging_area_path = path.joinpath("staging_area", "raw_data", BASE_YEAR)
    staging_area_path.mkdir(parents=True, exist_ok=True)
    return staging_area_path


def get_current_round():
    """
    Fetches the current round number from a dynamic API endpoint.

    Retrieves the current round number of a sports tournament from an API endpoint.

    Returns:
        int or None: Current round number if available, else None.
    """
    endpoint = dynamic_data_endpoints["mercado_status"]
    response = requests.get(endpoint)
    if response.status_code == 200:
        return response.json()["rodada_atual"]

    return None


def get_last_round_in_database():
    """
    Retrieves the last round number stored in the database.

    Connects to the data warehouse database and retrieves the maximum round number 
    stored in the 'fact_pontuacoes' table.

    Returns:
        int: Last round number in the database or 0 if no round is found.
    """
    with ConnectionManager(**datawarehouse_db_config).connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(rodada_id) FROM fact_pontuacoes")
            last_round = cursor.fetchone()[0]

    return last_round if last_round is not None else 0
