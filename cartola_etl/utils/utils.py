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
    return Path(__file__).parent.parent


def get_staging_area_path():
    root_staging_area_path = ROOT_STAGING_AREA_PATH or get_project_root()
    path = Path(root_staging_area_path)
    staging_area_path = path.joinpath("staging_area", "raw_data", BASE_YEAR)
    staging_area_path.mkdir(parents=True, exist_ok=True)
    return staging_area_path


def get_current_round():
    endpoint = dynamic_data_endpoints["mercado_status"]
    response = requests.get(endpoint)
    if response.status_code == 200:
        return response.json()["rodada_atual"]

    return None


def get_last_round_in_database():
    with ConnectionManager(**datawarehouse_db_config).connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT MAX(rodada_id) FROM fact_pontuacao")
            last_round = cursor.fetchone()[0]

    return last_round if last_round is not None else 0
