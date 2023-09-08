import os
from cartola_etl.config.utils import load_env

load_env()

HOST_DATAWAREHOUSE = os.environ.get("HOST_DATAWAREHOUSE")
USERNAME_DATAWAREHOUSE = os.environ.get("USERNAME_DATAWAREHOUSE")
PASSWORD_DATAWAREHOUSE = os.environ.get("PASSWORD_DATAWAREHOUSE")
NAME_DATAWAREHOUSE = os.environ.get("NAME_DATAWAREHOUSE")
PORT_DATAWAREHOUSE = os.environ.get("PORT_DATAWAREHOUSE")

datawarehouse_db_config = {
    "host": HOST_DATAWAREHOUSE,
    "username": USERNAME_DATAWAREHOUSE,
    "password": PASSWORD_DATAWAREHOUSE,
    "database": NAME_DATAWAREHOUSE,
    "port": PORT_DATAWAREHOUSE,
}


