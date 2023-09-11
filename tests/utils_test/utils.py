from pathlib import Path
from cartola_etl.config.etl_config import BASE_YEAR


def get_root_test_path():
    return Path(__file__).parent.parent


def get_test_staging_area_path():
    test_path = get_root_test_path()
    test_path = test_path.joinpath("test_files", "staging_area", "raw_data", BASE_YEAR)
    test_path.mkdir(parents=True, exist_ok=True)
    return test_path
