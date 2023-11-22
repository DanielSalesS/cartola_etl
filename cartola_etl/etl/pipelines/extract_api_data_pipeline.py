from cartola_etl.etl.pipelines.base_pipeline import (
    extract_fixed_api_data,
    extract_dynamic_api_data,
)
from cartola_etl.utils import get_staging_area_path


def main():
    """
    Extracts data from Cartola FC APIs and stores it in the staging area.

    The pipeline fetches both fixed and dynamic data from Cartola FC APIs and stores it 
    in the appropriate subdirectories within the staging area. Fixed data, which is 
    relatively static, is stored in the "fixed_data" directory. Dynamic data, which 
    changes frequently, is stored in a separate directory named after the current
    round.

    Returns:
        None
    """
    staging_area_path = get_staging_area_path()
    fixed_data_path = staging_area_path.joinpath("fixed_data")

    if not fixed_data_path.is_dir():
        extract_fixed_api_data()

    extract_dynamic_api_data()


if __name__ == "__main__":
    main()
