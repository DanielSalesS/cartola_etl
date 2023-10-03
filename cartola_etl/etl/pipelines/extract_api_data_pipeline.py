from cartola_etl.etl.pipelines.base_pipeline import (
    extract_fixed_api_data,
    extract_dynamic_api_data,
)
from cartola_etl.utils import get_staging_area_path


def main():
    staging_area_path = get_staging_area_path()
    fixed_data_path = staging_area_path.joinpath("fixed_data")

    if not fixed_data_path.is_dir():
        extract_fixed_api_data()

    extract_dynamic_api_data()


if __name__ == "__main__":
    main()
