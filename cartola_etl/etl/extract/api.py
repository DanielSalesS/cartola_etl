import json
import requests
from cartola_etl.utils import get_staging_area_path, get_current_round
from cartola_etl.config.etl_config import fixed_data_endpoints, dynamic_data_endpoints


class BaseApiDataExtractor:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.data = None

    def get_data(self):
        response = requests.get(self.endpoint)
        if response.status_code == 200:
            return response.json()

        raise Exception(
            f"Failed to get data from {self.endpoint}. Status code: {response.status_code}"
        )

    def extract_data(self):
        self.data = self.get_data()

    def make_statandard_data_path(self, folder_name):
        staging_area_path = get_staging_area_path()
        data_path = staging_area_path.joinpath(folder_name)
        data_path.mkdir(parents=True, exist_ok=True)
        return data_path


class FixedApiDataExtractor(BaseApiDataExtractor):
    def __init__(self, endpoint_name):
        super().__init__(fixed_data_endpoints[endpoint_name])
        self.endpoint_name = endpoint_name

    def save_data(self):
        folder_name = "fixed_data"
        data_path = self.make_statandard_data_path(folder_name)
        filename = f"{self.endpoint_name}.json"
        file_path = data_path.joinpath(filename)

        with open(file_path, "w") as file:
            json.dump(self.data, file)

    def execute(self):
        self.extract_data()
        self.save_data()


class DynamicApiDataExtractor(BaseApiDataExtractor):
    def __init__(self, endpoint_name: str):
        super().__init__(dynamic_data_endpoints[endpoint_name])
        self.endpoint_name = endpoint_name

    def get_round_number_str(self):
        current_round = get_current_round()
        return str(current_round).zfill(2)

    def save_data(self):
        round_number = self.get_round_number_str()
        folder_name = f"rodada{round_number}"
        data_path = self.make_statandard_data_path(folder_name)
        filename = f"{self.endpoint_name}.json"
        file_path = data_path.joinpath(filename)

        with open(file_path, "w") as file:
            json.dump(self.data, file)

    def execute(self):
        self.extract_data()
        self.save_data()
