import json
import requests
from cartola_etl.utils import get_staging_area_path, get_current_round
from cartola_etl.config.etl_config import fixed_data_endpoints, dynamic_data_endpoints


class BaseApiDataExtractor:
    """
    Base class for API data extractors.
    
    This class provides a common interface for extracting data from an API endpoint.
    
    Attributes:
        endpoint (str): The endpoint for the API.
        data (dict): The JSON data received from the endpoint.
    """
    def __init__(self, endpoint):
        """
        Initializes an instance of the class.

        Args:
            endpoint (str): The endpoint for the API.

        Returns:
            None
        """
        self.endpoint = endpoint
        self.data = None

    def get_data(self):
        """
        Fetches the data from the specified endpoint.

        The data is retrieved using the `requests` library and is returned as a JSON 
        object.

        Returns:
            dict: The JSON data received from the endpoint, if the response status code 
            is 200.

        Raises:
            Exception: If the GET request fails or the response status code is not 200.
        """
        response = requests.get(self.endpoint)
        if response.status_code == 200:
            return response.json()

        raise Exception(
            f"Failed to get data from {self.endpoint}. Status code: {response.status_code}"
        )

    def extract_data(self):
        """
        Executes the extraction of data by fetching it from the specified endpoint.

        This method internally calls the 'get_data' method to retrieve data from the 
        endpoint and assigns it to the 'data' attribute of the object.

        Returns: None
        """
        self.data = self.get_data()

    def make_statandard_data_path(self, folder_name):
        """
        Creates a standard data path for the given folder name.

        The standard data path is located in the staging area directory. If the 
        folder name does not already exist, it is created.

        Args:
            folder_name (str): The name of the folder to create.

        Returns:
            Path: The path to the data folder.
        """
        staging_area_path = get_staging_area_path()
        data_path = staging_area_path.joinpath(folder_name)
        data_path.mkdir(parents=True, exist_ok=True)
        return data_path


class FixedApiDataExtractor(BaseApiDataExtractor):
    def __init__(self, endpoint_name):
        """
        Initializes an instance of the class.

        Args:
            endpoint_name (str): The name of the endpoint.

        Returns:
            None
        """
        super().__init__(fixed_data_endpoints[endpoint_name])
        self.endpoint_name = endpoint_name

    def save_data(self):
        """
        Saves the current data to a JSON file.

        The data is saved to a file named `{endpoint_name}.json` in the `fixed_data` 
        folder.
        
        Returns:
            None
        """
        folder_name = "fixed_data"
        data_path = self.make_statandard_data_path(folder_name)
        filename = f"{self.endpoint_name}.json"
        file_path = data_path.joinpath(filename)

        with open(file_path, "w") as file:
            json.dump(self.data, file)

    def execute(self):
        """
        Executes the data extraction and saving process.

        This method first calls the `extract_data()` method to retrieve data from the 
        specified endpoint. Once the data is retrieved, it calls the `save_data()` 
        method to store the data in a JSON file.

        Returns:
            None
        """
        self.extract_data()
        self.save_data()


class DynamicApiDataExtractor(BaseApiDataExtractor):
    def __init__(self, endpoint_name: str):
        """
        Initializes a new instance of the class.
        
        Args:
            endpoint_name (str): The name of the endpoint.
        
        Returns:
            None
        """
        super().__init__(dynamic_data_endpoints[endpoint_name])
        self.endpoint_name = endpoint_name

    def get_round_number_str(self):
        """
        Retrieves the current round number as a padded string.

        The current round number is retrieved using the `get_current_round()` method. 
        The retrieved round number is then converted to a string and padded with zeros 
        to ensure a consistent format.

        Returns:
            str: The padded string representation of the current round number.
        """
        current_round = get_current_round()
        return str(current_round).zfill(2)

    def save_data(self):
        """
        Saves the current data to a JSON file named `{endpoint_name}.json` in a folder 
        named `rodada{round_number}`, where `{round_number}` is the padded string 
        representation of the current round number.

        The data is saved to the staging area directory. If the folder 
        `rodada{round_number}` does not already exist, it is created.

        Returns:
            None
        """
        round_number = self.get_round_number_str()
        folder_name = f"rodada{round_number}"
        data_path = self.make_statandard_data_path(folder_name)
        filename = f"{self.endpoint_name}.json"
        file_path = data_path.joinpath(filename)

        with open(file_path, "w") as file:
            json.dump(self.data, file)

    def execute(self):
        """
        Executes the data extraction and saving process.

        This method first calls the `extract_data()` method to retrieve data from the 
        specified endpoint. Once the data is retrieved, it calls the `save_data()` 
        method to store the data in a JSON file.

        Returns:
            None
        """
        self.extract_data()
        self.save_data()
