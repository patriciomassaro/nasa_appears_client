import os
import requests
import time
from typing import Any
import json
import geopandas as gpd
import datetime
from concurrent.futures import ThreadPoolExecutor,as_completed

from log_config import initialize_logger

MAX_RETRIES = 3
RETRY_DELAY = 2
APPEEARS_URL = "https://appeears.earthdatacloud.nasa.gov/api/"


class NasaApiConnection:
    """
    Class to connect to NASA APPEEARS API
    """

    def __init__(self,
                 log_file: str,
                 credentials_path: str,
                 url: str = APPEEARS_URL):
        self.logger = initialize_logger(log_file)
        self.url = url
        self.credentials_path = credentials_path
        self.headers = {"Content-Type": "application/json"}
        # open json
        with open(credentials_path) as json_file:
            self.credentials_dict = json.load(json_file)
        # look for a token in the dict
        if "token" in self.credentials_dict:
            self.logger.info("token found in credentials")
            self.token = self.credentials_dict["token"]
        else:
            self.logger.info("token not found in credentials, authenticating")
            self.token = self._authenticate(self.credentials_dict["user"], self.credentials_dict["password"])

    def _authenticate(self, user, password):
        """
        _authenticate to the API and get a token
        :param user:
        :param password:
        :return: token
        """
        self.logger.info("authenticating")
        try:
            response = requests.post(self.url + "login", auth=(user, password), timeout=180)

            token_response = response.json()
            # Extract the token from the json
            token = token_response["token"]
            self.credentials_dict["token"] = token

            # Save the token in the json for persistence
            with open(self.credentials_path, "w") as outfile:
                json.dump(self.credentials_dict, outfile)
            return token
        except Exception as e:
            self.logger.error("Error authenticating: " + str(e))
            self.logger.error(response.content)
            raise Exception("Error authenticating: " + str(e))

    def _make_request_with_auth_retries(self,
                                        request_config: dict[str, Any],
                                        timeout: int = 180):
        """
        Make a request with retries: if the token is expired, we get a new one
        """
        for i in range(MAX_RETRIES):
            # Add the token to the header
            request_config["headers"] = {"Authorization": f"Bearer {self.token}"}
            request_config["timeout"] = timeout
            response = requests.request(**request_config)
            # If the token is expired, get a new token and retry
            if response.status_code == 403:
                # Token is expired
                self.logger.info("token expired, getting a new one")
                self.token = self._authenticate(self.credentials_dict["user"], self.credentials_dict["password"])
                time.sleep(RETRY_DELAY)
            else:
                return response
        raise Exception("Max retries exceeded")

    @staticmethod
    def _convert_shapefile_to_json(shapefile: gpd.GeoDataFrame) -> dict:

        if shapefile.crs != "EPSG:4326":
            shapefile = shapefile.to_crs("EPSG:4326")

        geojson_dict = shapefile.__geo_interface__

        json_data = {
            "geo": {
                "type": "FeatureCollection",
                "features": []
            }
        }

        for feature in geojson_dict["features"]:
            json_data["geo"]["features"].append(feature)
        return json_data

    def build_submit_task_request(self,
                                  shapefile: gpd.GeoDataFrame,
                                  product: str,
                                  layers: list[str],
                                  start_date: str,
                                  end_date: str,
                                  projection: str = "native",
                                  output_format: str = "geotiff"
                                  ) -> dict:
        """
        Build a request to submit a task to the API
        :param shapefile:
        :param product:
        :param layers:
        :param start_date:
        :param end_date:
        :param projection:
        :param output_format:
        :return:
        """

        # assert that end date has the format DD-MM-YYYY
        try:
            start_date_comparison=datetime.datetime.strptime(start_date, "%m-%d-%Y")
            end_date_comparison=datetime.datetime.strptime(end_date, "%m-%d-%Y")
        except ValueError:
            raise ValueError("Incorrect data format, should be MM-DD-YYYY")
        if end_date_comparison <= start_date_comparison:
            raise ValueError("End date must be after start date")

        request_json = {"params": self._convert_shapefile_to_json(shapefile)}

        request_json["params"]["dates"] = [
            {
                "startDate": start_date,
                "endDate": end_date,
                "recurring": False,
            }
        ]

        request_json["params"]["output"] = {
            "format": {
                "type": output_format
            },
            "projection": projection
        }

        request_json["params"]["layers"] = [{"layer": layer, "product": product} for layer in layers]

        return request_json

    def _submit_request(self, request: json):
        request_config = {
            "method": "POST",
            "url": self.url + "task",
            "json": request
        }
        self.logger.info("submitting request")
        response = self._make_request_with_auth_retries(request_config)
        if response.status_code == 202:
            json_response = response.json()
            task_id = json_response["task_id"]
            self.logger.info("request submitted successfully")
            return task_id

    def delete_task(self, task_id):
        request_config = {
            "method": "DELETE",
            "url": self.url + "task/" + task_id,
        }
        response = self._make_request_with_auth_retries(request_config)

        if response.status_code == 204:
            self.logger.info("task deleted successfully")
        else:
            self.logger.error("Error submitting request: " + str(response.content))
            raise Exception("Error submitting request: " + str(response.content))

    def logout(self):
        request_config = {
            "method": "POST",
            "url": self.url + "logout",
        }
        response = self._make_request_with_auth_retries(request_config)
        if response.status_code == 204:
            # Delete the token from the credentials path
            with open(self.credentials_path) as json_file:
                credentials_dict = json.load(json_file)
            del credentials_dict["token"]
            with open(self.credentials_path, "w") as outfile:
                json.dump(credentials_dict, outfile)
            self.logger.info("logging off: ")
        else:
            self.logger.error("Error logging off: " + str(response.content))
            raise Exception("Error logging off: " + str(response.content))

    def list_statuses(self):
        self.logger.info("listing statuses")
        request_config = {
            "method": "GET",
            "url": self.url + "status",
        }
        response = self._make_request_with_auth_retries(request_config)
        status_response = response.json()
        return status_response

    def list_tasks(self):
        request_config = {
            "method": "GET",
            "url": self.url + "task",
        }
        self.logger.info("listing tasks")
        response = self._make_request_with_auth_retries(request_config)
        content = response.json()
        # log the length of the response
        self.logger.info(f"number of tasks: {len(content)}")
        return content

    def get_files_from_task(self, task_id):
        request_config = {
            "method": "GET",
            "url": self.url + "bundle/" + task_id,
        }
        self.logger.info("getting files from task")
        response = self._make_request_with_auth_retries(request_config)
        status_response = response.json()
        return status_response

    def _download_file(self,
                      task_id: str,
                      file_id: str,
                      file_path: str):
        request_config = {
            "method": "GET",
            "url": self.url + "bundle/" + task_id + "/" + file_id,
            "allow_redirects": True,
            "stream": True,
        }
        # self.logger.info(f"downloading file {file_id} from task {task_id}")
        response = self._make_request_with_auth_retries(request_config)
        # check that the folder of the file path exists, otherwise create it
        folder_path = os.path.dirname(file_path)
        if not os.path.exists(folder_path):
            # self.logger.info(f"creating folder {folder_path}")
            os.makedirs(folder_path)

        if response.status_code == 200:
            with open(file_path, "wb") as f:
                for data in response.iter_content(chunk_size=4096):
                    f.write(data)
            # self.logger.info("file downloaded successfully")
        else:
            self.logger.error("Error downloading file: " + str(response.status_code))
        return response.status_code


    def download_files_in_parallel(self,
                                   task_id: str,
                                   file_ids: list[str],
                                   file_paths: list[str]):
        self.logger.info(f"Starting parallel download of {len(file_ids)} files from task {task_id}")
        # repeat the task_id for each file_id
        task_ids = [task_id] * len(file_ids)
        files_to_download = zip(task_ids, file_ids, file_paths)
        status_codes = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self._download_file, *file_info): file_info for file_info in files_to_download}

            for future in as_completed(futures):
                file_info = futures[future]
                try:
                    status_code = future.result()
                    status_codes.append(status_code)
                except Exception as exc:
                    self.logger.error(f"An exception occurred while downloading file {file_info}: {exc}")

            # if all the status codes are 200, log it
            if all(status_code == 200 for status_code in status_codes):
                self.logger.info(f"All files downloaded successfully from task {task_id}")
            else:
                self.logger.error(f"Error downloading files from task {task_id}")
        return status_codes






if __name__ == "__main__":
    # Extract the user and password from the env variables
    credentials_path = "credentials.json"

    # Create an instance of the class
    nasa_api_connection = NasaApiConnection(credentials_path)
