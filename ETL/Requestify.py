import http.client
import json
from logging_config import setup_logging
import logging


# Logging configuration
setup_logging()
logger = logging.getLogger()  # start logging


class Requestify:
    def __init__(
        self,
        url: str = "api.openaq.org",
        headers: dict = {},
        payload: str = "",
        verbose: bool = False,
    ):
        """
        Creates a new request to OpenAQ through a wrapper

        Parameters:
        ----
            url (str, optional): The url to connect to. Defaults to "api.openaq.org".
            headers (dict, optional): The headers to send in the request. Defaults to {}.
            payload (str, optional): The payload to send in the request. Defaults to "".

        Returns:
        ----
            None
        """
        self.url = url
        self.payload = payload
        self.verbose = verbose

        # Get api key from vars -- REQUIRED
        try:
            with open(".vars/openaq-key.txt", "r") as f:
                self.api_key = f.read()
        except Exception as e:
            logger.error(f"{e}")
            logger.error("Please put your API key in .vars/openaq-key.txt")
            exit()  # cannot continue if the file is missing

        if headers == {}:
            self.headers = {"X-API-Key": self.api_key}
        else:
            self.headers = headers

        self.error_codes: dict[int, str] = {
            # 200: "OK", # Not needed, default is 200
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            408: "Request Timeout",
            410: "Gone/Removed",
            422: "Unprocessable Entity",
            500: "Internal Server Error",
            503: "Service Unavailable",
        }

        try:
            self.conn = http.client.HTTPSConnection(self.url)
            logger.info("OpenAQ Connection established")
        except Exception as e:
            logger.error(f"{e}")
            exit()

    def get(self, target_url: str = ""):
        self.conn = http.client.HTTPSConnection(self.url)
        self.conn.request("GET", target_url, self.payload, headers=self.headers)
        try:
            res = self.conn.getresponse()
            data = res.read()
            data_json = json.loads(data)
            logger.info("Request processed")
        except Exception as e:
            logger.error(f"{e}")
            exit()

        if res.status != 200:
            error_rx = self.error_codes[res.status]
            # First, error in the location
            try:
                error_msg = data_json["detail"]
            except Exception as e:
                logger.error(f"{e}")
            # Second, error in number of requests
            try:
                error_msg = data_json["detail"][0]["msg"]
            except Exception as e:
                logger.error(f"{e}")

            logger.error(f"{res.status} ({error_rx}) - {error_msg}")
            exit()

        # return data_json  # this is ok, but to simplify code later-->
        return data_json, data_json["results"]

    def close(self):
        try:
            self.conn.close()
            logger.info("OpenAQ Connection closed")
        except Exception as e:
            logger.error(f"{e}")

    def __del__(self):
        self.close()
