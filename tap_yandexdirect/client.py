"""REST client handling, including YandexDirectStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached
from requests import JSONDecodeError
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_yandexdirect.auth import YandexDirectAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class YandexDirectStream(RESTStream):
    """YandexDirect stream class."""

    url_base = "https://api.direct.yandex.com/json/v5"

    records_jsonpath = "$.result[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.

    @property
    @cached
    def authenticator(self) -> BearerTokenAuthenticator:
        token = self.config.get("access_token")
        return BearerTokenAuthenticator.create_for_stream(self, token=token)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # TODO: Delete this method if not needed.
        return row

    def validate_response(self, response):
        if response.status_code == 400:
            data = response.json()
            raise FatalAPIError(f"Error message found: {data['error']['error_string']} {data['error']['error_detail']}")
        super().validate_response(response)

        if response.status_code in [201, 202]:
            raise RetriableAPIError("The report is being generated in offline mode")
        try:
            data = response.json()
            if data.get("error"):
                    raise FatalAPIError(f"Error message found: {data['error']['error_detail']}")
        except JSONDecodeError:
            if 200 <= response.status_code < 300:
                pass
