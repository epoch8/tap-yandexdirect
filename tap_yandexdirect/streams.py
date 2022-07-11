"""Stream type classes for tap-yandexdirect."""
import io
import json
import requests
import pandas as pd

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from datetime import datetime
from requests import head
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_yandexdirect.client import YandexDirectStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class CampaignsStream(YandexDirectStream):
    """Define custom stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["Id"]
    replication_key = None
    records_jsonpath = "$.result.Campaigns[*]"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("Name", th.StringType),
        th.Property("Id", th.IntegerType, description="The user's system ID"),
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        data = {
            "method": "get",
            "params": {"SelectionCriteria": {}, "FieldNames": ["Id", "Name", "Status"]},
        }
        return data

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "campaign_id": record["Id"],
        }


class AdGroupsStream(YandexDirectStream):
    """Define custom stream."""

    name = "adgroups"
    path = "/adgroups"
    primary_keys = ["Id"]
    replication_key = None
    rest_method = "POST"
    records_jsonpath = "$.result.AdGroups[*]"
    parent_stream_type = CampaignsStream
    schema = th.PropertiesList(
        th.Property("Name", th.StringType),
        th.Property("Id", th.IntegerType, description="The user's system ID"),
        th.Property("CampaignId", th.IntegerType),
        th.Property("Status", th.StringType),
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"CampaignIds": [context["campaign_id"]]},
                "FieldNames": [
                    "Id",
                    "Name",
                    "CampaignId",
                    "Status",
                    "RegionIds",
                ],
            },
        }
        return data

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "adgroup_id": record["Id"],
        }


class AdsStream(YandexDirectStream):
    """Define custom stream."""

    name = "ads"
    path = "/ads"
    primary_keys = ["Id"]
    replication_key = None
    rest_method = "POST"
    records_jsonpath = "$.result.Ads[*]"
    parent_stream_type = AdGroupsStream
    schema = th.PropertiesList(
        th.Property("Id", th.IntegerType, description="The user's system ID"),
        th.Property("AdGroupId", th.IntegerType),
        th.Property("Type", th.StringType),
        th.Property("TextAd", th.StringType),
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        data = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"AdGroupIds": [context["adgroup_id"]]},
                "FieldNames": [
                    "Id",
                    "AdGroupId",
                    "Type",
                ],
            },
        }
        return data


class AdsPerfomanceStream(YandexDirectStream):
    """Define custom stream."""

    name = "ads_perfomance"
    path = "/reports"
    replication_key = "Date"
    rest_method = "POST"
    schema = th.PropertiesList(
        th.Property("Date", th.DateType),
        th.Property("AdId", th.StringType),
        th.Property("CampaignId", th.IntegerType),
        th.Property("Impressions", th.IntegerType),
        th.Property("Clicks", th.IntegerType),
        th.Property("Cost", th.IntegerType),
    ).to_dict()

    @property
    def http_headers(self) -> dict:
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["skipReportHeader"] = "true"
        headers["skipReportSummary"] = "true"
        headers["processingMode"] = "auto"
        return headers

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        data = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": self.config.get("start_date"),
                    "DateTo": self.config.get("end_date"),
                },
                "FieldNames": [
                    "Date",
                    "AdId",
                    "CampaignId",
                    "Impressions",
                    "Clicks",
                    "Cost",
                ],
                "OrderBy": [{"Field": "Date"}],
                "ReportName": f"AdPerfomance-{datetime.now()}",
                "ReportType": "AD_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "YES"
            }
        }
        return data

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        df = pd.read_csv(io.StringIO(response.text), sep='\t')
        data_str = df.to_json(None, orient='records')
        data = json.loads(data_str)
        yield from data

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        return row
