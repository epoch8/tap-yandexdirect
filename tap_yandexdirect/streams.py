"""Stream type classes for tap-yandexdirect."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_yandexdirect.client import YandexDirectStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class CampaignsStream(YandexDirectStream):
    """Define custom stream."""
    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.result.Campaigns[*]"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("Name", th.StringType),
        th.Property(
            "Dd",
            th.StringType,
            description="The user's system ID"
        )
    ).to_dict()

