"""Airtable target class."""

from pathlib import Path
from typing import List

from singer_sdk.target_base import Target
from singer_sdk.sinks import Sink
from singer_sdk import typing as th

from target_airtable.sinks import (
    AirtableSink,
)


class TargetAirtable(Target):
    """Sample target for Airtable."""

    name = "target-airtable"
    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("base_id", th.StringType, required=True),
        th.Property("records_url", th.StringType)
    ).to_dict()
    default_sink_class = AirtableSink

if __name__ == '__main__':
    TargetAirtable.cli()
