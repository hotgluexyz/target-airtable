"""Airtable target stream class, which handles writing streams."""

from typing import Any, Dict, List, Tuple, Union


from singer_sdk.sinks import BatchSink

import requests
import urllib.parse

class AirtableSink(BatchSink):
    """Airtable target sink class."""

    max_size = 10

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # The SDK populates `context["records"]` automatically
        # since we do not override `process_record()`.

        def preprocess_records(x):
            # Wrap every record in a JSON object under the fields
            if not "fields" in x:
                return {
                    "fields": x
                }
            else:
                return x

        records = [preprocess_records(x) for x in context["records"]]
        # Get the records_url (we have a default, but that may change)
        records_url = "https://api.airtable.com/v0"
        # Get the base id
        base_id = self.config.get("base_id")
        # Get the table name (URL encoded)
        table_name = self.config.get("table_name",urllib.parse.quote(self.stream_name))
        token = self.config.get("token")
        endpoint = f"{records_url}/{base_id}/{table_name}"

        # Make the request
        r = requests.post(endpoint, headers={
            "Authorization": f"Bearer {token}"
        },json={
            "records": records,
            "typecast": True # allow typecasting to fit Airtable schema
        })

        self.logger.info(f"Uploaded {len(records)} | success={r.ok}")

        if not r.ok:
            # If request fails, log error message
            self.logger.error(r.text)
            raise Exception(r.text)

        # Clean up records
        context["records"] = []
