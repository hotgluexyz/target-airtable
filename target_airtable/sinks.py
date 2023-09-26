"""Airtable target stream class, which handles writing streams."""

from typing import Any, Dict, List, Tuple, Union



import json
import uuid
import backoff
import requests
import urllib.parse

from airtable.client import Client
from singer_sdk.sinks import BatchSink

class AirtableSink(BatchSink):
    """Airtable target sink class."""

    max_size = 10
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = Client(
            self.config["client_id"],
            self.config["client_secret"],
            self.config["redirect_uri"],
            uuid.uuid4().__str__().replace("-", "")*2
        )

    def _gen_new_token_url(self):
        """
        Internal Helper function to help users generate
        an OAuth authorization URL on Airtable
        """
        url = self.client.authorization_url(uuid.uuid4().__str__().replace("-", ""))
        url += "+schema.bases%3Awrite"
        print(url)
    
    def gen_new_token(self, code):
        """
        Helper function to generate access and refresh tokens
        from the OAuth authorization flow on Airtable
        """
        response = self.client.token_creation(code)
        self.token = response["access_token"]
        self.refresh_token = response["refresh_token"]
        self._config["access_token"] = response["access_token"]
        self._config["refresh_token"] = response["refresh_token"]
        with open('./config.json', 'w') as f:
            json.dump(self._config, f, indent=4)


    def _refresh_token(self):
        """Refresh OAuth token."""
        self.client.set_token({
            "access_token": self.config["access_token"],
            "refresh_token": self.config["refresh_token"],
        })
        response = self.client.refresh_token(self.config["refresh_token"])
        self._config["access_token"] = response["access_token"]
        with open('./config.json', "w") as outfile:
            json.dump(self._config, outfile, indent=4)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code == 401:
            self._refresh_token()
        
        if response.status_code == 429:
            raise Exception(f"Too Many Requests for path: {response.request.url}")
        
        if response.status_code == 404:
            pass
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {response.request.url}"
                f" with text:{response.text} "
            )
            raise Exception(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {response.request.url}"
                f" with text:{response.text} "
            )
            raise Exception(msg)

        return response
    
    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def _request(self, method, url, params=None, headers={}, data={}, *args, **kwargs):
        new_headers = {'Authorization': 'Bearer {}'.format(self.config['access_token'])}
        headers.update(new_headers)
        response = requests.request(method, url, params=params, headers=headers, data=data, *args, **kwargs)
        return self.validate_response(response)

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
        table_name = self.config.get("table_name", urllib.parse.quote(self.stream_name))
        token = self.config.get("access_token")
        endpoint = f"{records_url}/{base_id}/{table_name}"


        records_to_update = [record for record in records if record["fields"]["id"] is not None]
        records_to_create = [record for record in records if record["fields"]["id"] in [None, ""]]

        # Make the request to patch the records
        clean_records_to_update = []
        for record in records_to_update:
            record_id = record["fields"].pop("id")
            record["id"] = record_id
            clean_records_to_update.append(record)
        
        clean_new_records = []
        for record in records_to_create:
            record["fields"].pop("id")
            clean_new_records.append(record)
        
        self._request(
            "PATCH",
            endpoint,
            json={
                "records": clean_records_to_update,
                "typecast": True
            },
        )


        self._request(
            "POST",
            endpoint,
            json={
                "records": clean_new_records,
                "typecast": True
            },
        )
        # Clean up records
        context["records"] = []
