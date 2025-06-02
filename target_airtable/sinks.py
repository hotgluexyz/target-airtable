"""Airtable target stream class, which handles writing streams."""

from typing import Any, Dict, List, Tuple, Union



import json
import uuid
import backoff
import requests
import urllib.parse

from airtable.client import Client
from singer_sdk.sinks import BatchSink
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
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

    def _chunk(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]


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
        if response.status_code == 422:
            if "DUPLICATE_OR_EMPTY_FIELD_NAME" in response.text:
                return
            else:
                raise FatalAPIError(f"Airtable API Error: {response.text}")

        if response.status_code == 401:
            self._refresh_token()
        
        if response.status_code == 429:
            raise RetriableAPIError(f"Too Many Requests for path: {response.request.url}")
        
        if response.status_code == 404:
            pass
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {response.request.url}"
                f" with text:{response.text} "
            )
            raise RetriableAPIError(msg)

        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {response.request.url}"
                f" with text:{response.text} "
            )
            raise FatalAPIError(msg)

        return response
    
    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException, RetriableAPIError), max_tries=5, base=5, jitter=None)
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

        endpoint = f"{records_url}/{base_id}/{table_name}"

        # Make sure all fields exist
        fields = []
        for field in self.schema['properties']:
            type = "singleLineText"
            options = None

            match self.schema['properties'][field]['type'][0]:
                case "boolean":
                    type = "checkbox"
                    options = { "icon": "check", "color": "greenBright" }
                case "integer":
                    type = "number"
                    options = {
                        "precision": 0
                    }

            # if self.schema['properties'][field].get('format') == "date-time":
            #     type = "date"
            #     options = {
            #         "dateFormat": {
            #         "name": "iso"
            #         },
            #         "timeFormat": "24hour",
            #         "timeZone": "utc",
            #         "showTime": True
            #     }

            payload = {
                "name": field,
                "type": type
            }
            if options is not None:
                payload['options'] = options

            fields.append(payload)

        tables_res = self._request(
            "GET",
            f"{records_url}/meta/bases/{base_id}/tables",
        )

        tables = tables_res.json()['tables']
        matching_table = [table['id'] for table in tables if table['name'] == table_name]

        if not matching_table:
            # https://api.airtable.com/v0/meta/bases/{baseId}/tables

            # Create the table
            self._request(
                "POST",
                f"{records_url}/meta/bases/{base_id}/tables",
                json={
                    "name": table_name,
                    "fields": fields
                }
            )
        else:
            table_id = matching_table[0]
            for field in fields:
                self._request(
                    "POST",
                    f"{records_url}/meta/bases/{base_id}/tables/{table_id}/fields",
                    json=field,
                )

        records_to_update = [record for record in records if record["fields"]["id"] is not None]
        records_to_create = [record for record in records if record["fields"]["id"] in [None, ""]]

        # Make the request to patch the records
        clean_records_to_update = []
        for record in records_to_update:
            clean_records_to_update.append(record)
        
        clean_new_records = []

        for record in records_to_create:
            record["fields"].pop("id")
            clean_new_records.append(record)
        

        for record_update_chunk in self._chunk(clean_records_to_update, self.max_size):
            self.logger.info(f"Posting records")
            self._request(
                "PUT",
                endpoint,
                json={
                    "performUpsert": {"fieldsToMergeOn": ["id"]},
                    "records": json.loads(json.dumps(record_update_chunk, default=str)),
                    "typecast": True
                },
            )

        for record_create_chunk in self._chunk(clean_new_records, self.max_size):
            self.logger.info(f"Posting records")
            self._request(
                "POST",
                endpoint,
                json={
                    "records": json.loads(json.dumps(record_create_chunk, default=str)),
                    "typecast": True
                },
            )
        # Clean up records
        context["records"] = []
