"""Stream type classes for tap-shopify."""

from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qsl, urlsplit
import requests

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.typing import JSONTypeHelper

from tap_shopify.client import tap_shopifyStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class IPv4Type(JSONTypeHelper):
    """Class for IPv4 type."""

    @classproperty
    def type_dict(cls) -> dict:
        """Define and return the type information."""
        return {
            "type": ["string"],
            "format": ["ipv4"],
        }


class CustomCollections(tap_shopifyStream):
    """Custom collections stream."""

    name = "custom_collections"
    path = "/api/2022-01/custom_collections.json"
    records_jsonpath = "$.custom_collections[*]"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "custom_collection.json"


class ProductsStream(tap_shopifyStream):
    """Products stream."""

    name = "products"
    path = "/api/2022-01/products.json"
    records_jsonpath = "$.products[*]"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "product.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # def preprocess_input(data):
        #     data_convert = []
        #     for item in data['products']:
        #         data_convert.append({
        #             'id': item['id'],
        #             'skus': item['vendor']
        #         })
        #     return data_convert
        # processed_data = response.json()
        # res = preprocess_input(processed_data)
        yield from extract_jsonpath(self.records_jsonpath, input={
            "products": [
                {
                    "id": 7300081942617,
                    "sku": "Acme",
                    "name": "example-pants",

                },
                {
                    "id": 7300081909849,
                    "sku": "Acme",
                    "name": "example-t-shirt",
                }
            ]
        })


class UsersStream(tap_shopifyStream):
    """Users stream."""

    name = "users"
    path = "/api/2022-01/users.json"
    records_jsonpath = "$.users[*]"
    primary_keys = ["id"]
    replication_key = None
    replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "user.json"
