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
        def preprocess_input(data):
            data_convert = []
            for item in processed_data['products']:
                data = {
                    "id": item['id'],
                    "name": item['title'],
                    "sku": item['handle'],
                    "created_at": item['created_at'],
                    "updated_at": item['updated_at'],
                    "options": [],
                    "media_gallery_entries": [],
                    "source": "shopify"
                }
                for variant in item['variants']:
                    data['options'].append({
                        "product_sku": variant['sku'],
                        "title": variant['title'],
                        "price": variant['price'],
                        "sku": variant['sku'],
                        "created_at": variant['created_at'],
                        "updated_at": variant['updated_at'],
                    })
                for image in item['images']:
                    data['media_gallery_entries'].append({
                        "id": image['id'],
                        "position": image['position'],
                        "file": image['src'],
                        "created_at": image['created_at'],
                        "updated_at": image['updated_at'],
                    })
                data_convert.append(data)
            return data_convert
        processed_data = response.json()
        res = preprocess_input(processed_data)
        yield from extract_jsonpath(self.records_jsonpath, input={
            "products": res
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
