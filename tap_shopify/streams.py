"""Stream type classes for tap-shopify."""

from decimal import Decimal
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


class AbandonedCheckouts(tap_shopifyStream):
    """Abandoned checkouts stream."""

    name = "abandoned_checkouts"
    path = "/api/2022-01/checkouts.json"
    records_jsonpath = "$.checkouts[*]"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "abandoned_checkout.json"


class CollectStream(tap_shopifyStream):
    """Collect stream."""

    name = "collects"
    path = "/api/2022-01/collects.json"
    records_jsonpath = "$.collects[*]"
    primary_keys = ["id"]
    replication_key = "id"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "collect.json"

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}

        if next_page_token:
            return dict(parse_qsl(urlsplit(next_page_token).query))

        context_state = self.get_context_state(context)
        last_id = context_state.get("replication_key_value")

        if last_id:
            params["since_id"] = last_id
        return params


class CustomCollections(tap_shopifyStream):
    """Custom collections stream."""

    name = "categories"
    path = "/api/2022-01/custom_collections.json"
    records_jsonpath = "$.custom_collections[*]"
    primary_keys = ["id"]
    schema_filepath = SCHEMAS_DIR / "custom_collection.json"

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        def preprocess_input(data):
            data_convert = []
            for item in data['custom_collections']:
                raw_data = {
                    "id": item['id'],
                    "name": item['title'],
                    "updated_at": item['updated_at'],
                    "source": "shopify"
                }
                data_convert.append(raw_data)
            return data_convert
        processed_data = response.json()
        res = preprocess_input(processed_data)
        yield from extract_jsonpath(self.records_jsonpath, input={
            "custom_collections": res
        })


class CustomersStream(tap_shopifyStream):
    """Customers stream."""

    name = "customers"
    path = "/api/2022-01/customers.json"
    records_jsonpath = "$.customers[*]"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "customer.json"


class LocationsStream(tap_shopifyStream):
    """Locations stream."""

    name = "locations"
    path = "/api/2022-01/locations.json"
    records_jsonpath = "$.locations[*]"
    primary_keys = ["id"]
    replication_key = None
    replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "location.json"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"location_id": record["id"]}


class InventoryLevelsStream(tap_shopifyStream):
    """Inventory levels stream."""

    parent_stream_type = LocationsStream

    name = "inventory_levels"
    path = "/api/2022-01/inventory_levels.json?location_ids={location_id}"
    records_jsonpath = "$.inventory_level[*]"
    primary_keys = ["inventory_item_id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "inventory_level.json"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"inventory_item_id": record["inventory_item_id"]}


class InventoryItemsStream(tap_shopifyStream):
    """Inventory items stream."""

    parent_stream_type = InventoryLevelsStream

    name = "inventory_items"
    path = "/api/2022-01/inventory_items/{inventory_item_id}.json"
    records_jsonpath = "$.inventory_items[*]"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "inventory_item.json"


class MetafieldsStream(tap_shopifyStream):
    """Metafields stream."""

    name = "metafields"
    path = "/api/2022-01/metafields.json"
    records_jsonpath = "$.metafields[*]"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "metafield.json"


class OrdersStream(tap_shopifyStream):
    """Orders stream."""

    name = "orders"
    path = "/api/2022-01/orders.json?status=any"
    records_jsonpath = "$.orders[*]"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "order.json"

    def post_process(self, row: dict, context: Optional[dict] = None):
        """Perform syntactic transformations only."""
        row = super().post_process(row, context)

        if row:
            row["subtotal_price"] = Decimal(row["subtotal_price"])
            row["total_price"] = Decimal(row["total_price"])
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"order_id": record["id"]}


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
            for item in data['products']:
                raw_data = {
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
                    raw_data['options'].append({
                        "product_sku": variant['sku'],
                        "title": variant['title'],
                        "price": variant['price'],
                        "sku": variant['sku'],
                        "created_at": variant['created_at'],
                        "updated_at": variant['updated_at'],
                    })
                for image in item['images']:
                    raw_data['media_gallery_entries'].append({
                        "id": image['id'],
                        "position": image['position'],
                        "file": image['src'],
                        "created_at": image['created_at'],
                        "updated_at": image['updated_at'],
                    })
                data_convert.append(raw_data)
            return data_convert
        processed_data = response.json()
        res = preprocess_input(processed_data)
        yield from extract_jsonpath(self.records_jsonpath, input={
            "products": res
        })


class TransactionsStream(tap_shopifyStream):
    """Transactions stream."""

    parent_stream_type = OrdersStream

    name = "transactions"
    path = "/api/2022-01/orders/{order_id}/transactions.json"
    records_jsonpath = "$.transactions[*]"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "transaction.json"


class UsersStream(tap_shopifyStream):
    """Users stream."""

    name = "users"
    path = "/api/2022-01/users.json"
    records_jsonpath = "$.users[*]"
    primary_keys = ["id"]
    replication_key = None
    replication_method = "FULL_TABLE"
    schema_filepath = SCHEMAS_DIR / "user.json"
