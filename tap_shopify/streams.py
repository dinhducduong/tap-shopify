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

    name = "custom_collections"
    path = "/api/2022-01/custom_collections.json"
    records_jsonpath = "$.custom_collections[*]"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "custom_collection.json"


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
                    "body_html": "",
                    "sku": "Acme",
                    "product_type": "Pants",
                    "created_at": "2023-06-09T10:19:56+07:00",
                    "handle": "example-pants",
                    "updated_at": "2023-06-09T10:19:57+07:00",
                    "published_at": None,
                    "template_suffix": None,
                    "status": "draft",
                    "published_scope": "web",
                    "tags": "mens pants example",
                    "admin_graphql_api_id": "gid://shopify/Product/7300081942617",
                    "variants": [
                        {
                            "id": 40502939484249,
                            "product_id": 7300081942617,
                            "title": "Jeans, W32H34",
                            "price": "50",
                            "sku": None,
                            "position": 1,
                            "inventory_policy": "deny",
                            "compare_at_price": "58",
                            "fulfillment_service": "manual",
                            "inventory_management": None,
                            "option1": "Jeans, W32H34",
                            "option2": None,
                            "option3": None,
                            "created_at": "2023-06-09T10:19:56+07:00",
                            "updated_at": "2023-06-09T10:19:56+07:00",
                            "taxable": True,
                            "barcode": None,
                            "grams": 1250,
                            "image_id": None,
                            "weight": 1250.0,
                            "weight_unit": "g",
                            "inventory_item_id": 42597128831065,
                            "inventory_quantity": 0,
                            "old_inventory_quantity": 0,
                            "requires_shipping": True,
                            "admin_graphql_api_id": "gid://shopify/ProductVariant/40502939484249"
                        }
                    ],
                    "options": [
                        {
                            "id": 9333169782873,
                            "product_id": 7300081942617,
                            "name": "Title",
                            "position": 1,
                            "values": [
                                "Jeans, W32H34"
                            ]
                        }
                    ],
                    "images": [
                        {
                            "id": 31643643871321,
                            "product_id": 7300081942617,
                            "position": 1,
                            "created_at": "2023-06-09T10:19:56+07:00",
                            "updated_at": "2023-06-09T10:19:56+07:00",
                            "alt": None,
                            "width": 5000,
                            "height": 3333,
                            "src": "https://cdn.shopify.com/s/files/1/0575/8943/2409/products/distressed-kids-jeans.jpg?v=1686280796",
                            "variant_ids": [],
                            "admin_graphql_api_id": "gid://shopify/ProductImage/31643643871321"
                        }
                    ],
                    "image": {
                        "id": 31643643871321,
                        "product_id": 7300081942617,
                        "position": 1,
                        "created_at": "2023-06-09T10:19:56+07:00",
                        "updated_at": "2023-06-09T10:19:56+07:00",
                        "alt": None,
                        "width": 5000,
                        "height": 3333,
                        "src": "https://cdn.shopify.com/s/files/1/0575/8943/2409/products/distressed-kids-jeans.jpg?v=1686280796",
                        "variant_ids": [],
                        "admin_graphql_api_id": "gid://shopify/ProductImage/31643643871321"
                    }
                },
                {
                    "id": 7300081909849,
                    "title": "Example T-Shirt",
                    "body_html": "",
                    "sku": "Acme",
                    "product_type": "Shirts",
                    "created_at": "2023-06-09T10:19:54+07:00",
                    "handle": "example-t-shirt",
                    "updated_at": "2023-06-09T10:19:55+07:00",
                    "published_at": "2023-06-09T10:19:52+07:00",
                    "template_suffix": None,
                    "status": "active",
                    "published_scope": "web",
                    "tags": "mens t-shirt example",
                    "admin_graphql_api_id": "gid://shopify/Product/7300081909849",
                    "variants": [
                        {
                            "id": 40502939385945,
                            "product_id": 7300081909849,
                            "title": "Lithograph - Height: 9\" x Width: 12\"",
                            "price": "25",
                            "sku": None,
                            "position": 1,
                            "inventory_policy": "deny",
                            "compare_at_price": None,
                            "fulfillment_service": "manual",
                            "inventory_management": None,
                            "option1": "Lithograph - Height: 9\" x Width: 12\"",
                            "option2": None,
                            "option3": None,
                            "created_at": "2023-06-09T10:19:54+07:00",
                            "updated_at": "2023-06-09T10:19:54+07:00",
                            "taxable": True,
                            "barcode": None,
                            "grams": 3629,
                            "image_id": None,
                            "weight": 3629.0,
                            "weight_unit": "g",
                            "inventory_item_id": 42597128732761,
                            "inventory_quantity": 0,
                            "old_inventory_quantity": 0,
                            "requires_shipping": True,
                            "admin_graphql_api_id": "gid://shopify/ProductVariant/40502939385945"
                        },
                        {
                            "id": 40502939418713,
                            "product_id": 7300081909849,
                            "title": "Small",
                            "price": "20",
                            "sku": "example-shirt-s",
                            "position": 2,
                            "inventory_policy": "deny",
                            "compare_at_price": "25",
                            "fulfillment_service": "manual",
                            "inventory_management": None,
                            "option1": "Small",
                            "option2": None,
                            "option3": None,
                            "created_at": "2023-06-09T10:19:54+07:00",
                            "updated_at": "2023-06-09T10:19:54+07:00",
                            "taxable": True,
                            "barcode": None,
                            "grams": 200,
                            "image_id": None,
                            "weight": 200.0,
                            "weight_unit": "g",
                            "inventory_item_id": 42597128765529,
                            "inventory_quantity": 0,
                            "old_inventory_quantity": 0,
                            "requires_shipping": True,
                            "admin_graphql_api_id": "gid://shopify/ProductVariant/40502939418713"
                        },
                        {
                            "id": 40502939451481,
                            "product_id": 7300081909849,
                            "title": "Medium",
                            "price": "20",
                            "sku": "example-shirt-m",
                            "position": 3,
                            "inventory_policy": "deny",
                            "compare_at_price": "25",
                            "fulfillment_service": "manual",
                            "inventory_management": "shopify",
                            "option1": "Medium",
                            "option2": None,
                            "option3": None,
                            "created_at": "2023-06-09T10:19:54+07:00",
                            "updated_at": "2023-06-09T10:19:54+07:00",
                            "taxable": True,
                            "barcode": None,
                            "grams": 200,
                            "image_id": None,
                            "weight": 200.0,
                            "weight_unit": "g",
                            "inventory_item_id": 42597128798297,
                            "inventory_quantity": 0,
                            "old_inventory_quantity": 0,
                            "requires_shipping": True,
                            "admin_graphql_api_id": "gid://shopify/ProductVariant/40502939451481"
                        }
                    ],
                    "options": [
                        {
                            "id": 9333169750105,
                            "product_id": 7300081909849,
                            "name": "Title",
                            "position": 1,
                            "values": [
                                "Lithograph - Height: 9\" x Width: 12\"",
                                "Small",
                                "Medium"
                            ]
                        }
                    ],
                    "images": [
                        {
                            "id": 31643643838553,
                            "product_id": 7300081909849,
                            "position": 1,
                            "created_at": "2023-06-09T10:19:54+07:00",
                            "updated_at": "2023-06-09T10:19:54+07:00",
                            "alt": None,
                            "width": 5000,
                            "height": 3335,
                            "src": "https://cdn.shopify.com/s/files/1/0575/8943/2409/products/green-t-shirt.jpg?v=1686280794",
                            "variant_ids": [],
                            "admin_graphql_api_id": "gid://shopify/ProductImage/31643643838553"
                        }
                    ],
                    "image": {
                        "id": 31643643838553,
                        "product_id": 7300081909849,
                        "position": 1,
                        "created_at": "2023-06-09T10:19:54+07:00",
                        "updated_at": "2023-06-09T10:19:54+07:00",
                        "alt": None,
                        "width": 5000,
                        "height": 3335,
                        "src": "https://cdn.shopify.com/s/files/1/0575/8943/2409/products/green-t-shirt.jpg?v=1686280794",
                        "variant_ids": [],
                        "admin_graphql_api_id": "gid://shopify/ProductImage/31643643838553"
                    }
                }
            ]
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
