"""CreateOrder Lambda: validates an order payload and saves it as one header item plus one
item per order line, in a single DynamoDB transaction."""
import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.logging import correlation_paths
from boto3.dynamodb.types import TypeSerializer

logger = Logger()
logger.append_keys(app_version=os.environ.get("APP_VERSION", "local"))  # git SHA set by CI
# Only patch botocore (AWS SDK calls become X-Ray subsegments); patching every supported
# library measured ~14 ms slower at cold start in Project 3.
tracer = Tracer(patch_modules=["botocore"])

TABLE_NAME = os.environ["TABLE_NAME"]
dynamodb = boto3.client("dynamodb")
serializer = TypeSerializer()

MAX_ITEMS = 25  # header + 25 lines stays well inside the 100-action transaction limit


class ValidationError(Exception):
    pass


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _parse_items(payload):
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValidationError("'items' must be a non-empty list")
    if len(items) > MAX_ITEMS:
        raise ValidationError(f"an order can contain at most {MAX_ITEMS} items")

    parsed, seen = [], set()
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValidationError(f"items[{i}] must be an object")
        item_id = item.get("itemId")
        if not isinstance(item_id, str) or not item_id.strip():
            raise ValidationError(f"items[{i}].itemId is required")
        if item_id in seen:
            raise ValidationError(f"duplicate itemId '{item_id}'")
        seen.add(item_id)

        quantity = item.get("quantity")
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity < 1:
            raise ValidationError(f"items[{i}].quantity must be a positive integer")
        try:
            unit_price = Decimal(str(item.get("unitPrice")))
        except InvalidOperation:
            raise ValidationError(f"items[{i}].unitPrice must be a number")
        if not unit_price.is_finite() or unit_price < 0:
            raise ValidationError(f"items[{i}].unitPrice must be a non-negative number")

        parsed.append({
            "itemId": item_id,
            "productName": str(item.get("productName", "")),
            "quantity": quantity,
            "unitPrice": unit_price,
        })
    return parsed


def _customer_id(event):
    # Identity comes from the verified Cognito token, never from the request body
    claims = (event.get("requestContext") or {}).get("authorizer", {}).get("claims") or {}
    return claims.get("sub", "anonymous")


def _put(item, condition=None):
    action = {"TableName": TABLE_NAME, "Item": {k: serializer.serialize(v) for k, v in item.items()}}
    if condition:
        action["ConditionExpression"] = condition
    return {"Put": action}


@tracer.capture_method
def _save_order(order_id, customer_id, items, total):
    created_at = datetime.now(timezone.utc).isoformat()
    header = {
        "orderId": order_id,
        "sk": "ORDER",
        "customerId": customer_id,
        "status": "CREATED",
        "total": total,
        "itemCount": len(items),
        "createdAt": created_at,
    }
    lines = [{"orderId": order_id, "sk": f"ITEM#{item['itemId']}", **item} for item in items]
    # All-or-nothing: an order never exists without its lines (or vice versa)
    dynamodb.transact_write_items(TransactItems=[
        _put(header, condition="attribute_not_exists(orderId)"),
        *(_put(line) for line in lines),
    ])
    return created_at


@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    try:
        payload = json.loads(event.get("body") or "{}")
        if not isinstance(payload, dict):
            raise ValidationError("request body must be a JSON object")
        items = _parse_items(payload)
    except json.JSONDecodeError:
        logger.warning("rejected order: invalid JSON")
        return _response(400, {"message": "request body must be valid JSON"})
    except ValidationError as err:
        logger.warning("rejected order", extra={"reason": str(err)})
        return _response(400, {"message": str(err)})

    order_id = str(uuid.uuid4())
    customer_id = _customer_id(event)
    tracer.put_annotation(key="orderId", value=order_id)

    total = sum(i["unitPrice"] * i["quantity"] for i in items)
    _save_order(order_id, customer_id, items, total)

    logger.info("order created", extra={"orderId": order_id, "itemCount": len(items), "total": str(total)})
    return _response(201, {"orderId": order_id, "itemCount": len(items), "total": str(total)})
