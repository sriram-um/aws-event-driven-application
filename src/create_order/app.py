"""CreateOrder Lambda: consumes accepted orders from the intake SQS queue (fed directly by
API Gateway, PUT /orders/{orderId}), saves each as one header item plus one item per order line
in a single DynamoDB transaction, then publishes an OrderCreated event to the orders bus.

The API has already validated the body against a JSON schema and returned 202; the checks a
schema can't express (e.g. duplicate itemIds) happen here, and orders that fail them are stored
with status REJECTED so GET /orders/{orderId} can report why."""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from boto3.dynamodb.types import TypeSerializer

logger = Logger()
logger.append_keys(app_version=os.environ.get("APP_VERSION", "local"))  # git SHA set by CI
# Only patch botocore (AWS SDK calls become X-Ray subsegments); patching every supported
# library measured ~14 ms slower at cold start in Project 3.
tracer = Tracer(patch_modules=["botocore"])

TABLE_NAME = os.environ["TABLE_NAME"]
EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]
EVENT_SOURCE = "orders.service"
dynamodb = boto3.client("dynamodb")
events = boto3.client("events")
serializer = TypeSerializer()
processor = BatchProcessor(event_type=EventType.SQS)

MAX_ITEMS = 25  # header + 25 lines stays well inside the 100-action transaction limit


class ValidationError(Exception):
    pass


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


def _attribute(record: SQSRecord, name):
    attrs = record.message_attributes
    # Index rather than .get(): only __getitem__ wraps the value in SQSMessageAttribute
    if name not in attrs or not attrs[name].string_value:
        raise ValidationError(f"message attribute '{name}' is missing")
    return attrs[name].string_value


def _now():
    return datetime.now(timezone.utc).isoformat()


def _put(item, condition=None):
    action = {"TableName": TABLE_NAME, "Item": {k: serializer.serialize(v) for k, v in item.items()}}
    if condition:
        action["ConditionExpression"] = condition
    return {"Put": action}


def _is_duplicate(err):
    """True when the header's attribute_not_exists condition failed (orderId already used)."""
    reasons = err.response.get("CancellationReasons") or []
    return bool(reasons) and reasons[0].get("Code") == "ConditionalCheckFailed"


@tracer.capture_method
def _save_order(order_id, customer_id, items, total, accepted_at):
    created_at = _now()
    header = {
        "orderId": order_id,
        "sk": "ORDER",
        "customerId": customer_id,
        "status": "CREATED",
        "total": total,
        "itemCount": len(items),
        "acceptedAt": accepted_at,
        "createdAt": created_at,
    }
    lines = [{"orderId": order_id, "sk": f"ITEM#{item['itemId']}", **item} for item in items]
    # All-or-nothing: an order never exists without its lines (or vice versa)
    dynamodb.transact_write_items(TransactItems=[
        _put(header, condition="attribute_not_exists(orderId)"),
        *(_put(line) for line in lines),
    ])
    return created_at


@tracer.capture_method
def _save_rejected(order_id, customer_id, reason, accepted_at):
    try:
        dynamodb.put_item(**_put({
            "orderId": order_id,
            "sk": "ORDER",
            "customerId": customer_id,
            "status": "REJECTED",
            "reason": reason,
            "acceptedAt": accepted_at,
            "createdAt": _now(),
        }, condition="attribute_not_exists(orderId)")["Put"])
    except dynamodb.exceptions.ConditionalCheckFailedException:
        logger.warning("rejected order id already exists, not overwriting", extra={"orderId": order_id})


@tracer.capture_method
def _publish_order_created(order_id, customer_id, items, total, created_at):
    detail = {
        "orderId": order_id,
        "customerId": customer_id,
        "items": [{**item, "unitPrice": str(item["unitPrice"])} for item in items],
        "itemCount": len(items),
        "total": str(total),
        "createdAt": created_at,
    }
    resp = events.put_events(Entries=[{
        "EventBusName": EVENT_BUS_NAME,
        "Source": EVENT_SOURCE,
        "DetailType": "OrderCreated",
        "Detail": json.dumps(detail),
    }])
    # PutEvents reports per-entry failures in the response instead of raising
    if resp.get("FailedEntryCount"):
        entry = resp["Entries"][0]
        raise RuntimeError(f"{entry.get('ErrorCode')}: {entry.get('ErrorMessage')}")
    return resp["Entries"][0]["EventId"]


def record_handler(record: SQSRecord):
    try:
        _process(record)
    except Exception:
        # Reported to SQS as a batch item failure; after 3 receives SQS moves it to the DLQ
        logger.exception("order processing failed", extra={
            "messageId": record.message_id,
            "receiveCount": int(record.attributes.approximate_receive_count)})
        raise


def _process(record: SQSRecord):
    # Set by the API Gateway mapping template: orderId (validated UUID from the path),
    # customerId (verified Cognito sub), acceptedAt (request time, epoch ms), requestId
    order_id = _attribute(record, "orderId")
    customer_id = _attribute(record, "customerId")
    accepted_at = datetime.fromtimestamp(int(_attribute(record, "acceptedAt")) / 1000, timezone.utc).isoformat()
    logger.set_correlation_id(_attribute(record, "requestId"))  # API Gateway request id

    try:
        payload = json.loads(record.body)
        if not isinstance(payload, dict):
            raise ValidationError("order body must be a JSON object")
        items = _parse_items(payload)
    except (json.JSONDecodeError, ValidationError) as err:
        # Permanent failure: retrying can't fix the payload, so record it and move on
        reason = str(err) if isinstance(err, ValidationError) else "order body must be valid JSON"
        logger.warning("order rejected", extra={"orderId": order_id, "reason": reason})
        _save_rejected(order_id, customer_id, reason, accepted_at)
        return

    total = sum(i["unitPrice"] * i["quantity"] for i in items)
    tracer.put_annotation(key="orderId", value=order_id)
    try:
        created_at = _save_order(order_id, customer_id, items, total, accepted_at)
    except dynamodb.exceptions.TransactionCanceledException as err:
        if not _is_duplicate(err):
            raise  # e.g. throttling - let SQS retry the message
        # Same orderId seen before (client retry or SQS redelivery). Exercise 8 turns this into
        # full idempotency (same result, payload check, in-progress state).
        logger.info("order already exists, skipping", extra={"orderId": order_id})
        return

    # The order is saved; a publish failure is logged rather than retried, because a retry
    # would now be skipped as a duplicate (the dual-write gap that exercise 8 closes).
    try:
        event_id = _publish_order_created(order_id, customer_id, items, total, created_at)
        logger.info("order created", extra={
            "orderId": order_id, "itemCount": len(items), "total": str(total), "eventId": event_id,
            "receiveCount": int(record.attributes.approximate_receive_count)})
    except Exception:
        logger.exception("OrderCreated publish failed", extra={"orderId": order_id})


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    return process_partial_response(
        event=event, record_handler=record_handler, processor=processor, context=context)
