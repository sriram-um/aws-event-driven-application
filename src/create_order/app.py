"""CreateOrder Lambda: consumes accepted orders from the intake SQS queue (fed directly by
API Gateway, PUT /orders/{orderId}), saves each as one header item plus one item per order line
in a single DynamoDB transaction, then publishes an OrderCreated event to the orders bus.

The API has already validated the body against a JSON schema and returned 202; the checks a
schema can't express (e.g. duplicate itemIds) happen here, and orders that fail them are stored
with status REJECTED so GET /orders/{orderId} can report why.

Idempotency (exercise 8): orderId is the idempotency key. The order header doubles as the
idempotency record - it stores a hash of the request (customer + items), a processing state
(IN_PROGRESS -> COMPLETED), which SQS message claimed it and until when (lock). Conditional
writes decide who processes an order, so duplicates (client retries, SQS redelivery, concurrent
copies) never create a second order or a second OrderCreated, and an order whose event could
not be published is resumed by the retry instead of being lost."""
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from botocore.exceptions import ClientError

logger = Logger()
logger.append_keys(app_version=os.environ.get("APP_VERSION", "local"))  # git SHA set by CI
# Only patch botocore (AWS SDK calls become X-Ray subsegments); patching every supported
# library measured ~14 ms slower at cold start in Project 3.
tracer = Tracer(patch_modules=["botocore"])

TABLE_NAME = os.environ["TABLE_NAME"]
EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]
EVENT_SOURCE = "orders.service"
table = boto3.resource("dynamodb").Table(TABLE_NAME)
# The resource's client takes plain Python values (it serializes them), also in transactions
dynamodb = table.meta.client
events = boto3.client("events")
processor = BatchProcessor(event_type=EventType.SQS)

MAX_ITEMS = 25  # header + 25 lines stays well inside the 100-action transaction limit
# A claim expires after this long. Longer than the function timeout (10 s), so a live
# invocation never loses its claim; shorter than the queue's visibility timeout (60 s), so the
# message's own retry always finds the lock expired and can resume.
LOCK_SECONDS = 30


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


def _payload_hash(customer_id, items):
    """Fingerprint of what was ordered and by whom: the same request always hashes the same
    (item order and number formatting don't matter), any real difference changes it."""
    canonical = {
        "customerId": customer_id,
        "items": sorted(({**i, "unitPrice": format(i["unitPrice"].normalize(), "f")} for i in items),
                        key=lambda i: i["itemId"]),
    }
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()


def _attribute(record: SQSRecord, name):
    attrs = record.message_attributes
    # Index rather than .get(): only __getitem__ wraps the value in SQSMessageAttribute
    if name not in attrs or not attrs[name].string_value:
        raise ValidationError(f"message attribute '{name}' is missing")
    return attrs[name].string_value


def _now():
    return datetime.now(timezone.utc).isoformat()


def _put(item, condition=None):
    action = {"TableName": TABLE_NAME, "Item": item}
    if condition:
        action["ConditionExpression"] = condition
    return {"Put": action}


def _condition_failed(err):
    """True when a conditional write lost (single write or the header in a transaction)."""
    code = err.response["Error"]["Code"]
    if code == "ConditionalCheckFailedException":
        return True
    reasons = err.response.get("CancellationReasons") or []
    return code == "TransactionCanceledException" and bool(reasons) and reasons[0].get("Code") == "ConditionalCheckFailed"


@tracer.capture_method
def _claim(order_id, customer_id, items, total, accepted_at, payload_hash, message_id):
    """Creates the order (header + lines) and claims it for this message - or raises if the
    orderId already exists."""
    header = {
        "orderId": order_id,
        "sk": "ORDER",
        "customerId": customer_id,
        "status": "CREATED",
        "total": total,
        "itemCount": len(items),
        "acceptedAt": accepted_at,
        "createdAt": _now(),
        "payloadHash": payload_hash,
        "processingState": "IN_PROGRESS",
        "claimedBy": message_id,
        "lockExpiresAt": int(time.time()) + LOCK_SECONDS,
    }
    lines = [{"orderId": order_id, "sk": f"ITEM#{item['itemId']}", **item} for item in items]
    # All-or-nothing: the order, its lines and the claim are written together. The condition
    # makes concurrent copies of the same order race safely - exactly one transaction wins.
    dynamodb.transact_write_items(TransactItems=[
        _put(header, condition="attribute_not_exists(orderId)"),
        *(_put(line) for line in lines),
    ])
    return header


def _existing(order_id):
    return table.get_item(Key={"orderId": order_id, "sk": "ORDER"}, ConsistentRead=True)["Item"]


@tracer.capture_method
def _take_over(order_id, payload_hash, message_id):
    """Re-claims an IN_PROGRESS order whose earlier attempt didn't finish: either this same
    message's previous receive, or another message whose lock has expired."""
    try:
        return table.update_item(
            Key={"orderId": order_id, "sk": "ORDER"},
            UpdateExpression="SET claimedBy = :me, lockExpiresAt = :expires",
            ConditionExpression=("processingState = :in_progress AND payloadHash = :hash "
                                 "AND (claimedBy = :me OR lockExpiresAt < :now)"),
            ExpressionAttributeValues={
                ":me": message_id, ":hash": payload_hash, ":in_progress": "IN_PROGRESS",
                ":now": int(time.time()), ":expires": int(time.time()) + LOCK_SECONDS},
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as err:
        if _condition_failed(err):
            return None
        raise


@tracer.capture_method
def _complete(order_id, message_id, event_id):
    try:
        table.update_item(
            Key={"orderId": order_id, "sk": "ORDER"},
            UpdateExpression="SET processingState = :completed, eventId = :event, completedAt = :at "
                             "REMOVE claimedBy, lockExpiresAt",
            ConditionExpression="processingState = :in_progress AND claimedBy = :me",
            ExpressionAttributeValues={":completed": "COMPLETED", ":in_progress": "IN_PROGRESS",
                                       ":me": message_id, ":event": event_id, ":at": _now()},
        )
    except ClientError as err:
        if not _condition_failed(err):
            raise
        # Our claim expired and another message took over after we published: the event may
        # be published twice - consumers are idempotent (FulfilOrder), so that is harmless.
        logger.warning("claim lost before completing; OrderCreated may be duplicated",
                       extra={"orderId": order_id})


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
            "processingState": "COMPLETED",
        }, condition="attribute_not_exists(orderId)")["Put"])
    except ClientError as err:
        if not _condition_failed(err):
            raise
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


def _result(header):
    """The stored outcome of an order - what every duplicate request gets back."""
    return {"orderId": header["orderId"], "status": header["status"],
            "total": str(header.get("total", "")), "eventId": header.get("eventId")}


def record_handler(record: SQSRecord):
    try:
        return _process(record)
    except Exception:
        # Reported to SQS as a batch item failure; after 3 receives SQS moves it to the DLQ
        attrs = record.message_attributes
        logger.exception("order processing failed", extra={
            "orderId": attrs["orderId"].string_value if "orderId" in attrs else None,
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
    message_id = record.message_id
    log = {"orderId": order_id, "messageId": message_id,
           "receiveCount": int(record.attributes.approximate_receive_count)}

    try:
        payload = json.loads(record.body)
        if not isinstance(payload, dict):
            raise ValidationError("order body must be a JSON object")
        items = _parse_items(payload)
    except (json.JSONDecodeError, ValidationError) as err:
        # Permanent failure: retrying can't fix the payload, so record it and move on
        reason = str(err) if isinstance(err, ValidationError) else "order body must be valid JSON"
        logger.warning("order rejected", extra={**log, "reason": reason})
        _save_rejected(order_id, customer_id, reason, accepted_at)
        return None

    total = sum(i["unitPrice"] * i["quantity"] for i in items)
    payload_hash = _payload_hash(customer_id, items)
    tracer.put_annotation(key="orderId", value=order_id)

    try:
        header = _claim(order_id, customer_id, items, total, accepted_at, payload_hash, message_id)
    except ClientError as err:
        if not _condition_failed(err):
            raise  # e.g. throttling - let SQS retry the message
        existing = _existing(order_id)
        if existing.get("payloadHash") != payload_hash:
            # Same key, different request (other items or another customer): never overwrite.
            # Permanent - consumed, not retried.
            logger.warning("ORDER_ID_CONFLICT: orderId already used for a different request", extra={
                **log, "existingCustomerId": existing.get("customerId"), "requestCustomerId": customer_id})
            return None
        # Orders saved before exercise 8 have no processingState - they are complete
        if existing.get("processingState", "COMPLETED") == "COMPLETED":
            logger.info("duplicate order: returning the stored result", extra={**log, **_result(existing)})
            return _result(existing)
        header = _take_over(order_id, payload_hash, message_id)
        if header is None:
            # Another message holds a live claim and will finish (or its own retry will)
            logger.info("duplicate order in progress elsewhere: dropping", extra=log)
            return None
        logger.info("resuming unfinished order", extra=log)

    # Not swallowed any more: if publishing fails, the record fails, SQS retries it, and the
    # retry resumes from IN_PROGRESS (take-over) and publishes - the event can't be lost.
    event_id = _publish_order_created(order_id, customer_id, items, total, header["createdAt"])
    _complete(order_id, message_id, event_id)
    logger.info("order created", extra={**log, "itemCount": len(items), "total": str(total), "eventId": event_id})
    return {**_result(header), "status": "CREATED", "eventId": event_id}


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    return process_partial_response(
        event=event, record_handler=record_handler, processor=processor, context=context)
