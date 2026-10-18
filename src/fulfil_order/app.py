"""FulfilOrder Lambda: consumes OrderCreated events from the fulfilment SQS queue, records a
fulfilment in the Fulfilments table and publishes OrderFulfilled to the orders event bus.

Messages are processed as a batch with partial failures: only the records that raise are
returned to SQS for a retry, the rest of the batch is deleted.

Idempotency (exercise 8): EventBridge and SQS deliver at least once, so the same OrderCreated
can arrive more than once. The fulfilment item (one per orderId) is claimed with a conditional
write and marked COMPLETED only after OrderFulfilled is published: duplicates are skipped, and
a fulfilment whose event couldn't be published is resumed by the retry."""
import hashlib
import json
import os
import time
from datetime import datetime, timezone

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from botocore.exceptions import ClientError

logger = Logger()
logger.append_keys(app_version=os.environ.get("APP_VERSION", "local"))  # git SHA set by CI
tracer = Tracer(patch_modules=["botocore"])

TABLE_NAME = os.environ["TABLE_NAME"]
EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]
EVENT_SOURCE = "orders.service"
# Exercise 9: publish through the EventBridge global endpoint when one is configured. The
# endpoint routes to the primary region's bus, or the secondary's after a failover; requests
# are signed with SigV4a, which needs awscrt (CRT layer). Empty = publish to the regional bus.
EVENT_ENDPOINT_ID = os.environ.get("EVENT_ENDPOINT_ID") or None
# Simulated fulfilment network: an order is always routed to the same warehouse
WAREHOUSES = ("IAD-1", "CMH-1", "PDX-1")
# Claim lifetime: longer than the function timeout (10 s), shorter than the queue's visibility
# timeout (60 s) - so a message's own retry always finds an expired lock (same as CreateOrder)
LOCK_SECONDS = 30

table = boto3.resource("dynamodb").Table(TABLE_NAME)
events = boto3.client("events")
processor = BatchProcessor(event_type=EventType.SQS)


class InvalidOrderEvent(Exception):
    pass


def _parse(record: SQSRecord):
    try:
        event = json.loads(record.body)
    except json.JSONDecodeError:
        raise InvalidOrderEvent("message body is not JSON")
    if event.get("detail-type") != "OrderCreated":
        raise InvalidOrderEvent(f"unexpected detail-type {event.get('detail-type')!r}")
    detail = event.get("detail") or {}
    for field in ("orderId", "customerId", "itemCount", "total", "createdAt"):
        if field not in detail:
            raise InvalidOrderEvent(f"detail.{field} is missing")
    return event["id"], detail


def _warehouse(order_id):
    return WAREHOUSES[int(hashlib.sha256(order_id.encode()).hexdigest(), 16) % len(WAREHOUSES)]


def _condition_failed(err):
    return err.response["Error"]["Code"] == "ConditionalCheckFailedException"


@tracer.capture_method
def _claim(event_id, order, message_id):
    """Records the fulfilment and claims it for this message, or returns None if the order
    already has one."""
    fulfilment = {
        "orderId": order["orderId"],
        "customerId": order["customerId"],
        "status": "FULFILLED",
        "warehouse": _warehouse(order["orderId"]),
        "itemCount": order["itemCount"],
        "total": order["total"],  # kept as the string from the event (no float rounding)
        "orderCreatedAt": order["createdAt"],
        "fulfilledAt": datetime.now(timezone.utc).isoformat(),
        "sourceEventId": event_id,
        "fulfilledIn": os.environ.get("AWS_REGION", "local"),  # exercise 9: which region processed it
        "processingState": "IN_PROGRESS",
        "claimedBy": message_id,
        "lockExpiresAt": int(time.time()) + LOCK_SECONDS,
    }
    try:
        table.put_item(Item=fulfilment, ConditionExpression="attribute_not_exists(orderId)")
        return fulfilment
    except ClientError as err:
        if _condition_failed(err):
            return None
        raise


@tracer.capture_method
def _take_over(order_id, message_id):
    """Re-claims an IN_PROGRESS fulfilment left by an earlier, unfinished attempt."""
    try:
        return table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET claimedBy = :me, lockExpiresAt = :expires",
            ConditionExpression="processingState = :in_progress AND (claimedBy = :me OR lockExpiresAt < :now)",
            ExpressionAttributeValues={":me": message_id, ":in_progress": "IN_PROGRESS",
                                       ":now": int(time.time()), ":expires": int(time.time()) + LOCK_SECONDS},
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as err:
        if _condition_failed(err):
            return None
        raise


@tracer.capture_method
def _complete(order_id, message_id):
    try:
        table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET processingState = :completed REMOVE claimedBy, lockExpiresAt",
            ConditionExpression="processingState = :in_progress AND claimedBy = :me",
            ExpressionAttributeValues={":completed": "COMPLETED", ":in_progress": "IN_PROGRESS", ":me": message_id},
        )
    except ClientError as err:
        if not _condition_failed(err):
            raise
        logger.warning("claim lost before completing; OrderFulfilled may be duplicated",
                       extra={"orderId": order_id})


@tracer.capture_method
def _publish_order_fulfilled(fulfilment):
    detail = {k: fulfilment[k] for k in (
        "orderId", "customerId", "warehouse", "itemCount", "total", "orderCreatedAt", "fulfilledAt",
        "fulfilledIn")}
    via_endpoint = {"EndpointId": EVENT_ENDPOINT_ID} if EVENT_ENDPOINT_ID else {}
    resp = events.put_events(**via_endpoint, Entries=[{
        "EventBusName": EVENT_BUS_NAME,  # still required with an endpoint (same name in both regions)
        "Source": EVENT_SOURCE,
        "DetailType": "OrderFulfilled",
        # A resumed fulfilment is read back from DynamoDB, where numbers are Decimal
        "Detail": json.dumps(detail, default=lambda d: int(d) if d == d.to_integral_value() else str(d)),
    }])
    if resp.get("FailedEntryCount"):
        entry = resp["Entries"][0]
        raise RuntimeError(f"{entry.get('ErrorCode')}: {entry.get('ErrorMessage')}")


def record_handler(record: SQSRecord):
    receive_count = int(record.attributes.approximate_receive_count)
    try:
        event_id, order = _parse(record)
        fulfilment = _claim(event_id, order, record.message_id)
        if fulfilment is None:
            existing = table.get_item(Key={"orderId": order["orderId"]}, ConsistentRead=True)["Item"]
            # Fulfilments recorded before exercise 8 have no processingState - they are complete
            if existing.get("processingState", "COMPLETED") == "COMPLETED":
                logger.info("duplicate OrderCreated: already fulfilled", extra={
                    "orderId": order["orderId"], "messageId": record.message_id, "eventId": event_id,
                    "firstEventId": existing.get("sourceEventId")})
                return
            fulfilment = _take_over(order["orderId"], record.message_id)
            if fulfilment is None:
                logger.info("duplicate OrderCreated in progress elsewhere: dropping", extra={
                    "orderId": order["orderId"], "messageId": record.message_id})
                return
            logger.info("resuming unfinished fulfilment", extra={"orderId": order["orderId"]})
        # Failing here is safe: the record goes back to SQS and the retry resumes the claim
        _publish_order_fulfilled(fulfilment)
        _complete(order["orderId"], record.message_id)
    except Exception:
        logger.exception("fulfilment failed", extra={
            "messageId": record.message_id, "receiveCount": receive_count})
        raise  # reported to SQS as a batch item failure
    sent_ms = int(record.attributes.sent_timestamp)
    logger.info("order fulfilled", extra={
        "orderId": order["orderId"],
        "warehouse": fulfilment["warehouse"],
        "messageId": record.message_id,
        "receiveCount": receive_count,
        # Queue dwell time: how long the message waited in SQS before Lambda received it
        "queueWaitMs": int(record.attributes.approximate_first_receive_timestamp) - sent_ms,
        "sentTimestamp": sent_ms,
    })


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    return process_partial_response(
        event=event, record_handler=record_handler, processor=processor, context=context)
