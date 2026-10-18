"""FulfilOrder Lambda: consumes OrderCreated events from the fulfilment SQS queue, records a
fulfilment in the Fulfilments table and publishes OrderFulfilled to the orders event bus.

Messages are processed as a batch with partial failures: only the records that raise are
returned to SQS for a retry, the rest of the batch is deleted."""
import hashlib
import json
import os
from datetime import datetime, timezone

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

logger = Logger()
logger.append_keys(app_version=os.environ.get("APP_VERSION", "local"))  # git SHA set by CI
tracer = Tracer(patch_modules=["botocore"])

TABLE_NAME = os.environ["TABLE_NAME"]
EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]
EVENT_SOURCE = "orders.service"
# Simulated fulfilment network: an order is always routed to the same warehouse
WAREHOUSES = ("IAD-1", "CMH-1", "PDX-1")

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


@tracer.capture_method
def _fulfil(event_id, order):
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
    }
    table.put_item(Item=fulfilment)
    return fulfilment


@tracer.capture_method
def _publish_order_fulfilled(fulfilment):
    detail = {k: fulfilment[k] for k in (
        "orderId", "customerId", "warehouse", "itemCount", "total", "orderCreatedAt", "fulfilledAt")}
    resp = events.put_events(Entries=[{
        "EventBusName": EVENT_BUS_NAME,
        "Source": EVENT_SOURCE,
        "DetailType": "OrderFulfilled",
        "Detail": json.dumps(detail),
    }])
    if resp.get("FailedEntryCount"):
        entry = resp["Entries"][0]
        raise RuntimeError(f"{entry.get('ErrorCode')}: {entry.get('ErrorMessage')}")


def record_handler(record: SQSRecord):
    receive_count = int(record.attributes.approximate_receive_count)
    try:
        event_id, order = _parse(record)
        fulfilment = _fulfil(event_id, order)
        # Unlike the API path, failing here is safe: the record goes back to SQS and is retried
        _publish_order_fulfilled(fulfilment)
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
