import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
from aws_lambda_powertools.utilities.batch.exceptions import BatchProcessingError
from moto import mock_aws

SRC = Path(__file__).resolve().parents[2] / "src" / "create_order"
TABLE = "test-orders"
BUS = "test-bus"
ORDER_ID = "3f0c6a8e-1b2d-4c5e-8f9a-0b1c2d3e4f5a"


@dataclass
class FakeLambdaContext:
    function_name: str = "CreateOrder"
    memory_limit_in_mb: int = 256
    invoked_function_arn: str = "arn:aws:lambda:us-east-1:123456789012:function:CreateOrder"
    aws_request_id: str = "test-request-id"


CTX = FakeLambdaContext()


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", TABLE)
    monkeypatch.setenv("EVENT_BUS_NAME", BUS)
    monkeypatch.setenv("POWERTOOLS_TRACE_DISABLED", "true")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        boto3.client("dynamodb").create_table(
            TableName=TABLE,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "orderId", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "orderId", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
        )
        boto3.client("events").create_event_bus(Name=BUS)
        sys.path.insert(0, str(SRC))
        module = importlib.reload(importlib.import_module("app"))
        yield module
        sys.path.remove(str(SRC))


@pytest.fixture
def published(app, monkeypatch):
    """Records PutEvents entries while still calling the (moto) EventBridge API."""
    entries = []
    real_put_events = app.events.put_events

    def recorder(**kwargs):
        entries.extend(kwargs["Entries"])
        return real_put_events(**kwargs)

    monkeypatch.setattr(app.events, "put_events", recorder)
    return entries


def _record(body, order_id=ORDER_ID, customer_id="user-123", i=0):
    """An SQS record as produced by the API Gateway -> SQS integration."""
    attrs = {"orderId": order_id, "customerId": customer_id, "acceptedAt": "1760796000000", "requestId": f"req-{i}"}
    return {
        "messageId": f"msg-{i}",
        "receiptHandle": f"rh-{i}",
        "body": body if isinstance(body, str) else json.dumps(body),
        "attributes": {"ApproximateReceiveCount": "1", "SentTimestamp": "1760796000010"},
        "messageAttributes": {k: {"stringValue": v, "dataType": "String"} for k, v in attrs.items() if v is not None},
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:intake",
        "awsRegion": "us-east-1",
    }


def _sqs_event(*records):
    return {"Records": list(records)}


def _rows():
    return {(r["orderId"], r["sk"]): r for r in boto3.resource("dynamodb").Table(TABLE).scan()["Items"]}


ITEMS = {"items": [
    {"itemId": "A", "productName": "Mouse", "quantity": 2, "unitPrice": 10.5},
    {"itemId": "B", "productName": "Hub", "quantity": 1, "unitPrice": 4},
]}


def test_creates_header_and_one_row_per_item(app):
    resp = app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)

    assert resp == {"batchItemFailures": []}
    rows = _rows()
    assert set(rows) == {(ORDER_ID, "ORDER"), (ORDER_ID, "ITEM#A"), (ORDER_ID, "ITEM#B")}
    header = rows[(ORDER_ID, "ORDER")]
    assert header["status"] == "CREATED"
    assert header["customerId"] == "user-123"
    assert header["itemCount"] == 2
    assert str(header["total"]) == "25.0"
    assert header["acceptedAt"].startswith("2025-10-18T14:00:00")
    assert rows[(ORDER_ID, "ITEM#A")]["quantity"] == 2


def test_publishes_order_created_event(app, published):
    app.lambda_handler(_sqs_event(_record({"items": [ITEMS["items"][0]]})), CTX)

    assert len(published) == 1
    entry = published[0]
    assert entry["EventBusName"] == BUS
    assert entry["Source"] == "orders.service"
    assert entry["DetailType"] == "OrderCreated"
    detail = json.loads(entry["Detail"])
    assert detail["orderId"] == ORDER_ID
    assert detail["customerId"] == "user-123"
    assert detail["total"] == "21.0"
    assert detail["items"] == [{"itemId": "A", "productName": "Mouse", "quantity": 2, "unitPrice": "10.5"}]


@pytest.mark.parametrize("body,reason", [
    ("not json", "valid JSON"),
    ("[]", "JSON object"),
    ({"items": []}, "non-empty"),
    ({"items": [{"itemId": "A", "quantity": 1, "unitPrice": 1}, {"itemId": "A", "quantity": 1, "unitPrice": 1}]}, "duplicate itemId"),
    ({"items": [{"itemId": "A", "quantity": 0, "unitPrice": 1}]}, "positive integer"),
    ({"items": [{"itemId": f"S{i}", "quantity": 1, "unitPrice": 1} for i in range(26)]}, "at most 25"),
])
def test_invalid_order_is_stored_as_rejected_not_retried(app, published, body, reason):
    resp = app.lambda_handler(_sqs_event(_record(body)), CTX)

    assert resp == {"batchItemFailures": []}  # permanent failure: consumed, not retried
    rows = _rows()
    assert set(rows) == {(ORDER_ID, "ORDER")}
    assert rows[(ORDER_ID, "ORDER")]["status"] == "REJECTED"
    assert reason in rows[(ORDER_ID, "ORDER")]["reason"]
    assert published == []


def test_missing_message_attribute_fails_only_that_record(app):
    other = "11111111-2222-4333-8444-555555555555"
    resp = app.lambda_handler(_sqs_event(
        _record(ITEMS, customer_id=None, i=0),
        _record(ITEMS, order_id=other, i=1),
    ), CTX)

    assert resp == {"batchItemFailures": [{"itemIdentifier": "msg-0"}]}
    assert (other, "ORDER") in _rows()


def _header():
    return _rows()[(ORDER_ID, "ORDER")]


def _set_header(**attrs):
    """Puts the header into a given state (e.g. an unfinished claim from an earlier attempt)."""
    expr = ", ".join(f"#{k} = :{k}" for k in attrs)
    boto3.resource("dynamodb").Table(TABLE).update_item(
        Key={"orderId": ORDER_ID, "sk": "ORDER"}, UpdateExpression=f"SET {expr}",
        ExpressionAttributeNames={f"#{k}": k for k in attrs},
        ExpressionAttributeValues={f":{k}": v for k, v in attrs.items()})


def _fail_publish(monkeypatch, app):
    monkeypatch.setattr(app.events, "put_events", lambda **kw: {
        "FailedEntryCount": 1, "Entries": [{"ErrorCode": "InternalFailure", "ErrorMessage": "boom"}]})


# --- Exercise 8: idempotency -----------------------------------------------------------------

def test_completed_order_records_idempotency_state(app, published):
    app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)

    header = _header()
    assert header["processingState"] == "COMPLETED"
    assert len(header["payloadHash"]) == 64
    assert header["eventId"]
    assert "claimedBy" not in header and "lockExpiresAt" not in header


def test_duplicate_returns_stored_result_without_side_effects(app, published):
    app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    first = _header()
    # Same request again (client retry / SQS redelivery), items in a different order and
    # prices formatted differently - still the same order
    retry = {"items": [{**ITEMS["items"][1], "unitPrice": 4.00}, ITEMS["items"][0]]}
    resp = app.lambda_handler(_sqs_event(_record(retry, i=1)), CTX)

    assert resp == {"batchItemFailures": []}
    assert len(published) == 1                      # no second OrderCreated
    assert _header() == first                       # nothing rewritten
    assert app.record_handler(app.SQSRecord(_record(retry, i=2))) == app._result(first)


@pytest.mark.parametrize("change", [
    {"body": {"items": [ITEMS["items"][0]]}},                     # different items
    {"customer_id": "someone-else"},                               # another customer, same id
])
def test_same_order_id_different_request_is_a_conflict(app, published, change):
    app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    first = _header()
    resp = app.lambda_handler(_sqs_event(_record(change.get("body", ITEMS), i=1,
                                                 customer_id=change.get("customer_id", "user-123"))), CTX)

    assert resp == {"batchItemFailures": []}        # permanent: consumed, not retried
    assert _header() == first
    assert len(published) == 1


def test_publish_failure_fails_record_and_retry_resumes(app, monkeypatch, published):
    recorder = app.events.put_events  # the published fixture
    _fail_publish(monkeypatch, app)
    # The dual-write gap is closed: the record fails instead of silently losing the event
    # (a batch whose only record fails raises, so the whole invocation is retried)...
    with pytest.raises(BatchProcessingError):
        app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    assert _header()["processingState"] == "IN_PROGRESS"
    assert len(_rows()) == 3                         # order + lines saved once

    # ...and SQS's retry of the same message resumes it
    monkeypatch.setattr(app.events, "put_events", recorder)
    resp = app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    assert resp == {"batchItemFailures": []}
    assert _header()["processingState"] == "COMPLETED"
    assert len(published) == 1
    assert len(_rows()) == 3


def test_concurrent_copy_with_live_claim_is_dropped(app, published):
    app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    # Another message is processing this order right now (fresh lock)
    _set_header(processingState="IN_PROGRESS", claimedBy="msg-other", lockExpiresAt=int(app.time.time()) + 30)
    resp = app.lambda_handler(_sqs_event(_record(ITEMS, i=1)), CTX)

    assert resp == {"batchItemFailures": []}
    assert len(published) == 1                       # only the original publish
    assert _header()["claimedBy"] == "msg-other"     # claim untouched


def test_expired_claim_of_another_message_is_taken_over(app, published):
    app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    # An earlier attempt by another message crashed after saving the order
    _set_header(processingState="IN_PROGRESS", claimedBy="msg-crashed", lockExpiresAt=int(app.time.time()) - 1)
    resp = app.lambda_handler(_sqs_event(_record(ITEMS, i=1)), CTX)

    assert resp == {"batchItemFailures": []}
    assert _header()["processingState"] == "COMPLETED"
    assert len(published) == 2                       # the event the crashed attempt never sent


def test_order_saved_before_exercise_8_counts_as_completed(app, published):
    boto3.resource("dynamodb").Table(TABLE).put_item(Item={
        "orderId": ORDER_ID, "sk": "ORDER", "customerId": "user-123", "status": "CREATED"})
    resp = app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)

    assert resp == {"batchItemFailures": []}
    assert published == []                           # no payloadHash -> treated as a conflict, never reprocessed


def test_payload_hash_ignores_item_order_and_number_format(app):
    a = app._parse_items(ITEMS)
    b = app._parse_items({"items": [{**ITEMS["items"][1], "unitPrice": "4.000"}, ITEMS["items"][0]]})
    assert app._payload_hash("u", a) == app._payload_hash("u", b)
    assert app._payload_hash("u", a) != app._payload_hash("v", a)


def test_publishes_through_global_endpoint_when_configured(app, monkeypatch):
    calls = []
    monkeypatch.setattr(app, "EVENT_ENDPOINT_ID", "abcde.fghij")
    monkeypatch.setattr(app.events, "put_events", lambda **kw: calls.append(kw) or {
        "FailedEntryCount": 0, "Entries": [{"EventId": "evt-1"}]})
    app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)

    assert calls[0]["EndpointId"] == "abcde.fghij"
    assert calls[0]["Entries"][0]["EventBusName"] == BUS  # still required with an endpoint
