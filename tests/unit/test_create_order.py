import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
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


def test_duplicate_order_id_is_skipped_without_second_event(app, published):
    app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    resp = app.lambda_handler(_sqs_event(_record({"items": [ITEMS["items"][0]]}, i=1)), CTX)

    assert resp == {"batchItemFailures": []}
    assert len(published) == 1
    assert str(_rows()[(ORDER_ID, "ORDER")]["total"]) == "25.0"  # first order kept


def test_missing_message_attribute_fails_only_that_record(app):
    other = "11111111-2222-4333-8444-555555555555"
    resp = app.lambda_handler(_sqs_event(
        _record(ITEMS, customer_id=None, i=0),
        _record(ITEMS, order_id=other, i=1),
    ), CTX)

    assert resp == {"batchItemFailures": [{"itemIdentifier": "msg-0"}]}
    assert (other, "ORDER") in _rows()


@pytest.mark.parametrize("failure", [
    {"FailedEntryCount": 1, "Entries": [{"ErrorCode": "InternalFailure", "ErrorMessage": "boom"}]},
    RuntimeError("network"),
])
def test_publish_failure_keeps_order_and_does_not_retry(app, monkeypatch, failure):
    def failing_put_events(**kwargs):
        if isinstance(failure, Exception):
            raise failure
        return failure

    monkeypatch.setattr(app.events, "put_events", failing_put_events)
    resp = app.lambda_handler(_sqs_event(_record(ITEMS)), CTX)
    assert resp == {"batchItemFailures": []}
    assert (ORDER_ID, "ORDER") in _rows()
