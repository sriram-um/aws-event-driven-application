import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
from aws_lambda_powertools.utilities.batch.exceptions import BatchProcessingError
from moto import mock_aws

SRC = Path(__file__).resolve().parents[2] / "src" / "fulfil_order"
TABLE = "test-fulfilments"
BUS = "test-bus"


@dataclass
class FakeLambdaContext:
    function_name: str = "FulfilOrder"
    memory_limit_in_mb: int = 256
    invoked_function_arn: str = "arn:aws:lambda:us-east-1:123456789012:function:FulfilOrder"
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
            AttributeDefinitions=[{"AttributeName": "orderId", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "orderId", "KeyType": "HASH"}],
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


def _order_created(order_id, **overrides):
    detail = {
        "orderId": order_id,
        "customerId": "user-123",
        "items": [{"itemId": "A", "productName": "Mouse", "quantity": 2, "unitPrice": "10.5"}],
        "itemCount": 1,
        "total": "21.0",
        "createdAt": "2026-10-18T14:00:00+00:00",
    }
    detail.update(overrides)
    return {"id": f"evt-{order_id}", "detail-type": "OrderCreated", "source": "orders.service", "detail": detail}


def _sqs_event(*bodies):
    return {"Records": [{
        "messageId": f"msg-{i}",
        "receiptHandle": f"rh-{i}",
        "body": body if isinstance(body, str) else json.dumps(body),
        "attributes": {"ApproximateReceiveCount": "1", "SentTimestamp": "1760796000000",
                       "ApproximateFirstReceiveTimestamp": "1760796000050"},
        "messageAttributes": {},
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:fulfilment",
        "awsRegion": "us-east-1",
    } for i, body in enumerate(bodies)]}


def _fulfilments():
    return {r["orderId"]: r for r in boto3.resource("dynamodb").Table(TABLE).scan()["Items"]}


def test_fulfils_order_and_publishes_event(app, published):
    resp = app.lambda_handler(_sqs_event(_order_created("o-1")), CTX)

    assert resp == {"batchItemFailures": []}
    row = _fulfilments()["o-1"]
    assert row["status"] == "FULFILLED"
    assert row["customerId"] == "user-123"
    assert row["total"] == "21.0"
    assert row["sourceEventId"] == "evt-o-1"
    assert row["warehouse"] in app.WAREHOUSES

    assert len(published) == 1
    assert published[0]["DetailType"] == "OrderFulfilled"
    detail = json.loads(published[0]["Detail"])
    assert detail["orderId"] == "o-1"
    assert detail["warehouse"] == row["warehouse"]


def test_same_order_always_goes_to_same_warehouse(app):
    assert app._warehouse("o-1") == app._warehouse("o-1")


@pytest.mark.parametrize("bad", [
    "not json",
    {"id": "x", "detail-type": "SomethingElse", "detail": {}},
    _order_created("o-x", customerId=None) | {"detail": {"orderId": "o-x"}},
])
def test_reports_only_failed_records(app, published, bad):
    resp = app.lambda_handler(_sqs_event(_order_created("o-1"), bad, _order_created("o-2")), CTX)

    assert resp == {"batchItemFailures": [{"itemIdentifier": "msg-1"}]}
    assert set(_fulfilments()) == {"o-1", "o-2"}
    assert len(published) == 2


def _failing_put_events(**kwargs):
    return {"FailedEntryCount": 1, "Entries": [{"ErrorCode": "InternalFailure", "ErrorMessage": "boom"}]}


def test_publish_failure_fails_the_record_for_retry(app, monkeypatch):
    real_put_events = app.events.put_events
    monkeypatch.setattr(app.events, "put_events", lambda **kw: (
        _failing_put_events() if json.loads(kw["Entries"][0]["Detail"])["orderId"] == "o-2"
        else real_put_events(**kw)))
    resp = app.lambda_handler(_sqs_event(_order_created("o-1"), _order_created("o-2")), CTX)
    assert resp == {"batchItemFailures": [{"itemIdentifier": "msg-1"}]}


def test_whole_batch_failure_raises(app, monkeypatch):
    # Powertools raises when every record fails, so the invocation is a Lambda error
    # (visible in the Errors metric) and SQS retries the whole batch
    monkeypatch.setattr(app.events, "put_events", _failing_put_events)
    with pytest.raises(BatchProcessingError):
        app.lambda_handler(_sqs_event(_order_created("o-1")), CTX)


# --- Exercise 8: idempotency -----------------------------------------------------------------

def test_duplicate_order_created_is_fulfilled_once(app, published):
    app.lambda_handler(_sqs_event(_order_created("o-1")), CTX)
    first = _fulfilments()["o-1"]
    # The same order delivered again (EventBridge/SQS at-least-once, or a re-published event)
    duplicate = _order_created("o-1") | {"id": "evt-o-1-again"}
    resp = app.lambda_handler(_sqs_event(duplicate, _order_created("o-1")), CTX)

    assert resp == {"batchItemFailures": []}
    assert len(published) == 1
    assert _fulfilments()["o-1"] == first
    assert first["processingState"] == "COMPLETED"


def test_publish_failure_is_resumed_by_the_retry(app, monkeypatch, published):
    recorder = app.events.put_events  # the published fixture
    monkeypatch.setattr(app.events, "put_events", _failing_put_events)
    with pytest.raises(BatchProcessingError):
        app.lambda_handler(_sqs_event(_order_created("o-1")), CTX)
    assert _fulfilments()["o-1"]["processingState"] == "IN_PROGRESS"

    monkeypatch.setattr(app.events, "put_events", recorder)
    resp = app.lambda_handler(_sqs_event(_order_created("o-1")), CTX)  # same messageId = SQS retry
    assert resp == {"batchItemFailures": []}
    assert _fulfilments()["o-1"]["processingState"] == "COMPLETED"
    assert len(published) == 1
