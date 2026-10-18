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
        sys.path.insert(0, str(SRC))
        module = importlib.reload(importlib.import_module("app"))
        yield module
        sys.path.remove(str(SRC))


def _event(body):
    return {"body": json.dumps(body) if not isinstance(body, str) else body}


def _rows():
    return boto3.resource("dynamodb").Table(TABLE).scan()["Items"]


def test_creates_header_and_one_row_per_item(app):
    resp = app.lambda_handler(_event({"items": [
        {"itemId": "A", "productName": "Mouse", "quantity": 2, "unitPrice": 10.5},
        {"itemId": "B", "productName": "Hub", "quantity": 1, "unitPrice": 4},
    ]}), CTX)

    assert resp["statusCode"] == 201
    body = json.loads(resp["body"])
    assert body["itemCount"] == 2
    assert body["total"] == "25.0"

    rows = {r["sk"]: r for r in _rows()}
    assert set(rows) == {"ORDER", "ITEM#A", "ITEM#B"}
    assert all(r["orderId"] == body["orderId"] for r in rows.values())
    header = rows["ORDER"]
    assert header["status"] == "CREATED"
    assert header["itemCount"] == 2
    assert str(header["total"]) == "25.0"
    assert rows["ITEM#A"]["quantity"] == 2


def test_uses_cognito_sub_as_customer(app):
    event = _event({"items": [{"itemId": "A", "quantity": 1, "unitPrice": 1}]})
    event["requestContext"] = {"authorizer": {"claims": {"sub": "user-123"}}}
    app.lambda_handler(event, CTX)
    header = next(r for r in _rows() if r["sk"] == "ORDER")
    assert header["customerId"] == "user-123"


@pytest.mark.parametrize("body", [
    "not json",
    "[]",
    {},
    {"items": []},
    {"items": [{"quantity": 1, "unitPrice": 1}]},
    {"items": [{"itemId": "A", "quantity": 0, "unitPrice": 1}]},
    {"items": [{"itemId": "A", "quantity": 1, "unitPrice": "abc"}]},
    {"items": [{"itemId": "A", "quantity": 1, "unitPrice": -1}]},
    {"items": [{"itemId": "A", "quantity": 1, "unitPrice": 1}, {"itemId": "A", "quantity": 1, "unitPrice": 1}]},
    {"items": [{"itemId": f"S{i}", "quantity": 1, "unitPrice": 1} for i in range(26)]},
])
def test_rejects_invalid_payloads(app, body):
    resp = app.lambda_handler(_event(body), CTX)
    assert resp["statusCode"] == 400
    assert _rows() == []
