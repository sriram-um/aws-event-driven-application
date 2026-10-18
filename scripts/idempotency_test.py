"""Idempotency tests against the deployed stack (exercise 8).

  concurrent  - the same PUT /orders/{id} sent 10x in parallel through the API
  sqs-burst   - 10 identical messages sent straight to the intake queue in one batch
  conflict    - the same orderId re-used with different items, and by another customer
  redeliver   - the stored OrderCreated event re-sent 3x to the fulfilment queue
  publish-gap - CreateOrder's PutEvents broken (EVENT_BUS_NAME pointed at a missing bus) while
                an order is processed, then restored: the SQS retry must resume and publish.
                The function's environment is always restored (finally block).

For every scenario the script counts what actually happened for the orderId: order items in
DynamoDB, OrderCreated / OrderFulfilled events in the /aws/events log group, fulfilments, and
the CreateOrder / FulfilOrder log messages.

Usage: python scripts/idempotency_test.py [scenario ...]   (default: all)
"""
import collections
import json
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from boto3.dynamodb.conditions import Key

from measure_fulfilment import ORDER, STACK, outputs, put_order, session, token

sqs = session.client("sqs")
logs = session.client("logs")
lam = session.client("lambda")
orders = session.resource("dynamodb").Table(outputs["OrdersTableName"])
fulfilments = session.resource("dynamodb").Table(outputs["FulfilmentsTableName"])
EVENTS_LOG_GROUP = f"/aws/events/{STACK}"


def log_messages(group, order_id, start_ms):
    found, kwargs = [], {"logGroupName": group, "startTime": start_ms, "filterPattern": f'"{order_id}"'}
    while True:
        page = logs.filter_log_events(**kwargs)
        found += [json.loads(e["message"]) for e in page["events"]]
        if "nextToken" not in page:
            return found
        kwargs["nextToken"] = page["nextToken"]


def summary(order_id, start_ms, settle=20):
    time.sleep(settle)  # let processing finish and logs arrive
    items = orders.query(KeyConditionExpression=Key("orderId").eq(order_id), ConsistentRead=True)["Items"]
    header = next((i for i in items if i["sk"] == "ORDER"), {})
    events = collections.Counter(e["detail-type"] for e in log_messages(EVENTS_LOG_GROUP, order_id, start_ms))
    create = collections.Counter(m["message"] for m in log_messages(
        f"/aws/lambda/{outputs['CreateOrderFunctionName']}", order_id, start_ms) if "message" in m)
    fulfil = collections.Counter(m["message"] for m in log_messages(
        f"/aws/lambda/{outputs['FulfilOrderFunctionName']}", order_id, start_ms) if "message" in m)
    result = {
        "order items (header + lines)": len(items),
        "header": {k: str(header.get(k)) for k in ("status", "processingState", "total", "customerId")},
        "OrderCreated events": events["OrderCreated"],
        "OrderFulfilled events": events["OrderFulfilled"],
        "fulfilments": 1 if fulfilments.get_item(Key={"orderId": order_id}).get("Item") else 0,
        "CreateOrder log": dict(create),
        "FulfilOrder log": dict(fulfil),
    }
    print(json.dumps(result, indent=2))
    return result


def concurrent():
    order_id, id_token = str(uuid.uuid4()), token()
    start = int(time.time() * 1000)
    with ThreadPoolExecutor(max_workers=10) as pool:
        statuses = list(pool.map(lambda _: put_order(id_token, order_id)[1], range(10)))
    print(f"10 parallel PUT /orders/{order_id[:8]}...: HTTP {collections.Counter(statuses)}")
    return summary(order_id, start)


def sqs_burst():
    order_id, start = str(uuid.uuid4()), int(time.time() * 1000)
    attrs = {"orderId": order_id, "customerId": "idempotency-test", "acceptedAt": str(start), "requestId": "sqs-burst"}
    sqs.send_message_batch(QueueUrl=outputs["OrderIntakeQueueUrl"], Entries=[{
        "Id": str(i), "MessageBody": json.dumps(ORDER),
        "MessageAttributes": {k: {"DataType": "Number" if k == "acceptedAt" else "String", "StringValue": v}
                              for k, v in attrs.items()},
    } for i in range(10)])
    print(f"10 identical messages for {order_id[:8]}... sent to the intake queue in one batch")
    return summary(order_id, start)


def conflict():
    order_id, start = str(uuid.uuid4()), int(time.time() * 1000)
    put_order(token(), order_id)
    time.sleep(5)
    put_order(token(), order_id, {"items": [{"itemId": "OTHER", "quantity": 9, "unitPrice": 0}]})
    put_order(token("TEST_USER2", "TEST_PASSWORD2"), order_id)  # another customer, same items
    print(f"{order_id[:8]}...: original, then different items, then another customer")
    return summary(order_id, start)


def redeliver():
    order_id, start = str(uuid.uuid4()), int(time.time() * 1000)
    put_order(token(), order_id)
    time.sleep(10)
    created = next(e for e in log_messages(EVENTS_LOG_GROUP, order_id, start) if e["detail-type"] == "OrderCreated")
    for _ in range(3):
        sqs.send_message(QueueUrl=outputs["FulfilmentQueueUrl"], MessageBody=json.dumps(created))
    print(f"{order_id[:8]}...: its OrderCreated event re-sent 3x to the fulfilment queue")
    return summary(order_id, start)


def publish_gap():
    fn = outputs["CreateOrderFunctionName"]
    original = lam.get_function_configuration(FunctionName=fn)["Environment"]["Variables"]
    order_id, start = str(uuid.uuid4()), int(time.time() * 1000)
    try:
        lam.update_function_configuration(FunctionName=fn, Environment={"Variables": {
            **original, "EVENT_BUS_NAME": f"{STACK}-missing-bus"}})
        lam.get_waiter("function_updated_v2").wait(FunctionName=fn)
        print("CreateOrder EVENT_BUS_NAME -> missing bus (PutEvents will fail)")
        put_order(token(), order_id)
        deadline = time.time() + 60
        while not any(m.get("message") == "order processing failed" for m in log_messages(
                f"/aws/lambda/{fn}", order_id, start)) and time.time() < deadline:
            time.sleep(3)
        header = orders.get_item(Key={"orderId": order_id, "sk": "ORDER"}, ConsistentRead=True)["Item"]
        print(f"first attempt failed after saving the order: processingState={header['processingState']}")
    finally:
        lam.update_function_configuration(FunctionName=fn, Environment={"Variables": original})
        lam.get_waiter("function_updated_v2").wait(FunctionName=fn)
        print("CreateOrder environment restored")
    deadline = time.time() + 150
    while time.time() < deadline:  # the retry comes after the 60 s visibility timeout
        if orders.get_item(Key={"orderId": order_id, "sk": "ORDER"}, ConsistentRead=True)["Item"].get(
                "processingState") == "COMPLETED":
            print(f"resumed and completed {time.time() * 1000 - start:.0f} ms after the PUT")
            break
        time.sleep(5)
    return summary(order_id, start)


SCENARIOS = {"concurrent": concurrent, "sqs-burst": sqs_burst, "conflict": conflict,
             "redeliver": redeliver, "publish-gap": publish_gap}

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    for name in sys.argv[1:] or SCENARIOS:
        print(f"\n===== {name} =====")
        SCENARIOS[name]()
