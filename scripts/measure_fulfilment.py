"""Sends N orders through the async API and breaks down the end-to-end latency:

  PUT /orders/{id} accepted (acceptedAt) -> order saved by CreateOrder (createdAt)
    -> fulfilment SQS SentTimestamp -> first receive by FulfilOrder -> fulfilledAt

Also reports the client-side round trip of the PUT (the 202) measured in this process.

Usage (credentials from ~/.orders-event-driven-test-user.env):
  python scripts/measure_fulfilment.py [count] [interval_seconds]
"""
import json
import statistics
import sys
import time
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

import boto3

STACK = "orders-event-driven-app"
REGION = "us-east-1"
ORDER = {"items": [{"itemId": "SKU-1001", "productName": "measure-fulfilment", "quantity": 1, "unitPrice": 1}]}

session = boto3.Session(profile_name="default", region_name=REGION)
env = dict(line.split("=", 1) for line in
           (Path.home() / ".orders-event-driven-test-user.env").read_text().split())
outputs = {o["OutputKey"]: o["OutputValue"] for o in
           session.client("cloudformation").describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]}


def token(user="TEST_USER", password="TEST_PASSWORD"):
    resp = session.client("cognito-idp").initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH", ClientId=env["CLIENT_ID"],
        AuthParameters={"USERNAME": env[user], "PASSWORD": env[password]})
    return resp["AuthenticationResult"]["IdToken"]


def put_order(id_token, order_id=None, order=ORDER):
    """PUT /orders/{orderId}; returns (orderId, HTTP status, round trip in ms)."""
    order_id = order_id or str(uuid.uuid4())
    req = urllib.request.Request(
        f"{outputs['ApiUrl']}/orders/{order_id}", data=json.dumps(order).encode(), method="PUT",
        headers={"Authorization": id_token, "Content-Type": "application/json"})
    start = time.perf_counter()
    with urllib.request.urlopen(req) as resp:
        resp.read()
        return order_id, resp.status, (time.perf_counter() - start) * 1000


def ms(iso):
    return datetime.fromisoformat(iso).timestamp() * 1000


def fulfilled_logs(start_ms, order_ids):
    logs = session.client("logs")
    found, kwargs = {}, {"logGroupName": f"/aws/lambda/{outputs['FulfilOrderFunctionName']}",
                         "startTime": start_ms, "filterPattern": '"order fulfilled"'}
    while True:
        page = logs.filter_log_events(**kwargs)
        for e in page["events"]:
            msg = json.loads(e["message"])
            if msg.get("orderId") in order_ids:
                found[msg["orderId"]] = msg
        if "nextToken" not in page:
            return found
        kwargs["nextToken"] = page["nextToken"]


def main(count=10, interval=2.0):
    id_token = token()
    start_ms = int(time.time() * 1000)
    order_ids, round_trips = [], []
    for _ in range(count):
        order_id, status, rtt = put_order(id_token)
        assert status == 202, status
        order_ids.append(order_id)
        round_trips.append(rtt)
        time.sleep(interval)

    orders = session.resource("dynamodb").Table(outputs["OrdersTableName"])
    fulfilments = session.resource("dynamodb").Table(outputs["FulfilmentsTableName"])
    deadline = time.time() + 90
    while time.time() < deadline:
        headers = {i: orders.get_item(Key={"orderId": i, "sk": "ORDER"}).get("Item") for i in order_ids}
        rows = {i: fulfilments.get_item(Key={"orderId": i}).get("Item") for i in order_ids}
        logs = fulfilled_logs(start_ms, set(order_ids))
        if all(rows.values()) and len(logs) == len(order_ids):
            break
        time.sleep(5)

    stages = {"PUT round trip (client)": round_trips, "accepted -> order saved": [],
              "saved -> fulfilment SQS": [], "fulfilment SQS wait": [],
              "receive -> fulfilled": [], "total accepted -> fulfilled": []}
    for i in order_ids:
        header, row, log = headers.get(i), rows.get(i), logs.get(i)
        if not header or not row or not log:
            print(f"{i}: not fulfilled yet")
            continue
        accepted, created, fulfilled = ms(header["acceptedAt"]), ms(header["createdAt"]), ms(row["fulfilledAt"])
        sent, wait = log["sentTimestamp"], log["queueWaitMs"]
        stages["accepted -> order saved"].append(created - accepted)
        stages["saved -> fulfilment SQS"].append(sent - created)
        stages["fulfilment SQS wait"].append(wait)
        stages["receive -> fulfilled"].append(fulfilled - sent - wait)
        stages["total accepted -> fulfilled"].append(fulfilled - accepted)

    print(f"{len(stages['total accepted -> fulfilled'])}/{count} orders fulfilled (interval {interval}s)")
    print(f"{'stage':<30}{'min':>8}{'median':>8}{'max':>8}  (ms)")
    for name, values in stages.items():
        if values:
            print(f"{name:<30}{min(values):>8.0f}{statistics.median(values):>8.0f}{max(values):>8.0f}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 10,
         float(sys.argv[2]) if len(sys.argv) > 2 else 2.0)
