"""Sends N orders through the API and breaks down the asynchronous fulfilment latency:

  order saved (createdAt) -> SQS SentTimestamp -> first receive by Lambda -> fulfilledAt

Usage (credentials from ~/.orders-event-driven-test-user.env):
  python scripts/measure_fulfilment.py [count] [interval_seconds]
"""
import json
import statistics
import sys
import time
import urllib.request
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


def token():
    resp = session.client("cognito-idp").initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH", ClientId=env["CLIENT_ID"],
        AuthParameters={"USERNAME": env["TEST_USER"], "PASSWORD": env["TEST_PASSWORD"]})
    return resp["AuthenticationResult"]["IdToken"]


def post_order(id_token):
    req = urllib.request.Request(
        f"{outputs['ApiUrl']}/orders", data=json.dumps(ORDER).encode(), method="POST",
        headers={"Authorization": id_token, "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)["orderId"]


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
    order_ids = []
    for _ in range(count):
        order_ids.append(post_order(id_token))
        time.sleep(interval)

    table = session.resource("dynamodb").Table(outputs["FulfilmentsTableName"])
    deadline = time.time() + 90
    while time.time() < deadline:
        rows = {i: table.get_item(Key={"orderId": i}).get("Item") for i in order_ids}
        logs = fulfilled_logs(start_ms, set(order_ids))
        if all(rows.values()) and len(logs) == len(order_ids):
            break
        time.sleep(5)

    stages = {"event -> SQS": [], "SQS wait": [], "receive -> fulfilled": [], "total": []}
    for i in order_ids:
        row, log = rows.get(i), logs.get(i)
        if not row or not log:
            print(f"{i}: not fulfilled yet")
            continue
        created, fulfilled = ms(row["orderCreatedAt"]), ms(row["fulfilledAt"])
        sent, wait = log["sentTimestamp"], log["queueWaitMs"]
        stages["event -> SQS"].append(sent - created)
        stages["SQS wait"].append(wait)
        stages["receive -> fulfilled"].append(fulfilled - sent - wait)
        stages["total"].append(fulfilled - created)

    print(f"{len(stages['total'])}/{count} orders fulfilled (interval {interval}s)")
    print(f"{'stage':<22}{'min':>8}{'median':>8}{'max':>8}  (ms)")
    for name, values in stages.items():
        if values:
            print(f"{name:<22}{min(values):>8.0f}{statistics.median(values):>8.0f}{max(values):>8.0f}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 10,
         float(sys.argv[2]) if len(sys.argv) > 2 else 2.0)
