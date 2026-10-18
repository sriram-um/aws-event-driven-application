"""Multi-region / global endpoint tests against both deployed stacks (exercise 9).

  normal   - one order through each region's API. Shows where it was created, which region's
             bus ingested its events (the endpoint routes to the primary while healthy), where
             it was fulfilled, that both APIs return the same status (global tables), and that
             every event is also replicated to the other region's bus but processed only once.
  failover - inverts the Route 53 health check (= primary reported unhealthy), times how long
             until events are ingested in the secondary region, places orders through the
             primary's API, checks they are processed in the secondary region, then reverts the
             health check and times the failback. The health check is always reverted (finally).

Usage: python scripts/dr_test.py normal|failover
"""
import json
import sys
import time
import urllib.request
import uuid
from pathlib import Path

import boto3

STACK = "orders-event-driven-app"
REGIONS = ("us-east-1", "us-west-2")
ORDER = {"items": [{"itemId": "SKU-1001", "productName": "dr-test", "quantity": 1, "unitPrice": 1}]}

session = boto3.Session(profile_name="default")


class Region:
    def __init__(self, name):
        self.name = name
        self.env = dict(line.split("=", 1) for line in (Path.home() / (
            ".orders-event-driven-test-user.env" if name == "us-east-1"
            else f".orders-event-driven-test-user-{name}.env")).read_text().split())
        cfn = session.client("cloudformation", region_name=name)
        self.outputs = {o["OutputKey"]: o["OutputValue"]
                        for o in cfn.describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]}
        self.logs = session.client("logs", region_name=name)
        self.sqs = session.client("sqs", region_name=name)
        self.orders = session.resource("dynamodb", region_name=name).Table(self.outputs["OrdersTableName"])
        self.fulfilments = session.resource("dynamodb", region_name=name).Table(self.outputs["FulfilmentsTableName"])
        self._token = None

    def token(self):
        if not self._token:
            self._token = session.client("cognito-idp", region_name=self.name).initiate_auth(
                AuthFlow="USER_PASSWORD_AUTH", ClientId=self.env["CLIENT_ID"],
                AuthParameters={"USERNAME": self.env["TEST_USER"], "PASSWORD": self.env["TEST_PASSWORD"]},
            )["AuthenticationResult"]["IdToken"]
        return self._token

    def call(self, method, order_id, body=None):
        req = urllib.request.Request(f"{self.outputs['ApiUrl']}/orders/{order_id}", method=method,
                                     data=json.dumps(body).encode() if body else None,
                                     headers={"Authorization": self.token(), "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as err:
            return err.code, json.load(err)

    def put(self):
        order_id = str(uuid.uuid4())
        status, _ = self.call("PUT", order_id, ORDER)
        assert status == 202, status
        return order_id

    def events(self, order_id, start_ms):
        """(detail-type, ingesting region) of every event for the order in this region's log."""
        resp = self.logs.filter_log_events(logGroupName=f"/aws/events/{STACK}", startTime=start_ms,
                                           filterPattern=f'"{order_id}"')
        return sorted((json.loads(e["message"])["detail-type"], json.loads(e["message"])["region"])
                      for e in resp["events"])

    def notifications(self, order_ids):
        """Drains the test subscriber queue; returns how many notifications concern order_ids."""
        n = 0
        while True:
            msgs = self.sqs.receive_message(QueueUrl=self.outputs["NotificationsTestQueueUrl"],
                                            MaxNumberOfMessages=10, WaitTimeSeconds=2).get("Messages", [])
            if not msgs:
                return n
            for m in msgs:
                n += json.loads(m["Body"]).get("orderId") in order_ids
                self.sqs.delete_message(QueueUrl=self.outputs["NotificationsTestQueueUrl"], ReceiptHandle=m["ReceiptHandle"])


regions = {name: Region(name) for name in REGIONS}
primary, secondary = regions["us-east-1"], regions["us-west-2"]


def wait_fulfilled(order_ids, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(primary.fulfilments.get_item(Key={"orderId": i}).get("Item") for i in order_ids):
            return True
        time.sleep(3)
    return False


def report(order_ids, start_ms):
    time.sleep(10)  # replication + log delivery
    for order_id in order_ids:
        print(f"\norder {order_id[:8]}...")
        for r in regions.values():
            status, body = r.call("GET", order_id)
            print(f"  GET via {r.name:<9} -> {status} status={body.get('status')} "
                  f"createdIn={body.get('createdIn')} fulfilledIn={body.get('fulfilledIn')}")
        for r in regions.values():
            print(f"  events in {r.name} bus log (detail-type, ingested in): {r.events(order_id, start_ms)}")
    for r in regions.values():
        print(f"  notifications in {r.name} test subscriber: {r.notifications(set(order_ids))}")


def normal():
    for r in regions.values():
        r.notifications(set())  # drain
    start = int(time.time() * 1000)
    order_ids = [r.put() for r in regions.values()]
    print(f"placed {order_ids[0][:8]}... via us-east-1 API, {order_ids[1][:8]}... via us-west-2 API")
    print("fulfilled:", wait_fulfilled(order_ids))
    report(order_ids, start)


def ingested_in(region_obj, start_ms):
    """Places a probe order via the primary API and returns the region whose bus ingested it."""
    order_id = region_obj.put()
    deadline = time.time() + 60
    while time.time() < deadline:
        for r in regions.values():
            evts = [reg for kind, reg in r.events(order_id, start_ms) if kind == "OrderCreated"]
            if evts:
                return order_id, evts[0]
        time.sleep(3)
    return order_id, None


def failover():
    r53 = session.client("route53")
    endpoint = session.client("cloudformation", region_name="us-east-1").describe_stacks(
        StackName=f"{STACK}-global-endpoint")["Stacks"][0]["Outputs"]
    health_check = next(o["OutputValue"] for o in endpoint if o["OutputKey"] == "HealthCheckId")
    for r in regions.values():
        r.notifications(set())
    start = int(time.time() * 1000)
    try:
        r53.update_health_check(HealthCheckId=health_check, Inverted=True)
        t0 = time.time()
        print("health check inverted (primary reported unhealthy)")
        while True:
            order_id, region = ingested_in(primary, start)
            print(f"  +{time.time() - t0:4.0f} s probe {order_id[:8]}... ingested in {region}")
            if region == "us-west-2":
                print(f"FAILOVER: events go to us-west-2 {time.time() - t0:.0f} s after the inversion")
                break
            if time.time() - t0 > 900:
                raise TimeoutError("no failover after 15 min")
            time.sleep(15)
        order_ids = [primary.put() for _ in range(3)]
        print(f"placed 3 orders via the *us-east-1* API during the failover")
        print("fulfilled:", wait_fulfilled(order_ids))
        report(order_ids, start)
    finally:
        r53.update_health_check(HealthCheckId=health_check, Inverted=False)
        t1 = time.time()
        print("\nhealth check restored")
    while True:
        order_id, region = ingested_in(primary, start)
        print(f"  +{time.time() - t1:4.0f} s probe {order_id[:8]}... ingested in {region}")
        if region == "us-east-1":
            print(f"FAILBACK: events go to us-east-1 again {time.time() - t1:.0f} s after restoring")
            break
        if time.time() - t1 > 900:
            raise TimeoutError("no failback after 15 min")
        time.sleep(15)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    {"normal": normal, "failover": failover}[sys.argv[1] if len(sys.argv) > 1 else "normal"]()
