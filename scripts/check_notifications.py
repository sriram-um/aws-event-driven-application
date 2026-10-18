"""Sends N orders and reads the SNS notifications back from the test subscriber queue.

Expects one OrderCreated and one OrderFulfilled notification per order and reports the delay
from the order being saved to the notification landing in the queue.

Usage: python scripts/check_notifications.py [count]
"""
import json
import statistics
import sys
import time

from measure_fulfilment import ms, outputs, put_order, session, token


def main(count=3):
    sqs = session.client("sqs")
    queue_url = outputs["NotificationsTestQueueUrl"]
    orders_table = session.resource("dynamodb").Table(outputs["OrdersTableName"])

    id_token = token()
    order_ids = {put_order(id_token)[0] for _ in range(count)}

    received, deadline = {}, time.time() + 60
    while len(received) < 2 * count and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5,
                                   MessageSystemAttributeNames=["SentTimestamp"])
        for msg in resp.get("Messages", []):
            body = json.loads(msg["Body"])
            if body["orderId"] in order_ids:
                received[(body["orderId"], body["type"])] = (body, int(msg["Attributes"]["SentTimestamp"]))
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    delays = {"OrderCreated": [], "OrderFulfilled": []}
    for (order_id, kind), (body, sent_ms) in received.items():
        created = orders_table.get_item(Key={"orderId": order_id, "sk": "ORDER"})["Item"]["createdAt"]
        delays[kind].append(sent_ms - ms(created))

    print(f"{len(received)}/{2 * count} notifications received")
    for kind, values in delays.items():
        if values:
            print(f"order saved -> {kind} notification in queue: "
                  f"min {min(values):.0f} / median {statistics.median(values):.0f} / max {max(values):.0f} ms")
    for kind in delays:
        sample = next((b for (_, k), (b, _) in received.items() if k == kind), None)
        print(f"\nsample {kind}:\n{json.dumps(sample, indent=2)}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 3)
