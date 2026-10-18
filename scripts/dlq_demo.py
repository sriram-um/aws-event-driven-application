"""Dead-letter queue demos against the deployed stack.

  poison  - sends one message the consumer can never process to each source queue (intake: no
            message attributes; fulfilment: not JSON) and times how long until SQS moves it to
            the DLQ after maxReceiveCount (3) failed receives. The poison messages are deleted
            from the DLQs at the end.
  outage  - simulates a FulfilOrder outage (reserved concurrency 0 = every invocation throttled),
            places real orders through the API, waits for them to land in the fulfilment DLQ,
            ends the outage, redrives the DLQ back to the source queue and checks that the
            orders are fulfilled. Concurrency is always restored (finally block).

Usage: python scripts/dlq_demo.py poison|outage [orders]
"""
import json
import sys
import time

from measure_fulfilment import outputs, put_order, session, token

sqs = session.client("sqs")
lam = session.client("lambda")


def queue_arn(url):
    return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]


def depth(url):
    attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=[
        "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
    return int(attrs["ApproximateNumberOfMessages"]), int(attrs["ApproximateNumberOfMessagesNotVisible"])


def wait_for(predicate, timeout, label, every=5):
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            elapsed = time.time() - start
            print(f"  {label}: after {elapsed:.0f} s")
            return elapsed
        time.sleep(every)
    raise TimeoutError(label)


def drain(url, match):
    """Receives and deletes messages for which match(msg) is true; returns them."""
    found = []
    for _ in range(10):
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=2,
                                   MessageSystemAttributeNames=["ApproximateReceiveCount", "SentTimestamp"]
                                   ).get("Messages", [])
        for m in msgs:
            if match(m):
                found.append(m)
                sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
            else:
                sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
        if not msgs:
            break
    return found


def poison():
    cases = [
        ("intake", outputs["OrderIntakeQueueUrl"], outputs["OrderIntakeDLQUrl"],
         {"MessageBody": json.dumps({"items": [{"itemId": "POISON", "quantity": 1, "unitPrice": 1}]})}),
        ("fulfilment", outputs["FulfilmentQueueUrl"], outputs["FulfilmentDLQUrl"],
         {"MessageBody": "POISON - not an EventBridge event"}),
    ]
    sent = {}
    for name, url, _, kwargs in cases:
        sent[name] = sqs.send_message(QueueUrl=url, **kwargs)["MessageId"]
        print(f"sent poison message to the {name} queue ({sent[name]})")
    start = time.time()
    for name, _, dlq, _ in cases:
        wait_for(lambda: depth(dlq)[0] >= 1, 600, f"{name}: message in DLQ")
    print(f"total {time.time() - start:.0f} s")
    for name, _, dlq, _ in cases:
        for m in drain(dlq, lambda m: m["MessageId"] == sent[name]):
            print(f"  {name} DLQ: messageId {m['MessageId']} (same id as sent), "
                  f"ApproximateReceiveCount={m['Attributes']['ApproximateReceiveCount']} -> deleted")


def outage(count=3):
    fn = outputs["FulfilOrderFunctionName"]
    fulfilments = session.resource("dynamodb").Table(outputs["FulfilmentsTableName"])
    dlq_url, source_url = outputs["FulfilmentDLQUrl"], outputs["FulfilmentQueueUrl"]
    try:
        lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=0)
        print(f"outage: {fn} reserved concurrency = 0 (every invocation throttled)")
        id_token = token()
        order_ids = [put_order(id_token)[0] for _ in range(count)]
        print(f"placed {count} orders: {', '.join(i[:8] for i in order_ids)}")
        wait_for(lambda: depth(dlq_url)[0] >= count, 900, f"{count} messages in the fulfilment DLQ", every=10)
        assert not any(fulfilments.get_item(Key={"orderId": i}).get("Item") for i in order_ids)
        print("  none of the orders is fulfilled")
    finally:
        lam.delete_function_concurrency(FunctionName=fn)
        print("outage over: reserved concurrency removed")

    # A move task only moves the *approximate* number of messages counted when it starts (one
    # run moved 2 of 3), so start another task until the DLQ is empty and the orders are done.
    dlq_arn = queue_arn(dlq_url)
    started = []

    def redriven():
        if all(fulfilments.get_item(Key={"orderId": i}).get("Item") for i in order_ids):
            return True
        running = started and sqs.list_message_move_tasks(
            SourceArn=dlq_arn, MaxResults=1)["Results"][0]["Status"] == "RUNNING"
        if not running and depth(dlq_url)[0] > 0:
            sqs.start_message_move_task(SourceArn=dlq_arn)  # destination = original source queue
            started.append(time.time())
            print(f"  redrive task {len(started)} started (DLQ depth {depth(dlq_url)[0]})")
        return False

    wait_for(redriven, 300, "all redriven orders fulfilled")
    tasks = sqs.list_message_move_tasks(SourceArn=dlq_arn, MaxResults=len(started))["Results"]
    print(f"  move tasks (status, moved): {[(t['Status'], t.get('ApproximateNumberOfMessagesMoved')) for t in tasks]}")
    print(f"  DLQ depth now (visible, in flight): {depth(dlq_url)}; source queue: {depth(source_url)}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    mode = sys.argv[1] if len(sys.argv) > 1 else "poison"
    poison() if mode == "poison" else outage(int(sys.argv[2]) if len(sys.argv) > 2 else 3)
