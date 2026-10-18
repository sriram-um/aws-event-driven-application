"""Copies DynamoDB items between tables, keeping exact types (low-level DynamoDB JSON).

Used once in exercise 9: the regional Orders/Fulfilments tables are replaced by global tables
(CloudFormation can't change a table's type), so the items are exported before the stack update
and imported into the new tables afterwards.

  export <table> <file>   scan the table into a JSON-lines file
  import <table> <file>   batch-write the file's items into the table
  count  <table>          number of items (consistent scan)

Usage: python scripts/copy_tables.py export|import|count <table> [file]   (region us-east-1)
"""
import json
import sys
import time

import boto3

client = boto3.Session(profile_name="default", region_name="us-east-1").client("dynamodb")


def scan(table):
    kwargs = {"TableName": table, "ConsistentRead": True}
    while True:
        page = client.scan(**kwargs)
        yield from page["Items"]
        if "LastEvaluatedKey" not in page:
            return
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def export(table, path):
    with open(path, "w", encoding="utf-8") as f:
        n = sum(f.write(json.dumps(item) + "\n") > 0 for item in scan(table))
    print(f"exported {n} items from {table} to {path}")


def import_(table, path):
    items = [json.loads(line) for line in open(path, encoding="utf-8")]
    for i in range(0, len(items), 25):
        requests = [{"PutRequest": {"Item": item}} for item in items[i:i + 25]]
        while requests:  # retry anything DynamoDB couldn't process (throttling)
            resp = client.batch_write_item(RequestItems={table: requests})
            requests = resp.get("UnprocessedItems", {}).get(table, [])
            if requests:
                time.sleep(1)
    print(f"imported {len(items)} items into {table}")


if __name__ == "__main__":
    action, table = sys.argv[1], sys.argv[2]
    if action == "export":
        export(table, sys.argv[3])
    elif action == "import":
        import_(table, sys.argv[3])
    else:
        print(f"{table}: {sum(1 for _ in scan(table))} items")
