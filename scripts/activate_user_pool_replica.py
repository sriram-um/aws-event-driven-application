"""Activates the Cognito user pool replica in the secondary region (exercise 9).

AWS::Cognito::UserPoolReplica creates the replica INACTIVE and has no status property, so this
one-off step switches it to ACTIVE (sign-in and token operations then work in the secondary
region too). Prints the replicas and whether the app client exists in the replica.

Usage: python scripts/activate_user_pool_replica.py [ACTIVE|INACTIVE]
"""
import sys
import time

import boto3

STACK, PRIMARY, SECONDARY = "orders-event-driven-app", "us-east-1", "us-west-2"
session = boto3.Session(profile_name="default")
outputs = {o["OutputKey"]: o["OutputValue"] for o in session.client("cloudformation", region_name=PRIMARY)
           .describe_stacks(StackName=STACK)["Stacks"][0]["Outputs"]}
pool, client = outputs["UserPoolId"], outputs["UserPoolClientId"]
cognito = session.client("cognito-idp", region_name=PRIMARY)


def replicas():
    return cognito.list_user_pool_replicas(UserPoolId=pool)["UserPoolReplicas"]


status = sys.argv[1] if len(sys.argv) > 1 else "ACTIVE"
cognito.update_user_pool_replica(UserPoolId=pool, RegionName=SECONDARY, Status=status)
for _ in range(60):
    current = {r["RegionName"]: (r["Role"], r["Status"]) for r in replicas()}
    if current.get(SECONDARY, (None, None))[1] == status:
        break
    time.sleep(5)
print("replicas:", current)
replica_client = session.client("cognito-idp", region_name=SECONDARY).describe_user_pool_client(
    UserPoolId=pool, ClientId=client)["UserPoolClient"]
print(f"app client {client} in {SECONDARY} replica: {replica_client['ClientName']}")
