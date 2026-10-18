#!/usr/bin/env bash
# Deletes the whole multi-region deployment, in the only order that works:
#   1. global endpoint stack - its managed replication rules sit on both buses (a bus with
#      rules can't be deleted)
#   2. Cognito user pool replica -> INACTIVE (only inactive replicas can be deleted)
#   3. secondary stack (us-west-2) - references the replicas, owns the KMS replica key
#   4. primary stack (us-east-1) - global tables + replicas, user pool + replica, KMS key
# KMS keys are scheduled for deletion (7 days). Kept on purpose: the CI bootstrap stack (IAM
# roles are free, and the GitHub secrets stay valid for scripts/rebuild.sh) and the shared
# SAM artifact buckets. Pushes to main skip the deploy while the stacks don't exist.
# Usage: bash scripts/teardown.sh --yes
set -euo pipefail
cd "$(dirname "$0")/.."
[ "${1:-}" = "--yes" ] || { echo "This deletes all order data in both regions. Re-run with --yes."; exit 1; }
export AWS_PROFILE=default
STACK=orders-event-driven-app
PRIMARY=us-east-1
SECONDARY=us-west-2
SAM=${SAM:-sam}
PYTHON=${PYTHON:-.venv/Scripts/python}
[ -x "$PYTHON" ] || PYTHON=.venv/bin/python
step() { echo; echo "===== $(date +%H:%M:%S) $*"; }
exists() { aws cloudformation describe-stacks --region "$1" --stack-name "$2" >/dev/null 2>&1; }

aws sts get-caller-identity --query Account --output text | grep -qx 977534234172 \
  || { echo "wrong AWS account - check the default profile"; exit 1; }

step "1/4 global endpoint stack"
if exists $PRIMARY $STACK-global-endpoint; then
  aws cloudformation delete-stack --region $PRIMARY --stack-name $STACK-global-endpoint
  aws cloudformation wait stack-delete-complete --region $PRIMARY --stack-name $STACK-global-endpoint
fi

step "2/4 deactivate the user pool replica"
if exists $PRIMARY $STACK; then
  "$PYTHON" scripts/activate_user_pool_replica.py INACTIVE || echo "no active replica"
fi

step "3/4 secondary stack ($SECONDARY)"
exists $SECONDARY $STACK && "$SAM" delete --config-env dr --no-prompts

step "4/4 primary stack ($PRIMARY)"
exists $PRIMARY $STACK && "$SAM" delete --no-prompts

step "check"
for region in $PRIMARY $SECONDARY; do
  echo "$region stacks: $(aws cloudformation list-stacks --region $region --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
    --query "StackSummaries[?starts_with(StackName,'$STACK')].StackName" --output text)"
  echo "$region tables: $(aws dynamodb list-tables --region $region --query "TableNames[?starts_with(@,'$STACK')]" --output text)"
done
echo "KMS keys pending deletion (stop billing once deleted, 7 days):"
aws kms list-keys --region $PRIMARY --query 'Keys[].KeyId' --output text | tr '\t' '\n' | while read -r k; do
  aws kms describe-key --region $PRIMARY --key-id "$k" --query \
    "KeyMetadata.[KeyId,KeyState,Description]" --output text 2>/dev/null | grep -i "orders user pool" || true
done
