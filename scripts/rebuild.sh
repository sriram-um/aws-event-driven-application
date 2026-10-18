#!/usr/bin/env bash
# Rebuilds the whole multi-region deployment from scratch (after scripts/teardown.sh).
# Every id that changes between deployments (KMS key, user pool, app client, global endpoint)
# is read from stack outputs - nothing is hard-coded. Order matters:
#   1. primary stack (us-east-1): global tables + replicas, user pool, its multi-Region KMS key
#      (user pool replica off: Cognito needs the key's replica in us-west-2 first)
#   2. KMS key replica stack (us-west-2)
#   3. primary stack again with the user pool replica, then activate it (created INACTIVE)
#   4. secondary stack (us-west-2): authorizer on the replica pool
#   5. global endpoint stack (needs the bus in both regions), then both app stacks again,
#      now publishing through the endpoint
#   6. test users (created in the primary, replicated) + local credential files
#   7. smoke test both regions
# Prerequisites: the CI bootstrap stack can stay deployed (IAM is free); Docker not needed.
# Usage: bash scripts/rebuild.sh        (~25-30 min, mostly CloudFormation waits)
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PROFILE=default   # the shell may point at another account (playbook)
STACK=orders-event-driven-app
PRIMARY=us-east-1
SECONDARY=us-west-2
SAM=${SAM:-sam}
PYTHON=${PYTHON:-.venv/Scripts/python}
[ -x "$PYTHON" ] || PYTHON=.venv/bin/python

out() {  # out <region> <stack> <output key>
  aws cloudformation describe-stacks --region "$1" --stack-name "$2" \
    --query "Stacks[0].Outputs[?OutputKey=='$3'].OutputValue" --output text
}
step() { echo; echo "===== $(date +%H:%M:%S) $*"; }

aws sts get-caller-identity --query Account --output text | grep -qx 977534234172 \
  || { echo "wrong AWS account - check the default profile"; exit 1; }

step "build (awscrt layer + SAM)"
PYTHON=$PYTHON bash scripts/build_crt_layer.sh
"$SAM" validate --lint
"$SAM" build

step "1/7 primary stack ($PRIMARY) - user pool replica off, no global endpoint yet"
"$SAM" deploy --no-fail-on-empty-changeset --parameter-overrides EnableUserPoolReplica=false

KEY_ARN=$(out $PRIMARY $STACK UserPoolKeyArn)
POOL_ID=$(out $PRIMARY $STACK UserPoolId)
CLIENT_ID=$(out $PRIMARY $STACK UserPoolClientId)
echo "user pool $POOL_ID, client $CLIENT_ID, key $KEY_ARN"

step "2/7 KMS key replica in $SECONDARY (Cognito needs it before the user pool replica)"
aws cloudformation deploy --region $SECONDARY --template-file infra/user-pool-key-replica.yaml \
  --stack-name $STACK-user-pool-key-replica --parameter-overrides PrimaryKeyArn="$KEY_ARN" \
  --tags project=event-driven-orders --no-fail-on-empty-changeset

step "3/7 primary stack - create the user pool replica, then activate it"
"$SAM" deploy --no-fail-on-empty-changeset --parameter-overrides EnableUserPoolReplica=true
"$PYTHON" scripts/activate_user_pool_replica.py ACTIVE

step "4/7 secondary stack ($SECONDARY) - authorizer on the replica pool"
SECONDARY_PARAMS="PrimaryUserPoolId=$POOL_ID PrimaryUserPoolClientId=$CLIENT_ID"
"$SAM" deploy --config-env dr --no-fail-on-empty-changeset --parameter-overrides $SECONDARY_PARAMS

step "5/7 global endpoint stack ($PRIMARY), then both app stacks publish through it"
aws cloudformation deploy --region $PRIMARY --template-file infra/global-endpoint.yaml \
  --stack-name $STACK-global-endpoint --capabilities CAPABILITY_NAMED_IAM \
  --tags project=event-driven-orders --no-fail-on-empty-changeset
ENDPOINT_ID=$(out $PRIMARY $STACK-global-endpoint EndpointId)
echo "global endpoint $ENDPOINT_ID"
"$SAM" deploy --no-fail-on-empty-changeset --parameter-overrides GlobalEndpointId=$ENDPOINT_ID EnableUserPoolReplica=true
"$SAM" deploy --config-env dr --no-fail-on-empty-changeset \
  --parameter-overrides GlobalEndpointId=$ENDPOINT_ID $SECONDARY_PARAMS

step "6/7 test users + local credential files (outside the repo)"
PW1="Tst-$("$PYTHON" -c 'import secrets;print(secrets.token_hex(8))')-Aa1!"
PW2="Tst-$("$PYTHON" -c 'import secrets;print(secrets.token_hex(8))')-Bb2!"
for pair in "testuser@example.com:$PW1" "testuser2@example.com:$PW2"; do
  user=${pair%%:*}; pw=${pair#*:}
  aws cognito-idp admin-create-user --region $PRIMARY --user-pool-id "$POOL_ID" --username "$user" \
    --user-attributes Name=email,Value="$user" Name=email_verified,Value=true \
    --message-action SUPPRESS >/dev/null 2>&1 || echo "$user already exists - resetting password"
  aws cognito-idp admin-set-user-password --region $PRIMARY --user-pool-id "$POOL_ID" \
    --username "$user" --password "$pw" --permanent
done
umask 077
for region in $PRIMARY $SECONDARY; do
  file=~/.orders-event-driven-test-user.env
  [ $region = $PRIMARY ] || file=~/.orders-event-driven-test-user-$region.env
  printf 'USER_POOL_ID=%s\nCLIENT_ID=%s\nAPI_URL=%s\nTEST_USER=%s\nTEST_PASSWORD=%s\nTEST_USER2=%s\nTEST_PASSWORD2=%s\n' \
    "$POOL_ID" "$CLIENT_ID" "$(out $region $STACK ApiUrl)" testuser@example.com "$PW1" testuser2@example.com "$PW2" > "$file"
  echo "wrote $file"
done

step "7/7 smoke tests"
PATH="$(dirname "$PYTHON"):$PATH" bash scripts/smoke_test.sh $PRIMARY
PATH="$(dirname "$PYTHON"):$PATH" bash scripts/smoke_test.sh $SECONDARY
echo
echo "Rebuilt. End-to-end check: $PYTHON scripts/dr_test.py normal"
