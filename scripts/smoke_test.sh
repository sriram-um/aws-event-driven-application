#!/usr/bin/env bash
# Post-deploy smoke test for one region's stack (used by CI for both regions).
#   1. PUT and GET /orders/{orderId} without a token must be rejected with 401. A new API
#      deployment can take ~1 min to go live (the stage serves the previous one meanwhile, and
#      an unknown route answers 403), so each check retries for up to 2 minutes.
#   2. The event-driven resources must be in the stack outputs.
# Usage: scripts/smoke_test.sh <region> [stack-name]
set -euo pipefail
REGION=$1
STACK=${2:-orders-event-driven-app}
output() {
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

UUID=$(uuidgen 2>/dev/null || python -c "import uuid; print(uuid.uuid4())")
ORDER_URL="$(output ApiUrl)/orders/$(echo "$UUID" | tr 'A-Z' 'a-z')"
for method in PUT GET; do
  for attempt in $(seq 12); do
    code=$(curl -s -o /dev/null -w "%{http_code}" -X "$method" "$ORDER_URL" \
      -H 'Content-Type: application/json' -d '{"items":[]}')
    echo "[$REGION] $method /orders/{orderId} without token -> HTTP $code (attempt $attempt)"
    [ "$code" = "401" ] && break
    sleep 10
  done
  test "$code" = "401"
done

for key in EventBusName OrderIntakeQueueUrl FulfilmentQueueUrl OrderNotificationsTopicArn DashboardUrl UserPoolId; do
  value=$(output "$key")
  echo "[$REGION] $key = $value"
  test -n "$value" && test "$value" != "None"
done
