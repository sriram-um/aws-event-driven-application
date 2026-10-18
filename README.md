# Event-Driven Orders Application (AWS SAM)

An orders backend that grows, exercise by exercise, from a synchronous REST API into an
event-driven system: Amazon EventBridge, SQS, SNS, CloudWatch, an asynchronous API, dead-letter
queues, idempotent processing and multi-region disaster recovery with EventBridge Global
Endpoints. Everything is defined in a single [AWS SAM](https://docs.aws.amazon.com/serverless-application-model/)
template and deployed as updates to one CloudFormation stack (`orders-event-driven-app`).

| | |
|---|---|
| **IaC** | AWS SAM (CloudFormation transform) — one template, one stack per region |
| **Runtime** | Python 3.12 on arm64 (Graviton) |
| **Regions** | us-east-1 (primary), us-west-2 (secondary, exercise 9) |

## Progress

| # | Exercise | Services | Status |
|---|---|---|---|
| 0 | Baseline orders API | API Gateway, Lambda, DynamoDB, Cognito | ✅ Done |
| 1 | Publish events | EventBridge | ✅ Done |
| 2 | Process messages | SQS, Lambda, DynamoDB | ✅ Done |
| 3 | Send notifications | SNS | ✅ Done |
| 4 | Monitoring | CloudWatch dashboard | ⬜ |
| 5 | CI/CD | GitHub Actions (OIDC) | ⬜ |
| 6 | Asynchronous API | API Gateway → SQS | ⬜ |
| 7 | Dead-letter queues | SQS | ⬜ |
| 8 | Idempotency | Lambda, DynamoDB conditional writes | ⬜ |
| 9 | Disaster recovery | EventBridge Global Endpoints | ⬜ |
| 10 | Cost estimate | AWS Pricing Calculator | ⬜ |

## Architecture (current)

```
                 ┌──────────────────────┐
                 │ Cognito User Pool    │ 1. sign in → ID token (JWT)
        ┌───────►│ admin-created users  │
        │        └──────────┬───────────┘
        │                   │ validates JWT
 client ┴──► ┌──────────────▼──────────┐      ┌────────────────────────────┐      ┌───────────────────────────┐
 HTTPS       │ API Gateway (REST)      │─────►│ Lambda: CreateOrder        │─────►│ DynamoDB: Orders          │
 Authorization│ stage prod · REGIONAL  │proxy │ python3.12 / arm64 / 256MB │trans-│ PK orderId · SK sk        │
 : <IdToken> │ POST /orders            │      │ validates, Powertools logs │action│ ORDER header + ITEM# lines│
             │ Cognito authorizer      │      └─────────────┬──────────────┘      │ on-demand · SSE · PITR    │
             │ throttle 100 rps/50 burst│                    │ PutEvents          └───────────────────────────┘
             └─────────────────────────┘                    │ (after the write succeeds)
                                              ┌─────────────▼──────────────┐      ┌───────────────────────────┐
                                              │ EventBridge custom bus     │rule  │ CloudWatch Logs           │
                                     ┌───────►│ orders-event-driven-app-bus│─────►│ /aws/events/<stack>       │
                                     │        │ source orders.service      │all   │ copy of every order event │
                                     │        │ OrderCreated/OrderFulfilled│events│ (14-day retention)        │
                                     │        └──────┬──────┬──────────────┘      └───────────────────────────┘
                                     │               │      │ rule: detail-type = OrderCreated
                                     │               │      │
                                     │   rule: OrderCreated + OrderFulfilled, InputTransformer
                                     │               │      │      ┌────────────────────────┐  raw   ┌─────────────────────┐
                                     │               └──────┼─────►│ SNS: order-            │───────►│ SQS test subscriber │
                                     │                      │      │ notifications          │delivery│ <stack>-            │
                                     │                      │      │ policy: only the rule  │        │ notifications-test  │
                                     │                      │      └────────────────────────┘        └─────────────────────┘
                                     │        ┌─────────────▼──────────────┐
                                     │        │ SQS: <stack>-fulfilment    │ SSE-SQS · visibility 60 s
                                     │        │ queue policy: only the rule│ (6× function timeout)
                                     │        └─────────────┬──────────────┘
                                     │                      │ event source mapping: batch 10,
                                     │                      │ ReportBatchItemFailures
                                     │        ┌─────────────▼──────────────┐      ┌───────────────────────────┐
                                     │        │ Lambda: FulfilOrder        │─────►│ DynamoDB: Fulfilments     │
                                     └────────│ Powertools BatchProcessor  │ Put  │ PK orderId · warehouse,   │
                              PutEvents       │ python3.12 / arm64 / 256MB │      │ status, fulfilledAt       │
                              OrderFulfilled  └────────────────────────────┘      └───────────────────────────┘
```

**Request path:** API Gateway (Cognito authorizer checks the JWT) → CreateOrder Lambda (validates
the payload, takes `customerId` from the token's `sub` claim) → one DynamoDB `TransactWriteItems`
that writes the order header and every line together, or nothing → `PutEvents` of an
`OrderCreated` event to the custom bus. A rule copies every `orders.service` event to CloudWatch
Logs for inspection.

**Fulfilment path (asynchronous):** a second rule routes `OrderCreated` to the SQS fulfilment
queue → FulfilOrder (event source mapping) records the fulfilment (simulated warehouse, chosen by
a hash of `orderId`) in the Fulfilments table → publishes `OrderFulfilled` back to the bus.
The API response doesn't wait for any of this.

**Notification path:** a third rule matches `OrderCreated` and `OrderFulfilled` and publishes a
short notification (built by an EventBridge **input transformer**) to the SNS topic. An SQS
queue subscribed with raw message delivery stands in for real subscribers (email, SMS,
webhooks, other services) so delivery can be checked from the CLI.

**Observability:** X-Ray active tracing on the API stage and the Lambda; structured JSON logs
(Powertools `Logger`, correlation id = API Gateway request id, `app_version` key).

## Data model

`orders-event-driven-app-Orders` — one partition per order:

| Item | `orderId` (PK) | `sk` (SK) | Attributes |
|---|---|---|---|
| Header | UUID v4 | `ORDER` | `customerId` (Cognito `sub`), `status` (`CREATED`), `total` (N), `itemCount`, `createdAt` (ISO-8601 UTC) |
| Line | same | `ITEM#<itemId>` | `itemId`, `productName`, `quantity` (N), `unitPrice` (N) |

### Event: `OrderCreated`

`source: orders.service`, `detail-type: OrderCreated`, bus `orders-event-driven-app-bus`.
Money values are strings so consumers never lose precision to floats.

```json
{
  "orderId": "adf157f7-011f-416e-974b-9c900f9d5299",
  "customerId": "843874a8-4081-70c7-f151-d3c8ab4c97c2",
  "items": [{"itemId": "SKU-1001", "productName": "Wireless Mouse", "quantity": 2, "unitPrice": "24.99"}],
  "itemCount": 1,
  "total": "49.98",
  "createdAt": "2026-10-18T14:08:48.471944+00:00"
}
```

### Event: `OrderFulfilled`

```json
{"orderId": "…", "customerId": "…", "warehouse": "PDX-1", "itemCount": 2, "total": "99.48",
 "orderCreatedAt": "2026-10-18T14:20:19.072+00:00", "fulfilledAt": "2026-10-18T14:20:20.419+00:00"}
```

### Notification (SNS message)

Built by the input transformer — subscribers get a small, stable message instead of the full
order with items and prices:

```json
{
  "type": "OrderFulfilled",
  "orderId": "e910267c-7708-4ce0-b3f9-5ea341d559b8",
  "customerId": "843874a8-4081-70c7-f151-d3c8ab4c97c2",
  "total": "1",
  "warehouse": "CMH-1",
  "time": "2026-10-18T14:30:57Z",
  "message": "OrderFulfilled: order e910267c-7708-4ce0-b3f9-5ea341d559b8 (total 1)"
}
```

`warehouse` is `""` for `OrderCreated` (the path doesn't exist in that event).

### Table: `orders-event-driven-app-Fulfilments`

| Attribute | Notes |
|---|---|
| `orderId` (PK) | Same id as the order |
| `status` | `FULFILLED` |
| `warehouse` | `IAD-1` / `CMH-1` / `PDX-1` (simulated; stable per order) |
| `customerId`, `itemCount`, `total` | Copied from the event (`total` kept as a string) |
| `orderCreatedAt`, `fulfilledAt` | ISO-8601 UTC — the difference is the end-to-end async latency |
| `sourceEventId` | EventBridge event id that produced the fulfilment (traceability) |

A `Query` on `orderId` returns the whole order. The header item gives later exercises one place
for order-level state (status, idempotency/processing state) without changing the table schema.

## Repository layout

```
template.yaml                 SAM template (all application infrastructure)
samconfig.toml                Deploy config: "default" env (local, pinned profile) and "ci" env
src/create_order/app.py       CreateOrder Lambda handler (API → DynamoDB → EventBridge)
src/fulfil_order/app.py       FulfilOrder Lambda handler (SQS → DynamoDB → EventBridge)
scripts/measure_fulfilment.py Sends N orders and breaks down the async fulfilment latency
scripts/check_notifications.py Sends N orders and reads the SNS notifications from the test queue
events/create_order.json      Sample API Gateway-style event
tests/unit/                   pytest + moto unit tests
requirements-dev.txt          Test dependencies
```

## Getting started

**Prerequisites:** AWS CLI v2 (configured profile), AWS SAM CLI, Python 3.12.

```bash
# Unit tests
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt   # Windows (.venv/bin on macOS/Linux)
.venv/Scripts/python -m pytest -q

# Build and deploy (settings come from samconfig.toml)
sam validate --lint
sam build
sam deploy
```

### Calling the API

Self sign-up is disabled, so create a user as an admin first (IDs from the stack outputs
`UserPoolId` and `UserPoolClientId`):

```bash
aws cognito-idp admin-create-user --user-pool-id $USER_POOL_ID --username you@example.com \
  --user-attributes Name=email,Value=you@example.com Name=email_verified,Value=true \
  --message-action SUPPRESS
aws cognito-idp admin-set-user-password --user-pool-id $USER_POOL_ID --username you@example.com \
  --password '<strong-password>' --permanent

TOKEN=$(aws cognito-idp initiate-auth --auth-flow USER_PASSWORD_AUTH --client-id $CLIENT_ID \
  --auth-parameters USERNAME=you@example.com,PASSWORD='<strong-password>' \
  --query AuthenticationResult.IdToken --output text)

curl -X POST "$API_URL/orders" -H "Authorization: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"items":[{"itemId":"SKU-1001","productName":"Wireless Mouse","quantity":2,"unitPrice":24.99}]}'
```

| Response | When |
|---|---|
| `201 {"orderId", "itemCount", "total"}` | Order saved |
| `400 {"message"}` | Invalid JSON / payload (empty or >25 items, duplicate `itemId`, bad quantity/price) |
| `401` | Missing or invalid ID token |
| `429` | API Gateway throttling (100 rps, burst 50) |

## Design decisions

- **New, slim stack instead of extending Project 3** — the event-driven exercises only need the
  core order write path; DAX, WAF and Global Tables from Project 3 would add cost and complicate
  the multi-region exercise.
- **Header + line items with a single-table key (`sk`)** — chosen up front so later exercises
  (order status, idempotency state) don't force a table replacement.
- **`TransactWriteItems` instead of `BatchWriteItem`** — atomic: a failure can't leave a partial
  order. The header put is conditional (`attribute_not_exists`), the starting point for the
  idempotency exercise.
- **Custom event bus, not the default bus** — keeps application events apart from AWS service
  events, gives a resource to scope IAM (`EventBridgePutEventsPolicy` on this bus only) and is
  what EventBridge Global Endpoints need in exercise 9.
- **Fat event (full order in `detail`)** — consumers (fulfilment, notifications) don't have to
  call back into the Orders table; the trade-off is a larger event and a contract to keep stable.
- **Publish after the write, never fail the request on a publish error** — the order is already
  saved, so returning 5xx would make the client retry and create a *duplicate* order. Failures are
  logged (`OrderCreated publish failed`) instead. This is the classic **dual-write problem**:
  a crash between the DynamoDB write and `PutEvents` loses the event. Exercise 8 revisits it;
  the alternatives are DynamoDB Streams → EventBridge Pipes (transactional outbox) or retrying
  from a processing state on the order header.
- **EventBridge → SQS → Lambda instead of EventBridge → Lambda directly** — the queue buffers
  bursts, controls consumer concurrency, keeps messages for 4 days if the consumer is broken, and
  gives a place for a dead-letter queue (exercise 7).
- **Partial batch responses (`ReportBatchItemFailures` + Powertools `BatchProcessor`)** — one bad
  message in a batch of 10 doesn't make the 9 good ones be processed again. If *every* record
  fails, Powertools raises `BatchProcessingError` so the invocation shows up as a Lambda error.
- **FulfilOrder fails the record if `OrderFulfilled` can't be published** — the opposite choice to
  CreateOrder: here a retry is safe (the message goes back to the queue and the put is
  overwritten), so redelivery is preferred over a silently lost event.
- **Queue policy scoped with `aws:SourceArn` = the rule ARN** — only this rule can send to the
  queue (confused-deputy protection). Queue encrypted with SSE-SQS (no KMS key cost).
- **`MaximumBatchingWindowInSeconds: 0`** — measured: a 1 s window delayed fulfilment by up to
  20 s at low traffic (see below).
- **EventBridge rule → SNS (no Lambda)** — EventBridge routes and reshapes the event itself
  with an input transformer, so notifications need no code, and SNS fans out to any number of
  subscribers.
- **Input transformer instead of passing the whole event** — decouples subscribers from the
  internal event schema and keeps item/price data out of notifications.
- **Topic not encrypted** — SNS encrypted with the AWS-managed `aws/sns` key can't receive from
  EventBridge (the managed key policy can't grant `events.amazonaws.com`); a customer-managed
  KMS key would work for ~$1/month. Messages carry ids and totals only.
- **SQS test subscriber with raw message delivery** — scriptable verification without an
  inbox; raw delivery passes the notification JSON as-is instead of in an SNS envelope.
- **Log-group rule as a debug/audit trail** — makes every event visible (and queryable with Logs
  Insights) before any consumer exists. Log targets must be under `/aws/events/` and need a
  CloudWatch Logs **resource policy** allowing `events.amazonaws.com`.
- **Cognito authorizer as the default for every route**; `customerId` comes from the verified
  token, never from the body.
- **Nothing bundled** — boto3 comes from the runtime and Powertools from the pinned public layer
  (deployment package: 2.3 KB).

## Observations and lessons learned

### Exercise 0 — baseline (measured 2026-10-18, us-east-1)

| Measurement | Value |
|---|---|
| Cold start: Init Duration / Duration | 716 ms / 178 ms |
| Warm Duration (TransactWriteItems, 2 lines) | 23–27 ms |
| Rejected payload (400, no DynamoDB call) | 2.6 ms |
| End-to-end `curl` latency, warm | 0.16–0.24 s (cold: 1.6 s) |
| Max memory used | 104 MB of 256 MB |
| Deployment package size | 2,264 bytes |

- No token → `401` from the Cognito authorizer; invalid payload → `400` with the reason.
- The Init Duration (~0.7 s) is mostly importing the Powertools Logger/Tracer and the X-Ray SDK,
  the same trade-off seen in Project 3.

### Exercise 1 — EventBridge (measured 2026-10-18, us-east-1)

| Measurement | Before (ex. 0) | After |
|---|---|---|
| Warm Lambda Duration | 23–27 ms | 36–44 ms (+~15 ms for `PutEvents`) |
| Cold start: Init / Duration | 716 / 178 ms | 699 / 271 ms (first `events` client call) |
| End-to-end `curl` latency, warm | 0.16–0.24 s | 0.17–0.20 s (no visible change) |
| Order written → event ingested in CloudWatch Logs | — | 350–590 ms |

- 6 valid orders → 6 `OrderCreated` events in the log group, 0 publish failures; a rejected order
  (400) publishes nothing.
- The event's `time` field has **1-second precision**, so it can be up to ~1 s *earlier* than
  `createdAt` — use `detail.createdAt` (or the log ingestion time) for latency measurements.
- Rule metrics (`AWS/Events` `MatchedEvents`, `Invocations`, dims `EventBusName` + `RuleName`)
  arrive with a few minutes' delay — the first query showed 5 matched vs 6 invocations;
  a few minutes later both were 6.

### Exercise 2 — SQS + FulfilOrder (measured 2026-10-18, us-east-1)

Measured with `scripts/measure_fulfilment.py` (orders through the real API; timestamps from the
order header, SQS `SentTimestamp` / `ApproximateFirstReceiveTimestamp`, and `fulfilledAt`):

| Stage (ms, min / median / max) | Batching window 1 s | Batching window 0 (final) | Burst, window 0 |
|---|---|---|---|
| Order saved → message in SQS (EventBridge delivery) | 75 / 88 / 300 | 76 / 87 / 299 | 75 / 87 / 224 |
| Waiting in SQS before first receive | 1 / 1 / 2 | 1 / 1 / 34 | 1 / 1 / 2 |
| Received → fulfilment written | **1,385 / 9,345 / 20,450** | 39 / 57 / 76 | 34 / 38 / 109 |
| **Total (order saved → fulfilled)** | **1,498 / 9,546 / 20,526** | **121 / 147 / 409** | **114 / 130 / 272** |

10 orders 2 s apart; burst = 20 orders back to back.

- **The batching window was the whole delay.** With `MaximumBatchingWindowInSeconds: 1`, SQS
  handed each message to the Lambda poller within 1–2 ms, but the function was invoked up to
  20 s later — the poller kept collecting for a batch instead of invoking. The ~20 s ceiling
  matches the SQS long-poll wait, so at low traffic the "1 s" window behaved more like "up to
  20 s". With the window at 0, fulfilment finishes in ~150 ms. Use a batching window only when
  throughput/cost per invocation matters more than latency.
- With the window at 0 every invocation carried **1 record**, even in the burst — the API
  produces orders ~170 ms apart, slower than the pollers drain them. (With the 1 s window,
  batches were 1–4 records.)
- FulfilOrder: cold start Init 639–780 ms; warm Duration 24–41 ms per record (DynamoDB put +
  `PutEvents`).
- 50 orders processed across the runs, **0 failures**, queue drained to 0 every time, and every
  `OrderCreated` has a matching `OrderFulfilled` in the event log group.
- **Delivery is at-least-once**: today a duplicate message would rewrite the same fulfilment
  (harmless) but publish a second `OrderFulfilled`. Exercise 8 makes FulfilOrder idempotent.
- A consumer failing *all* records makes Powertools raise (whole batch retried) — caught by a
  unit test that initially expected a partial-failure response.

### Exercise 3 — SNS notifications (measured 2026-10-18, us-east-1)

`scripts/check_notifications.py 5` (5 orders through the API):

| Measurement | min / median / max |
|---|---|
| Order saved → `OrderCreated` notification in the subscriber queue | 103 / 128 / 378 ms |
| Order saved → `OrderFulfilled` notification in the subscriber queue | 213 / 254 / 805 ms |

- 10/10 notifications received (one `OrderCreated` + one `OrderFulfilled` per order).
- Metrics agree end to end: rule `MatchedEvents` 10 = `Invocations` 10, no
  `FailedInvocations`; SNS `NumberOfMessagesPublished` 10 = `NumberOfNotificationsDelivered` 10,
  `NumberOfNotificationsFailed` 0.
- **Input transformer: a path missing from the event becomes an empty string** inside a quoted
  template placeholder (`"warehouse": ""` on `OrderCreated`) — no error, so check templates
  against every event type the rule matches.
- Notifications are not ordered: SNS (standard) and EventBridge give no ordering guarantee, so a
  subscriber could see `OrderFulfilled` before `OrderCreated` under load — consumers should rely
  on `type`/`time`, not arrival order. (A FIFO topic would only help for FIFO-capable targets.)
