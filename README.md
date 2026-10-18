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
| 1 | Publish events | EventBridge | ⬜ |
| 2 | Process messages | SQS, Lambda, DynamoDB | ⬜ |
| 3 | Send notifications | SNS | ⬜ |
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
             │ Cognito authorizer      │      └────────────────────────────┘      │ on-demand · SSE · PITR    │
             │ throttle 100 rps/50 burst│                                         └───────────────────────────┘
             └─────────────────────────┘
```

**Request path:** API Gateway (Cognito authorizer checks the JWT) → CreateOrder Lambda (validates
the payload, takes `customerId` from the token's `sub` claim) → one DynamoDB `TransactWriteItems`
that writes the order header and every line together, or nothing.

**Observability:** X-Ray active tracing on the API stage and the Lambda; structured JSON logs
(Powertools `Logger`, correlation id = API Gateway request id, `app_version` key).

## Data model

`orders-event-driven-app-Orders` — one partition per order:

| Item | `orderId` (PK) | `sk` (SK) | Attributes |
|---|---|---|---|
| Header | UUID v4 | `ORDER` | `customerId` (Cognito `sub`), `status` (`CREATED`), `total` (N), `itemCount`, `createdAt` (ISO-8601 UTC) |
| Line | same | `ITEM#<itemId>` | `itemId`, `productName`, `quantity` (N), `unitPrice` (N) |

A `Query` on `orderId` returns the whole order. The header item gives later exercises one place
for order-level state (status, idempotency/processing state) without changing the table schema.

## Repository layout

```
template.yaml                 SAM template (all application infrastructure)
samconfig.toml                Deploy config: "default" env (local, pinned profile) and "ci" env
src/create_order/app.py       CreateOrder Lambda handler
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
