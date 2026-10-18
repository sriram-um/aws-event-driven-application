# Cost estimate (exercise 10)

Monthly cost of the architecture as deployed: the full stack in **us-east-1 and us-west-2**, the
EventBridge global endpoint, DynamoDB global tables and Cognito multi-Region replication. Prices
are on-demand list prices **from the AWS Price List API on 2026-10-18** (marked ✓; identical in
both regions) or from the public pricing pages (marked ◦). Like Project 3, the 12-month free tier
is **ignored**. The permanent free allowances that don't expire (Cognito's 10,000 MAU,
DynamoDB's 25 GB of storage) are counted.

## Assumptions

| Driver | Value | Notes |
|---|---|---|
| Orders placed (`PUT /orders/{id}`) | 100,000 / month | 2 lines per order |
| Status reads (`GET /orders/{id}`) | 300,000 / month | a client polls about 3 times per order |
| Monthly active users | 5,000 | Cognito Essentials plan (required for multi-Region replication) |
| Region split | All traffic in us-east-1; us-west-2 idle, as the warm standby | The secondary still receives every replicated write and event |
| Events per order | `OrderCreated` + `OrderFulfilled` = 2, each replicated to the other bus | 2 notifications per order |
| Lambda | arm64, 256 MB; CreateOrder ~60 ms, FulfilOrder ~40 ms | Warm durations measured in the README, rounded up |
| Item size | < 1 KB | 1 write unit per item; a status read is 2 items × 0.5 RRU (eventually consistent) |
| Average traffic | ~0.15 requests/s | X-Ray's 1 req/s reservoir samples nearly every request |

## Scenario A: 100,000 orders / month

| Service | Calculation | Monthly |
|---|---|---|
| **API Gateway** (REST) | 400 k requests × $3.50/M ✓ | $1.40 |
| **SQS** requests | Lambda polls 4 queues (intake + fulfilment × 2 regions) with long polling, ~0.66 M empty receives per queue ◦ = 2.6 M; plus 100 k orders × (send + receive + delete) × 2 queues = 0.6 M; plus 0.2 M SNS → test-queue deliveries. 3.4 M × $0.40/M ✓ | $1.36 |
| **Lambda** | 200 k invocations × $0.20/M ✓ = $0.04; CreateOrder 100 k × 0.25 GB × 60 ms + FulfilOrder 100 k × 0.25 GB × 40 ms = 2,500 GB‑s × $0.0000133334 ✓ = $0.03 | $0.07 |
| **DynamoDB** writes | Per order: transaction of header + 2 lines = 3 items × 2 (transactions cost double) + completion update 1 = 7 WRU; fulfilment put + completion = 2 WRU. 900 k WRU × $0.625/M ✓ | $0.56 |
| **DynamoDB** replicated writes (to us-west-2) | 6 items written per order are replicated (transactions replicate at the normal rate). 600 k rWRU × $0.625/M ✓ | $0.38 |
| **DynamoDB** reads | 300 k status reads × 2 items × 0.5 RRU = 300 k RRU × $0.125/M ✓ | $0.04 |
| **DynamoDB** storage + PITR, 2 regions | ~0.2 GB growth/month: storage within the 25 GB free; PITR 0.2 GB × $0.20 ✓ × 2 | $0.08 |
| **EventBridge** | 200 k custom events × $1.00/M ✓, plus 200 k replicated events received by the other bus × $1.00/M; delivery to SQS/SNS/Logs targets is free ◦; the global endpoint has no charge ◦ | $0.40 |
| **SNS** | 200 k publishes × $0.50/M ✓ (delivery to SQS free ◦) | $0.10 |
| **Cognito** Essentials | 5,000 MAU, within the 10,000 MAU that don't expire ◦ ($0.015/MAU above that) | $0.00 |
| **Cognito** multi-Region replication | 5,000 MAU × $0.0045 ✓, **no free tier**; every user is counted once globally | **$22.50** |
| **KMS** multi-Region key + replica | 2 keys × $1 ✓ (requests: a few thousand × $0.03/10 k ✓) | **$2.00** |
| **Route 53** health check | CloudWatch-alarm check, priced as an AWS-endpoint check: $0.50 ✓ beyond the first 50 per account | $0.00–0.50 |
| **CloudWatch** alarms | 3 DLQ alarms × 2 regions + 1 EventBridge health alarm = 7 × $0.10 ✓ | $0.70 |
| **CloudWatch** dashboards | 2 (one per region) × $3 ◦ | **$6.00** |
| **CloudWatch** API Gateway detailed metrics | `MetricsEnabled` on all methods: ~7 metrics × 2 methods × 2 regions = 28 custom metrics × $0.30 ✓ | **$8.40** |
| **CloudWatch** Logs | ~1.5 KB per Lambda invocation (0.3 GB) + every event logged in both regions (400 k × 1 KB = 0.4 GB) → 0.7 GB × $0.50 ✓ | $0.35 |
| **X-Ray** | Nearly all of ~400 k API requests and their async segments are sampled; ~$5 per million traces ◦ | ~$2.50 |
| Inter-region data transfer | DynamoDB + EventBridge replication, ~0.4 GB × $0.02/GB ◦ | < $0.01 |
| **Total** | | **≈ $47 / month** |

**What drives the bill**

| Group | Monthly | Share |
|---|---|---|
| Usage: API Gateway, SQS, Lambda, DynamoDB, EventBridge, SNS, logs | ≈ $4.70 | 10 % |
| Fixed: dashboards, detailed metrics, alarms, KMS, health check | ≈ $17.60 | 37 % |
| Per user: Cognito multi-Region replication | $22.50 | 48 % |
| Tracing | ≈ $2.50 | 5 % |

The event-driven part is the cheapest: EventBridge, SQS, SNS and both Lambdas together cost
**under $2** for 100,000 orders, replication included. The biggest single line is
**identity DR**. Cognito multi-Region replication is charged per MAU with no free tier, so at
5,000 users it costs about five times everything else that is usage-based.

## Scenario B: 1,000,000 orders / month (50,000 MAU)

| Group | Calculation | Monthly |
|---|---|---|
| Usage | API Gateway 4 M × $3.50 = $14.00; SQS (2.6 M polls + 6.2 M message ops) × $0.40 = $3.52; Lambda $0.73; DynamoDB writes 9 M × $0.625 = $5.63, replicated 6 M × $0.625 = $3.75, reads $0.38, PITR ~$0.80; EventBridge 4 M × $1 = $4.00; SNS $1.00; Logs 7 GB × $0.50 = $3.50 | ≈ $37.30 |
| Fixed | Same as scenario A: dashboards, metrics, alarms, KMS, health check | ≈ $17.60 |
| Cognito | Essentials (50,000 − 10,000 free) × $0.015 = $600; multi-Region replication 50,000 × $0.0045 = $225 | **≈ $825** |
| X-Ray | ~2.6 M traces from the reservoir + 5 % of the rest ◦ | ≈ $15 |
| **Total** | | **≈ $895 / month** |

At this scale the per-order cost of the event pipeline is about **$0.00004**, and identity is
over 90 % of the bill. Any real deployment should compare Cognito's per-MAU price with the
business value of signing in during a regional outage.

## Cheapest improvements (not applied)

1. **Turn off per-method `MetricsEnabled`** on the API stages (the same lesson as Project 3). The
   dashboard only uses stage-level metrics, which are free. Saves **$8.40**.
2. **One dashboard in the primary region** with cross-region widgets, instead of one per region.
   Saves **$3**.
3. **Cognito replication only if sign-in must survive a regional outage.** Without it, orders are
   still processed and stored in the secondary region; only new sign-ins depend on the primary.
   You could also use the Lite plan (no replication): saves **$22.50** in scenario A and about
   **$605** in scenario B (Lite: (50,000 − 10,000) × $0.0055 = $220 instead of $825).
4. **An X-Ray sampling rule** (e.g. 1 % after the reservoir) removes most of the tracing cost at
   higher volumes.
5. **SQS polling** in the idle secondary costs ~$0.53/month (2 queues). Accepted: it keeps the
   standby warm.

## What it costs while idle (no traffic, as it is now)

Lambda event source mappings polling 4 queues (~$1.05), 7 alarms ($0.70), 2 KMS keys ($2),
the health check ($0–0.50), Cognito replication for the 2 test users (< $0.01), and the
dashboards. The 2 dashboards are within the 3 free per account, together with Project 3's.
**≈ $4 / month.** Everything else is pay-per-request.

## What this project actually cost to build

Charges came from one day of development:
- a few thousand test orders and their events and notifications;
- the health check, alarms and KMS keys, prorated;
- two short outage demos and the failover test.

All of that is billed per request, apart from the prorated fixed items. Cost Explorer lags by up
to 24 hours; check *Billing → Bills* for `us-east-1` and `us-west-2` the next day.

## Reproducing this in the AWS Pricing Calculator

Create an estimate at <https://calculator.aws/#/estimate> with these entries for **US East (N.
Virginia)**, and a second group for **US West (Oregon)** where noted:

- **API Gateway**: REST API, 400 thousand requests/month.
- **Amazon SQS**: standard queue, 3.4 million requests/month (both regions together).
- **AWS Lambda** (arm): 2 functions, 100 thousand requests each, 256 MB, 60 ms and 40 ms.
- **DynamoDB** on-demand, as a global table: 900 thousand write request units, 300 thousand
  eventually consistent reads; replica in US West (Oregon) with 600 thousand replicated write
  request units; PITR on.
- **Amazon EventBridge**: 200 thousand custom events in us-east-1, 200 thousand replicated
  events in us-west-2.
- **Amazon SNS**: 200 thousand requests, SQS deliveries.
- **Amazon Cognito**: Essentials, 5,000 MAU, multi-Region replication add-on.
- **AWS KMS**: 2 customer managed keys.
- **Amazon Route 53**: 1 health check (AWS endpoint).
- **Amazon CloudWatch**: 2 dashboards, 7 alarms, 28 custom metrics, 0.7 GB of logs ingested.
- **AWS X-Ray**: ~0.5 million traces recorded.

The calculator needs an interactive session to save and share an estimate, so no shared link
was generated here.
