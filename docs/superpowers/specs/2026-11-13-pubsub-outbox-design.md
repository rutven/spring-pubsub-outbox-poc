# Spring Pub/Sub Transactional Outbox PoC: Design

Status: agreed 2026-11-13 (grilling session).

## Goal

Show the transactional outbox pattern with GCP Pub/Sub. It covers:

- an atomic write of the business row and the outbox row
- at-least-once relay
- per-aggregate ordering
- an idempotent consumer
- failure demos
- relaying from several instances without double-publishing

It runs on the Pub/Sub emulator first, then on real GCP.

## Stack

- Spring Boot 4.1.x, Kotlin 2.3.x, JDK 25 toolchain, Gradle Kotlin DSL with a version catalog, Gradle 9.8.x
- JPA (Hibernate), Liquibase, PostgreSQL 18
- `spring-cloud-gcp-starter-pubsub` 8.0.x. It officially targets Boot 4.0, so Task 1 checks that it works on 4.1. If it breaks, fall back to the raw `google-cloud-pubsub` client behind the same `MessagePublisher`/subscriber interfaces.
- Blocking code on virtual threads (`spring.threads.virtual.enabled=true`), no coroutines
- `spring-boot-starter-opentelemetry` exports OTLP to `grafana/otel-lgtm`, the same as spring-opentelemetry-poc

## Services

| Module | Port | Schema | Publishes to | Consumes |
|---|---|---|---|---|
| order-service | 8081 | `orders` (user `order_svc`) | `order-events` | `order-service.payment-events` on `payment-events` |
| payment-service | 8082 | `payments` (user `payment_svc`) | `payment-events` | `payment-service.order-events` on `order-events` |

Shared outbox, relay and inbox code lives in the library module `outbox-lib`.

Flow (choreography):
1. `POST /orders {amount}` creates an `Order(NEW)` and writes `OrderCreated` to the outbox.
2. Payment consumes `OrderCreated`. If amount > 1000 it records `Payment(DECLINED)` and writes `PaymentFailed`. Otherwise it records `Payment(APPROVED)` and writes `PaymentCompleted`.
3. Order consumes these. `PaymentCompleted` moves NEW to PAID. `PaymentFailed` moves NEW to CANCELLED.
4. `POST /orders/{id}/cancel` moves NEW to CANCELLED and writes `OrderCancelled`. Payment acks and ignores it.

API: `POST /orders`, `GET /orders/{id}`, `POST /orders/{id}/cancel`, plus actuator. Payment has no public API.

## Messages

- JSON body. Attributes: `eventId`, `eventType`, `aggregateId`, `occurredAt`, `traceparent`.
- Ordering key = `aggregateId` = orderId. Payment events also use orderId as the key.
- Payloads:
  - `OrderCreated{orderId, amount}`
  - `OrderCancelled{orderId}`
  - `PaymentCompleted{paymentId, orderId, amount}`
  - `PaymentFailed{paymentId, orderId, reason}`
- Subscription settings: message ordering on, dead-letter topic `<topic>-dlq` after 5 delivery attempts, retry backoff 10s–600s. Pub/Sub exactly-once delivery is off.

## Outbox

Table `outbox` sits in each service schema. Columns:

| Column | Type |
|---|---|
| `id` | bigserial PK |
| `event_id` | uuid unique |
| `topic` | text |
| `aggregate_type` | text |
| `aggregate_id` | text |
| `event_type` | text |
| `payload` | jsonb |
| `headers` | jsonb |
| `created_at` | timestamptz |
| `published_at` | timestamptz null |
| `attempts` | int |
| `next_attempt_at` | timestamptz null |
| `last_error` | text null |

`topic` was added beyond the grilling list because the relay needs it.

Relay (`@Scheduled` fixed delay 500 ms, batch 100):
- One transaction claims rows with `FOR UPDATE SKIP LOCKED`, publishes them, waits for the futures and sets `published_at`.
- Per-key ordering when several instances run: the claim takes `pg_try_advisory_xact_lock(hashtext(aggregate_id))`. Only one instance can own a key in a transaction.
- A row is not claimed while an earlier unpublished row of the same key is in backoff.
- If a publish fails, `attempts` goes up, `next_attempt_at = now + min(2^attempts s, 60 s)` and `last_error` is set. Later rows of the same key in the batch are skipped, and `resumePublish(key)` runs on the next cycle. Retries never stop; there is no outbox-side dead letter.
- Published rows are purged after 7 days by an hourly job. Inbox rows are purged after 14 days.

## Inbox / consumer

- `inbox(event_id uuid PK, received_at)`.
- One DB transaction does the inbox insert (`ON CONFLICT DO NOTHING`), the business change and any outgoing outbox row.
- Ack after commit. A duplicate is acked and skipped. An exception is nacked.

## Observability

- `traceparent` is captured when the outbox row is written, stored in `headers` and sent as an attribute. The consumer continues the trace.
- Metrics:
  - `outbox.pending` (gauge)
  - `outbox.lag` (gauge, seconds since the oldest unpublished `created_at`)
  - `outbox.published` (counter)
  - `outbox.publish.failures` (counter)
- On GCP, the apps export OTLP to `localhost:4318`. A Google-built OpenTelemetry Collector sidecar (`otelcol-google`) in each Cloud Run service forwards traces to Cloud Trace and metrics to Cloud Monitoring. Logs go to stdout and Cloud Logging.

## Failure demos (config flags)

- `poc.chaos.crash-after-publish=true`: halts the JVM after the publish futures finish and before the commit.
- `poc.chaos.consumer-fail-rate=0.3`: the consumer throws for that share of messages.
- Pub/Sub outage: run `docker compose stop pubsub`.
- Multi-instance: run `docker compose --profile apps up --scale order-service=2`.

## Local

`compose.yaml` holds:
- `postgres:18`, with an init script that creates the schemas and users
- the Pub/Sub emulator (`gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`, project `poc-local`, port 8085), with a topic/subscription init over its REST API
- `grafana/otel-lgtm`
- both apps, under profile `apps`, as Jib images

Tests use Testcontainers (Postgres + emulator).

## GCP

- Project and region are Terraform variables (default `europe-west1`). The user creates the project and billing.
- Terraform creates:
  - the APIs
  - an Artifact Registry repo `poc`
  - topics, DLQs and subscriptions
  - service accounts `order-svc` and `payment-svc`
  - a Cloud SQL Postgres 18 instance with IAM auth and IAM DB users
  - Cloud Run v2 services with `min_instance_count=1`, CPU always allocated and no unauthenticated access
- Apps connect through the Cloud SQL Java connector with `enableIamAuth=true` (profile `gcp`).
- Schemas and grants come from a one-time bootstrap SQL, run in Cloud SQL Studio.
- Images: Jib → Artifact Registry. CI: GitHub Actions runs build and tests only. Deploy is a manual `terraform apply`.

## Repo

`rutven/spring-pubsub-outbox-poc`, public, in `~/workspace/poc/`. It gets an entry in home-repo `repos/poc.md`. It has a README (Mermaid diagram, local run, demos, GCP deploy) and an AGENTS.md.
