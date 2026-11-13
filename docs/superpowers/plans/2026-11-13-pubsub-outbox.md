# Spring Pub/Sub Transactional Outbox PoC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two Spring Boot services (order, payment) exchange events over GCP Pub/Sub through a transactional outbox and an idempotent inbox. They run on the emulator and then on GCP.

**Architecture:**
- The shared `outbox-lib` holds the outbox entity and writer, a polling relay (SKIP LOCKED + advisory lock per key), an inbox processor and a subscriber adapter.
- Each service owns its own Postgres schema and includes the lib's Liquibase changelog.
- Pub/Sub access goes through two small interfaces (`MessagePublisher`, `EventSubscriber`), so the relay and consumers can be tested without Pub/Sub, and the client lib can be swapped.

**Tech Stack:** Spring Boot 4.1.2, Kotlin 2.3.21, JDK 25, Gradle 9.8.2, JPA/Hibernate, Liquibase, PostgreSQL 18, spring-cloud-gcp 8.0.5, Testcontainers, Micrometer/OTel, Jib, Terraform, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-13-pubsub-outbox-design.md`

## Global Constraints

- Repo root: `~/workspace/poc/spring-pubsub-outbox-poc`. Modules: `outbox-lib`, `order-service`, `payment-service`, `e2e-tests`. Package root: `dev.poc.outbox`.
- Copy the build conventions from `~/workspace/poc/spring-opentelemetry-poc`:
  - version catalog
  - `platform(SpringBootPlugin.BOM_COORDINATES)`
  - `jvmToolchain(25)`
  - `-Xjsr305=strict -Xannotation-default-target=param-property`
  - the gradle wrapper 9.8.2
- Add `kotlin("plugin.jpa")` to modules that have entities.
- Add `com.google.cloud:spring-cloud-gcp-dependencies:8.0.5` as a platform.
- Turn on `spring.threads.virtual.enabled=true` in both services. Use no coroutines.
- Local Pub/Sub project id: `poc-local`. Emulator host: `localhost:8085`.
- Topics: `order-events`, `payment-events`. DLQs: `order-events-dlq`, `payment-events-dlq`.
- Subscriptions: `payment-service.order-events`, `order-service.payment-events`. Both have ordering on, `maxDeliveryAttempts=5` and retry 10s–600s.
- Schemas and users: `orders`/`order_svc`, `payments`/`payment_svc`. Password = user name, local only.
- Ports: order 8081, payment 8082, Postgres 5432, emulator 8085, Grafana 3000, OTLP 4317/4318.
- Relay: fixed delay `500ms`, batch `100`, backoff `min(2^attempts, 60)` seconds. Purge published outbox rows older than `7d` and inbox rows older than `14d`.
- Payment rule: `amount > 1000` → declined (reason `"amount exceeds limit"`).
- Commit messages: extremely concise, end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Two relay instances working on the same key.** Events for one orderId must reach Pub/Sub in `id` order, with none skipped. Test: `OutboxRelayConcurrencyTest` (Task 3).
2. **Publish failure in the middle of a batch for key K.** Later rows of K must not be published before the failed one. Other keys must still go out. Test: `relay skips later rows of failed key and publishes other keys` (Task 3).
3. **Duplicate delivery after crash-after-publish.** The business effect must happen exactly once and the duplicate must be acked. Test: `crash after publish leads to redelivery deduplicated by inbox` (Task 7).
4. **Malformed or unknown `eventType` arrives.** It must be acked and ignored (logged), not nacked forever. A bad JSON payload must be nacked so it reaches the DLQ. Test: `unknown event type is acked and ignored` and `malformed payload is nacked` (Task 4).
5. **Payment event arrives for an order already CANCELLED by the user.** Status must stay CANCELLED with no exception, and the message must be acked. Test: `payment completed for cancelled order keeps cancelled` (Task 5).

The emulator may not apply dead-letter policies. DLQ behaviour is checked only on GCP (Task 10).

---

### Task 1: Scaffold, local infra, Boot 4.1 + spring-cloud-gcp compatibility

**Files:**
- Create: `settings.gradle.kts`, `build.gradle.kts`, `gradle/libs.versions.toml`, gradle wrapper (copy from spring-opentelemetry-poc), `.gitignore`
- Create: `outbox-lib/build.gradle.kts` (`java-library`, no Boot plugin application)
- Create: `order-service/build.gradle.kts`, `payment-service/build.gradle.kts`, `e2e-tests/build.gradle.kts`
- Create: `order-service/src/main/kotlin/dev/poc/outbox/order/OrderApplication.kt`, `payment-service/.../payment/PaymentApplication.kt`
- Create: `order-service/src/main/resources/application.yaml` and the payment equivalent. Datasource URL `jdbc:postgresql://localhost:5432/poc?currentSchema=orders`, `spring.liquibase.default-schema`, `spring.jpa.properties.hibernate.default_schema`, `spring.cloud.gcp.project-id=poc-local`, `spring.cloud.gcp.pubsub.emulator-host=localhost:8085`, `spring.cloud.gcp.pubsub.publisher.enable-message-ordering=true`, `spring.jpa.open-in-view=false`
- Create: `docker/postgres/init.sql` (database `poc`, users and schemas from Global Constraints, each schema owned by its user)
- Create: `docker/pubsub/init.sh`: idempotent `curl -X PUT` against the emulator REST API (`/v1/projects/poc-local/topics/...` and `/subscriptions/...` with `enableMessageOrdering`, `deadLetterPolicy`, `retryPolicy`)
- Create: `compose.yaml`: services `postgres` (mounts init.sql), `pubsub` (emulator, `gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=poc-local`), `pubsub-init` (runs init.sh after pubsub is healthy), `lgtm`
- Create: `outbox-lib/src/testFixtures/kotlin/dev/poc/outbox/lib/test/Containers.kt` (Gradle `java-test-fixtures`)
- Test: `order-service/src/test/kotlin/dev/poc/outbox/order/OrderApplicationTest.kt`, `payment-service/.../PaymentApplicationTest.kt`

**Interfaces:**
- Produces: `object Containers { val postgres: PostgreSQLContainer; val pubsub: PubSubEmulatorContainer; fun register(registry: DynamicPropertyRegistry, schema: String, user: String) }`. Containers start once per JVM. Postgres runs `docker/postgres/init.sql` (copied into `/docker-entrypoint-initdb.d/`). `register` sets the datasource URL/user/password, `spring.cloud.gcp.pubsub.emulator-host` and the project id, then creates the topics/subscriptions from Global Constraints through `TopicAdminClient`/`SubscriptionAdminClient` against the emulator (idempotent).

- [ ] **Step 1: Write the failing tests.** `OrderApplicationTest.contextLoads()` (`@SpringBootTest`, `@DynamicPropertySource` → `Containers.register(r, "orders", "order_svc")`) asserts that the beans `PubSubTemplate` and `EntityManagerFactory` exist. The payment test is the same with `payments`/`payment_svc`.
- [ ] **Step 2: Run** `./gradlew test`. Expected: FAIL (no build / no classes).
- [ ] **Step 3: Add the build files, apps, resources, init scripts, compose and `Containers`.**
- [ ] **Step 4: Run** `./gradlew test`. Expected: PASS. If the context fails because of a spring-cloud-gcp × Boot 4.1 incompatibility, stop and report the error before falling back to the raw client.
- [ ] **Step 5: Run** `docker compose up -d && curl -s localhost:8085/v1/projects/poc-local/subscriptions`. Expected: both subscriptions listed.
- [ ] **Step 6: Commit** `Scaffold PoC: build, compose, containers`.

---

### Task 2: Outbox write path (entity, writer, Liquibase, trace capture)

**Files:**
- Create: `outbox-lib/src/main/kotlin/dev/poc/outbox/lib/outbox/OutboxEvent.kt`, `OutboxRepository.kt`, `OutboxWriter.kt`
- Create: `outbox-lib/src/main/kotlin/dev/poc/outbox/lib/OutboxAutoConfiguration.kt` and `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Create: `outbox-lib/src/main/resources/db/changelog/outbox/outbox-changelog.yaml` (tables `outbox` and `inbox` per the spec; index `outbox(published_at, id)` partial `where published_at is null`; index `outbox(aggregate_id, id)`)
- Create: `outbox-lib/src/test/kotlin/dev/poc/outbox/lib/TestApplication.kt` (uses schema `orders`; test changelog includes the outbox changelog)
- Test: `outbox-lib/src/test/kotlin/dev/poc/outbox/lib/outbox/OutboxWriterTest.kt`

**Interfaces:**
- Produces:
  - `@Entity OutboxEvent(id: Long?, eventId: UUID, topic: String, aggregateType: String, aggregateId: String, eventType: String, payload: String /* jsonb via @JdbcTypeCode(SqlTypes.JSON) */, headers: Map<String,String>, createdAt: Instant, publishedAt: Instant?, attempts: Int, nextAttemptAt: Instant?, lastError: String?)`
  - `class OutboxWriter { @Transactional(propagation = MANDATORY) fun append(topic: String, aggregateType: String, aggregateId: String, eventType: String, payload: Any): OutboxEvent }`. It serializes `payload` with the injected Jackson `JsonMapper`. It puts `traceparent` into `headers` through Micrometer `Propagator.inject` on `tracer.currentTraceContext().context()` and skips it when there is no current span.
  - `interface OutboxRepository : JpaRepository<OutboxEvent, Long>` (relay queries added in Task 3)

- [ ] **Step 1: Write the failing tests**
  - `append inside committed transaction persists row`: assert `published_at == null`, `attempts == 0` and that `payload` round-trips as JSON.
  - `append inside rolled back transaction leaves no row`: `TransactionTemplate` + `setRollbackOnly`, then assert count == 0.
  - `append without transaction throws`: `assertThrows<IllegalTransactionStateException>`.
  - `append inside observation stores traceparent`: wrap in `Observation.start("test", registry)`, then assert `headers["traceparent"]` matches `^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$`.
- [ ] **Step 2: Run** `./gradlew :outbox-lib:test --tests '*OutboxWriterTest'`. Expected: FAIL.
- [ ] **Step 3: Implement the entity, repository, writer, auto-config and changelog.**
- [ ] **Step 4: Run the tests again.** Expected: PASS.
- [ ] **Step 5: Commit** `Add outbox writer`.

---

### Task 3: Relay (claim, publish, ordering, backoff, multi-instance)

**Files:**
- Create: `outbox-lib/src/main/kotlin/dev/poc/outbox/lib/publish/MessagePublisher.kt`, `PubSubMessagePublisher.kt`
- Create: `outbox-lib/src/main/kotlin/dev/poc/outbox/lib/outbox/OutboxRelay.kt`, `OutboxRelayScheduler.kt`, `OutboxProperties.kt`
- Modify: `OutboxRepository.kt` (claim query, failure update)
- Test: `outbox-lib/src/test/kotlin/dev/poc/outbox/lib/outbox/OutboxRelayTest.kt`, `OutboxRelayConcurrencyTest.kt`, `outbox-lib/src/test/kotlin/dev/poc/outbox/lib/publish/PubSubMessagePublisherTest.kt`
- Create (test): `RecordingPublisher` in testFixtures. It records `(topic, orderingKey, eventId)` in arrival order, can fail on chosen eventIds and can delay.

**Interfaces:**
- Produces:
  - `data class OutboundMessage(val topic: String, val orderingKey: String, val data: String, val attributes: Map<String,String>)`
  - `interface MessagePublisher { fun publish(message: OutboundMessage): CompletableFuture<String>; fun resume(topic: String, orderingKey: String) }`
  - `PubSubMessagePublisher(PubSubPublisherTemplate, PublisherFactory)`. Ordering key comes from the `GcpPubSubHeaders.ORDERING_KEY` header. `resume` calls `publisherFactory.createPublisher(topic).resumePublish(key)`.
  - Attributes: `eventId`, `eventType`, `aggregateId`, `occurredAt` (ISO-8601) and every entry of `headers`.
  - `@ConfigurationProperties("poc.outbox") data class OutboxProperties(val batchSize: Int = 100, val pollDelay: Duration = 500ms, val maxBackoff: Duration = 60s, val outboxRetention: Duration = 7d, val inboxRetention: Duration = 14d, val relayEnabled: Boolean = true)`
  - `class OutboxRelay { fun relayOnce(): RelayResult }` and `data class RelayResult(val published: Int, val failed: Int, val skipped: Int)`
  - `fun interface CrashAction { fun crash() }`. The default bean calls `Runtime.getRuntime().halt(1)`. It is called after all futures finish and before commit when `poc.chaos.crash-after-publish=true`.
- Claim query (native, inside the relay transaction). The advisory lock serializes keys across instances, and `NOT EXISTS` blocks a key while its head row is in backoff:

```sql
select o.* from outbox o
where o.published_at is null
  and (o.next_attempt_at is null or o.next_attempt_at <= now())
  and not exists (select 1 from outbox p
                  where p.aggregate_id = o.aggregate_id and p.published_at is null
                    and p.id < o.id and p.next_attempt_at > now())
  and pg_try_advisory_xact_lock(hashtext(o.aggregate_id))
order by o.id
limit :limit
for update skip locked
```

- `relayOnce` algorithm, all in one `@Transactional`:
  1. Claim rows.
  2. Call `resume(topic, key)` for keys that failed in the previous cycle (kept in memory).
  3. Publish in id order. After a failure on key K, skip the remaining rows of K (counted as `skipped`).
  4. Wait for all futures.
  5. Mark the successes with `published_at = now()`.
  6. Mark the failures with `attempts+1`, `next_attempt_at = now + min(2^attempts s, maxBackoff)` and `last_error`.
  7. Run the chaos crash check.
  8. Commit.
- `OutboxRelayScheduler`: `@Scheduled(fixedDelayString = "\${poc.outbox.poll-delay:500ms}")`. It is active only when `relay-enabled`.

- [ ] **Step 1: Write the failing tests in `OutboxRelayTest`.** Use `RecordingPublisher` and call `relayOnce()` directly with the scheduler disabled.
  - `publishes pending rows in id order and marks them published`: 3 rows, then assert the recorded order and that `published_at` is set on all.
  - `failed publish increments attempts and sets backoff`: assert `attempts == 1`, `next_attempt_at` ≈ now+2s (±1s) and `last_error` not null.
  - `relay skips later rows of failed key and publishes other keys`: K1 rows a (fails), b; K2 row c. Assert recorded = [c], `RelayResult(1,1,1)`, b unpublished with `attempts == 0`.
  - `row behind a backing-off head is not claimed`: after the previous test, a second `relayOnce()` publishes nothing for K1.
  - `resume is called for previously failed key`: assert `RecordingPublisher.resumed` contains (topic, K1).
  - `crash after publish rolls back marking`: test `CrashAction` throws, then assert the exception, the row still unpublished and the message recorded once.
  - `attributes contain envelope and traceparent`.
- [ ] **Step 2: Write `OutboxRelayConcurrencyTest`.** 20 keys × 10 rows. Run two `relayOnce()` loops on 2 threads until nothing is pending. Assert each eventId is recorded exactly once and that the recorded ids are strictly increasing per key.
- [ ] **Step 3: Write `PubSubMessagePublisherTest`** against the emulator. Publish 3 messages with the same key, pull them from `payment-service.order-events`, then assert the order, the attributes and that the ordering key is set.
- [ ] **Step 4: Run** `./gradlew :outbox-lib:test`. Expected: the new tests FAIL.
- [ ] **Step 5: Implement.**
- [ ] **Step 6: Run** `./gradlew :outbox-lib:test`. Expected: PASS.
- [ ] **Step 7: Commit** `Add outbox relay`.

---

### Task 4: Inbox, subscriber adapter, purge, metrics

**Files:**
- Create: `outbox-lib/src/main/kotlin/dev/poc/outbox/lib/inbox/InboxProcessor.kt`, `InboxRepository.kt` (+ `InboxEntry` entity)
- Create: `outbox-lib/src/main/kotlin/dev/poc/outbox/lib/subscribe/InboundEvent.kt`, `EventHandler.kt`, `PubSubEventSubscriber.kt`
- Create: `outbox-lib/src/main/kotlin/dev/poc/outbox/lib/outbox/OutboxPurgeJob.kt`, `OutboxMetrics.kt`
- Test: `InboxProcessorTest.kt`, `PubSubEventSubscriberTest.kt`, `OutboxPurgeJobTest.kt`, `OutboxMetricsTest.kt`

**Interfaces:**
- Produces:
  - `data class InboundEvent(val eventId: UUID, val eventType: String, val aggregateId: String, val payload: String, val attributes: Map<String,String>)`
  - `interface EventHandler { val subscription: String; fun handles(eventType: String): Boolean; fun handle(event: InboundEvent) }`
  - `class InboxProcessor { @Transactional fun process(event: InboundEvent, handler: EventHandler): Boolean }`. It returns false on a duplicate. The insert is native `insert into inbox(event_id, received_at) values (?, now()) on conflict do nothing`, and the row count decides.
  - `PubSubEventSubscriber`: on `ApplicationReadyEvent` it subscribes each `EventHandler` bean through `PubSubSubscriberTemplate.subscribe`. Per message:
    1. If `poc.chaos.consumer-fail-rate` > `Random.nextDouble()`, nack.
    2. Missing or unknown `eventType` (no `handles`) → log, ack.
    3. Otherwise open an `Observation` "pubsub.consume" whose parent comes from `traceparent` (Micrometer `Propagator.extract`), call `inboxProcessor.process`, then ack.
    4. Any exception → log, nack.
    5. Handlers throw `MalformedEventException` on JSON parse errors. It is nacked like any other exception.
  - `OutboxPurgeJob.purge(): Pair<Int, Int>` (deleted outbox, deleted inbox) on an `@Scheduled(cron = "0 0 * * * *")`.
  - `OutboxMetrics`: Micrometer gauges `outbox.pending` and `outbox.lag` (seconds, 0 when empty), and counters `outbox.published` and `outbox.publish.failures`, which the relay increments.

- [ ] **Step 1: Write the failing tests**
  - `InboxProcessorTest`:
    - `first delivery runs handler and returns true`
    - `duplicate delivery skips handler and returns false`
    - `handler exception rolls back inbox row`: count == 0 afterwards, so a redelivery is processed.
  - `PubSubEventSubscriberTest` (emulator, test `EventHandler` recording calls):
    - `message is handled and acked`: no redelivery within 15s ack deadline, verified by the handler call count staying 1.
    - `unknown event type is acked and ignored`
    - `malformed payload is nacked`: the handler is called ≥2 times.
    - `consumer fail rate 1.0 nacks everything`
    - `trace continues from traceparent attribute`: the handler sees the same traceId.
  - `OutboxPurgeJobTest`: a published row aged 8d is deleted, aged 6d is kept and an unpublished row aged 30d is kept. An inbox row aged 15d is deleted.
  - `OutboxMetricsTest`: 2 pending rows, the oldest 10s old, so `outbox.pending == 2` and `outbox.lag >= 10`.
- [ ] **Step 2: Run** `./gradlew :outbox-lib:test`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `./gradlew :outbox-lib:test`. Expected: PASS.
- [ ] **Step 5: Commit** `Add inbox, subscriber, purge, metrics`.

---

### Task 5: order-service domain, API, events

**Files:**
- Create: `order-service/src/main/kotlin/dev/poc/outbox/order/Order.kt` (entity: `id: UUID`, `amount: BigDecimal`, `status: OrderStatus {NEW, PAID, CANCELLED}`, `cancelReason: String?`, `createdAt`, `@Version version`), `OrderRepository.kt`, `OrderService.kt`, `OrderController.kt`, `events/OrderEvents.kt`, `events/PaymentEventsHandler.kt`
- Create: `order-service/src/main/resources/db/changelog/db.changelog-master.yaml` (includes the outbox changelog and the `orders` table)
- Test: `OrderControllerTest.kt`, `PaymentEventsHandlerTest.kt`

**Interfaces:**
- Consumes: `OutboxWriter.append`, `EventHandler`, `InboundEvent` (Tasks 2 and 4).
- Produces:
  - Payloads `OrderCreated(orderId: UUID, amount: BigDecimal)` and `OrderCancelled(orderId: UUID)`. Event type strings equal the class simple names. Topic `order-events`, aggregateType `Order`, aggregateId `orderId.toString()`.
  - API:
    - `POST /orders {"amount": 100.00}` → 201 + `OrderDto(id, amount, status, cancelReason)`
    - `GET /orders/{id}` → 200 / 404
    - `POST /orders/{id}/cancel` → 200, or 409 if not NEW, or 404
  - `PaymentEventsHandler` subscribes to `order-service.payment-events` and handles `PaymentCompleted` and `PaymentFailed`. `PaymentCompleted` moves NEW → PAID. `PaymentFailed` moves NEW → CANCELLED with `cancelReason = reason`. Any other status is logged and left unchanged.

- [ ] **Step 1: Write the failing tests** (`@SpringBootTest` + `Containers`, relay disabled)
  - `create order persists NEW and writes OrderCreated to outbox`
  - `get unknown order returns 404`
  - `cancel NEW order writes OrderCancelled`
  - `cancel PAID order returns 409 and writes nothing`
  - `amount must be positive`: 400 for `0`.
  - `PaymentEventsHandlerTest`: `payment completed marks order PAID`, `payment failed cancels order with reason` and `payment completed for cancelled order keeps cancelled`. Call `inboxProcessor.process(event, handler)` directly.
- [ ] **Step 2: Run** `./gradlew :order-service:test`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `./gradlew :order-service:test`. Expected: PASS.
- [ ] **Step 5: Commit** `Add order service`.

---

### Task 6: payment-service

**Files:**
- Create: `payment-service/src/main/kotlin/dev/poc/outbox/payment/Payment.kt` (entity: `id: UUID`, `orderId: UUID` unique, `amount`, `status: PaymentStatus {APPROVED, DECLINED}`, `reason: String?`, `createdAt`), `PaymentRepository.kt`, `PaymentService.kt`, `events/PaymentEvents.kt`, `events/OrderEventsHandler.kt`
- Create: `payment-service/src/main/resources/db/changelog/db.changelog-master.yaml`
- Test: `OrderEventsHandlerTest.kt`

**Interfaces:**
- Consumes: Task 2 and Task 4 interfaces.
- Produces: payloads `PaymentCompleted(paymentId: UUID, orderId: UUID, amount: BigDecimal)` and `PaymentFailed(paymentId: UUID, orderId: UUID, reason: String)`. Topic `payment-events`, aggregateType `Payment`, aggregateId = orderId (the ordering key). `OrderEventsHandler` subscribes to `payment-service.order-events` and handles `OrderCreated` only.

- [ ] **Step 1: Write the failing tests**
  - `order up to 1000 is approved and PaymentCompleted written`: amount `1000.00` → APPROVED.
  - `order above 1000 is declined and PaymentFailed written`: `1000.01` → DECLINED, reason `"amount exceeds limit"`.
  - `OrderCancelled is ignored`: `handles("OrderCancelled") == false`.
- [ ] **Step 2: Run** `./gradlew :payment-service:test`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `./gradlew :payment-service:test`. Expected: PASS.
- [ ] **Step 5: Commit** `Add payment service`.

---

### Task 7: End-to-end and failure-demo tests

**Files:**
- Create: `e2e-tests/src/test/kotlin/dev/poc/outbox/e2e/TwoServices.kt`. It starts both apps in one JVM with `SpringApplicationBuilder`, random ports and `Containers` properties, and can start extra order instances with overrides.
- Test: `e2e-tests/src/test/kotlin/dev/poc/outbox/e2e/OrderPaymentFlowTest.kt`, `FailureDemoTest.kt`

**Interfaces:**
- Consumes: the order API (Task 5), `CrashAction` (Task 3) and the chaos properties.

- [ ] **Step 1: Write the failing tests** (Awaitility, timeout 30s)
  - `small order becomes PAID`: amount 100.
  - `large order becomes CANCELLED with reason`: amount 5000.
  - `crash after publish leads to redelivery deduplicated by inbox`:
    1. Start order with `poc.chaos.crash-after-publish=true` and a `CrashAction` bean that throws once, then becomes a no-op.
    2. Create the order.
    3. Assert it becomes PAID, the payment table has exactly 1 row for the orderId, and the order outbox `OrderCreated` row is published once in the DB.
  - `consumer failures are retried until success`: payment with `consumer-fail-rate=0.5` and 5 orders → all PAID.
  - `two order instances relay without duplicates`: 2 order contexts on the same DB and 50 orders → all PAID, every outbox row published, payment row count 50.
- [ ] **Step 2: Run** `./gradlew :e2e-tests:test`. Expected: FAIL.
- [ ] **Step 3: Implement `TwoServices`** and any wiring fixes the tests expose.
- [ ] **Step 4: Run** `./gradlew build`. Expected: all modules PASS.
- [ ] **Step 5: Commit** `Add e2e and failure demo tests`.

---

### Task 8: Images, compose apps profile, CI

**Files:**
- Modify: `order-service/build.gradle.kts`, `payment-service/build.gradle.kts`. Jib plugin (latest 3.x; if it fails on Gradle 9.8, switch to `bootBuildImage` and record the switch in AGENTS.md). Base `eclipse-temurin:25-jre`. Image `${'$'}{System.getenv("IMAGE_REPO") ?: "local"}/order-service`. Ports 8081/8082.
- Modify: `compose.yaml`. Add `order-service` and `payment-service` under `profiles: [apps]`, with env overrides for the DB host, emulator host, OTLP endpoint and the chaos flags (default off). Do not set `container_name`, so `--scale` works. Order maps the host port range `"8081-8083:8081"`. Payment exposes no host port.
- Create: `.github/workflows/ci.yml`: ubuntu-latest, `actions/setup-java` temurin 25, `gradle/actions/setup-gradle`, `./gradlew build`.

- [ ] **Step 1: Run** `./gradlew jibDockerBuild`. Expected: images `local/order-service` and `local/payment-service` exist (`docker images`).
- [ ] **Step 2: Run** `docker compose --profile apps up -d --scale order-service=2`, then `curl -XPOST localhost:8081/orders -H 'content-type: application/json' -d '{"amount":50}'`, then `GET` the order. Expected: status `PAID` within ~5s.
- [ ] **Step 3: Run** the manual Pub/Sub outage demo: `docker compose stop pubsub`, create an order, check that `outbox` has `attempts > 0`, `docker compose start pubsub`, then check the order becomes PAID. Then `docker compose --profile apps down`.
- [ ] **Step 4: Commit** `Add images, compose apps, CI`.

---

### Task 9: GCP profile and Terraform

**Files:**
- Create: `order-service/src/main/resources/application-gcp.yaml` and the payment equivalent:
  - `jdbc:postgresql:///poc?cloudSqlInstance=${CLOUD_SQL_INSTANCE}&socketFactory=com.google.cloud.sql.postgres.SocketFactory&enableIamAuth=true&currentSchema=orders`
  - username from `${DB_IAM_USER}`
  - no emulator host
  - project from the metadata server
  - OTLP export disabled (`management.otlp.*.export.enabled=false`, `management.tracing.export.enabled=false`)
- Modify: service builds, adding `com.google.cloud.sql:postgres-socket-factory` (runtimeOnly, version in the catalog).
- Create: `infra/terraform/{versions.tf,variables.tf,main.tf,pubsub.tf,sql.tf,run.tf,iam.tf,outputs.tf}`
  - Variables: `project_id`, `region` (default `europe-west1`), `order_image`, `payment_image`.
  - Enable APIs: run, sqladmin, pubsub, artifactregistry, iam.
  - Artifact Registry repo `poc` (docker).
  - Topics, DLQs and subscriptions as in Global Constraints. Grant the Pub/Sub service agent `roles/pubsub.publisher` on the DLQs and `roles/pubsub.subscriber` on the source subscriptions.
  - Cloud SQL `POSTGRES_18`, tier `db-f1-micro`, flag `cloudsql.iam_authentication=on`, database `poc`, built-in user `postgres` with `random_password` (output, sensitive), and `google_sql_user` type `CLOUD_IAM_SERVICE_ACCOUNT` for both SAs.
  - SAs `order-svc` and `payment-svc` with `roles/cloudsql.client`, `roles/cloudsql.instanceUser`, publisher on their own topic and subscriber on their own subscription.
  - Cloud Run v2 services `order-service` and `payment-service`: `min_instance_count=1`, `cpu_idle=false`, env `SPRING_PROFILES_ACTIVE=gcp`, `CLOUD_SQL_INSTANCE` and `DB_IAM_USER`. Ingress all for order and internal for payment. No `allUsers` invoker.
- Create: `infra/sql/bootstrap.sql`: create schemas `orders` and `payments` authorized to the IAM users (`"order-svc@<project>.iam"`, with `<project>` as a placeholder documented in the README).

- [ ] **Step 1: Run** `./gradlew build`. Expected: PASS, with the gcp profile not loaded.
- [ ] **Step 2: Run** `terraform -chdir=infra/terraform init -backend=false && terraform -chdir=infra/terraform validate`. Expected: `Success!`. If terraform is missing, install it via `sdk`/`tfenv`, or ask the user.
- [ ] **Step 3: Commit** `Add GCP profile and Terraform`.

---

### Task 10: GCP deploy and smoke test (needs the user)

The user does part of this task.

- [ ] **Step 1: Ask the user** for the GCP project id. They create the project and billing and run `gcloud auth login` and `gcloud auth application-default login`.
- [ ] **Step 2: Run** `terraform apply -target=google_artifact_registry_repository.poc -target=google_project_service.apis`, then push images with `IMAGE_REPO=europe-west1-docker.pkg.dev/<p>/poc ./gradlew jib`.
- [ ] **Step 3: Run** a full `terraform apply` with the image vars. The user then runs `infra/sql/bootstrap.sql` in Cloud SQL Studio as `postgres`. Restart both services: `gcloud run services update ... --update-env-vars RESTART=$(date +%s)`.
- [ ] **Step 4: Smoke test.** `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" -XPOST <order-url>/orders -d '{"amount":50}'` → PAID, and `{"amount":5000}` → CANCELLED.
- [ ] **Step 5: DLQ check.** Redeploy payment with `POC_CHAOS_CONSUMER_FAIL_RATE=1.0`, create an order, then `gcloud pubsub subscriptions pull` a temp subscription on `order-events-dlq` shows the message after 5 attempts. Revert the env.
- [ ] **Step 6: Record results** in the README "GCP" section and commit `Document GCP run`.

---

### Task 11: Docs and publishing

**Files:**
- Create: `README.md`: Mermaid sequence and component diagrams, local run, the 4 failure demos with exact commands, GCP deploy steps (Tasks 9 and 10), and known limits (no outbox-side DLQ, emulator DLQ gap, OTLP off on GCP).
- Create: `AGENTS.md` (commands, decisions, conventions, in the style of spring-opentelemetry-poc) and `CLAUDE.md` containing `@AGENTS.md`.
- Modify: `~/workspace/home-repo/repos/poc.md`, adding a `spring-pubsub-outbox-poc` line. Modify `~/workspace/home-repo/AGENTS.md`, changing the repo count.

- [ ] **Step 1: Write the docs.**
- [ ] **Step 2: Ask the user** before running `gh repo create rutven/spring-pubsub-outbox-poc --public --source . --push`.
- [ ] **Step 3: Commit** the docs and the home-repo changes, in separate repos.
