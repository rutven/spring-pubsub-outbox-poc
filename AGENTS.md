# AGENTS.md

Learning PoC: transactional outbox and idempotent inbox over GCP Pub/Sub with Spring Boot 4.1 (Kotlin, JDK 25). See README.md for the architecture, demos and GCP deploy. The design and plan live in `docs/superpowers/`.

## Commands

- `./gradlew build`: all tests. Needs Docker (Testcontainers runs Postgres 18 and the Pub/Sub emulator). e2e is the slow part.
- `./gradlew :outbox-lib:test --tests '*OutboxRelayTest'`: one class.
- `./gradlew jibDockerBuild`, then `docker compose --profile apps up -d [--scale order-service=2]`.
- `terraform -chdir=infra/terraform validate` (Terraform via mise).

## Decisions (keep them unless the user says otherwise)

- Pub/Sub sits behind `MessagePublisher` and the `EventHandler`/`PubSubEventSubscriber` pair, so relay tests use `RecordingPublisher` (testFixtures). It mimics the client: a failed publish pauses its ordering key until `resume`.
- The relay claims in two statements: first advisory-lock the unblocked keys, then select rows. The second statement's snapshot must postdate the locks; one statement can let a later row jump a head row that just went into backoff. `OutboxRelayConcurrencyTest` guards this. The row select uses plain `FOR UPDATE`, not `SKIP LOCKED`: the advisory locks already split keys, and skipping a locked head row would let a later row of its key go first.
- A row that exhausts `maxAttempts` gets `failed_at` and blocks its key until a manual requeue. Never auto-skip it: that breaks ordering.
- The inbox insert, the handler and its outbox writes share one transaction. Ack only after commit.
- Blocking code on virtual threads, no coroutines.
- Each service owns its schema and includes `db/changelog/outbox/outbox-changelog.yaml` from the lib. Changelog file names are per service (`order-changelog.yaml`, `payment-changelog.yaml`) because e2e puts both apps on one classpath.

## Conventions

- Versions live in `gradle/libs.versions.toml`. Spring and GCP versions come from the Boot BOM plus `spring-cloud-gcp-dependencies`.
- Lib beans are declared in `OutboxAutoConfiguration`, not component-scanned. Lib entities are registered through `@AutoConfigurationPackage`.
- `e2e-tests/TwoServices` passes every service-specific property as a command-line arg: the two `application.yaml` files shadow each other on the shared classpath.
- `spring.cloud.gcp.sql.enabled=false`: the `gcp` profile builds the Cloud SQL JDBC URL itself.
