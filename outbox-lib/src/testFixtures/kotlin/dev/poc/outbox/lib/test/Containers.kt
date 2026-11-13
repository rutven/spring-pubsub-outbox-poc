package dev.poc.outbox.lib.test

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.AlreadyExistsException
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.protobuf.Duration
import com.google.pubsub.v1.DeadLetterPolicy
import com.google.pubsub.v1.RetryPolicy
import com.google.pubsub.v1.Subscription
import com.google.pubsub.v1.SubscriptionName
import com.google.pubsub.v1.TopicName
import io.grpc.ManagedChannelBuilder
import org.springframework.test.context.DynamicPropertyRegistry
import org.testcontainers.gcloud.PubSubEmulatorContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile
import java.nio.file.Path

/** Postgres + Pub/Sub emulator, started once per JVM. */
object Containers {
    const val PROJECT = "poc-local"

    /** Subscription name to topic name, as in Global Constraints. */
    val subscriptions = mapOf(
        "payment-service.order-events" to "order-events",
        "order-service.payment-events" to "payment-events",
    )

    val postgres: PostgreSQLContainer by lazy {
        PostgreSQLContainer(DockerImageName.parse("postgres:18"))
            .withDatabaseName("poc")
            .withCopyFileToContainer(
                MountableFile.forHostPath(Path.of(System.getProperty("poc.rootDir"), "docker/postgres/init.sql")),
                "/docker-entrypoint-initdb.d/init.sql",
            )
            .also { it.start() }
    }

    val pubsub: PubSubEmulatorContainer by lazy {
        PubSubEmulatorContainer(DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"))
            .also { it.start() }
            .also { createTopology(it.emulatorEndpoint) }
    }

    fun jdbcUrl(schema: String) = "${postgres.jdbcUrl}&currentSchema=$schema"

    fun register(registry: DynamicPropertyRegistry, schema: String, user: String) {
        registry.add("spring.datasource.url") { jdbcUrl(schema) }
        registry.add("spring.datasource.username") { user }
        registry.add("spring.datasource.password") { user }
        registry.add("spring.cloud.gcp.project-id") { PROJECT }
        registry.add("spring.cloud.gcp.pubsub.emulator-host") { pubsub.emulatorEndpoint }
    }

    fun <T> withChannel(block: (FixedTransportChannelProvider) -> T): T {
        val channel = ManagedChannelBuilder.forTarget(pubsub.emulatorEndpoint).usePlaintext().build()
        try {
            return block(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
        } finally {
            channel.shutdownNow()
        }
    }

    private fun createTopology(endpoint: String) {
        val channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build()
        val transport = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
        val credentials = NoCredentialsProvider.create()
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder().setTransportChannelProvider(transport).setCredentialsProvider(credentials).build(),
        ).use { topics ->
            for (topic in subscriptions.values.flatMap { listOf(it, "$it-dlq") }) {
                ignoreExisting { topics.createTopic(TopicName.of(PROJECT, topic)) }
            }
        }
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder().setTransportChannelProvider(transport).setCredentialsProvider(credentials).build(),
        ).use { subs ->
            for ((sub, topic) in subscriptions) {
                ignoreExisting {
                    subs.createSubscription(
                        Subscription.newBuilder()
                            .setName(SubscriptionName.of(PROJECT, sub).toString())
                            .setTopic(TopicName.of(PROJECT, topic).toString())
                            .setAckDeadlineSeconds(15)
                            .setEnableMessageOrdering(true)
                            .setDeadLetterPolicy(
                                DeadLetterPolicy.newBuilder()
                                    .setDeadLetterTopic(TopicName.of(PROJECT, "$topic-dlq").toString())
                                    .setMaxDeliveryAttempts(5),
                            )
                            .setRetryPolicy(
                                RetryPolicy.newBuilder()
                                    .setMinimumBackoff(Duration.newBuilder().setSeconds(10))
                                    .setMaximumBackoff(Duration.newBuilder().setSeconds(600)),
                            )
                            .build(),
                    )
                }
            }
        }
        channel.shutdownNow()
    }

    private fun ignoreExisting(block: () -> Unit) {
        try {
            block()
        } catch (_: AlreadyExistsException) {
        }
    }
}
