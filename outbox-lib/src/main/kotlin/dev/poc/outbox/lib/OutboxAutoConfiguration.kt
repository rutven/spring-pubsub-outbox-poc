package dev.poc.outbox.lib

import com.google.cloud.spring.pubsub.core.publisher.PubSubPublisherTemplate
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate
import dev.poc.outbox.lib.inbox.InboxProcessor
import dev.poc.outbox.lib.inbox.InboxRepository
import dev.poc.outbox.lib.outbox.OutboxMetrics
import dev.poc.outbox.lib.outbox.OutboxPurgeJob
import dev.poc.outbox.lib.subscribe.EventHandler
import dev.poc.outbox.lib.subscribe.PubSubEventSubscriber
import io.micrometer.observation.ObservationRegistry
import org.springframework.jdbc.core.JdbcTemplate
import com.google.cloud.spring.pubsub.support.PublisherFactory
import dev.poc.outbox.lib.outbox.CrashAction
import dev.poc.outbox.lib.outbox.OutboxProperties
import dev.poc.outbox.lib.outbox.OutboxRelay
import dev.poc.outbox.lib.outbox.OutboxRelayScheduler
import dev.poc.outbox.lib.outbox.OutboxRepository
import dev.poc.outbox.lib.outbox.OutboxWriter
import dev.poc.outbox.lib.publish.MessagePublisher
import dev.poc.outbox.lib.publish.PubSubMessagePublisher
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.json.JsonMapper

/** Registers the lib's entities/repositories (via the auto-configuration package) and beans. */
@AutoConfiguration
@AutoConfigurationPackage
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties::class)
class OutboxAutoConfiguration {
    @Bean
    fun outboxWriter(
        repository: OutboxRepository,
        jsonMapper: JsonMapper,
        tracer: ObjectProvider<Tracer>,
        propagator: ObjectProvider<Propagator>,
    ) = OutboxWriter(repository, jsonMapper, tracer, propagator)

    @Bean
    @ConditionalOnMissingBean
    fun messagePublisher(template: PubSubPublisherTemplate, publisherFactory: PublisherFactory): MessagePublisher =
        PubSubMessagePublisher(template, publisherFactory)

    @Bean
    @ConditionalOnMissingBean
    fun crashAction() = CrashAction { Runtime.getRuntime().halt(1) }

    @Bean
    fun outboxRelay(
        repository: OutboxRepository,
        publisher: MessagePublisher,
        properties: OutboxProperties,
        tx: TransactionTemplate,
        meterRegistry: MeterRegistry,
        @Value("\${poc.chaos.crash-after-publish:false}") crashAfterPublish: Boolean,
        crashAction: CrashAction,
    ) = OutboxRelay(repository, publisher, properties, tx, meterRegistry, crashAfterPublish, crashAction)

    @Bean
    @ConditionalOnProperty("poc.outbox.relay-enabled", matchIfMissing = true)
    fun outboxRelayScheduler(relay: OutboxRelay) = OutboxRelayScheduler(relay)

    @Bean
    fun outboxPurgeJob(jdbc: JdbcTemplate, properties: OutboxProperties) = OutboxPurgeJob(jdbc, properties)

    @Bean
    fun outboxMetrics(jdbc: JdbcTemplate, meterRegistry: MeterRegistry) = OutboxMetrics(jdbc, meterRegistry)

    @Bean
    fun inboxProcessor(inbox: InboxRepository) = InboxProcessor(inbox)

    @Bean
    fun pubSubEventSubscriber(
        template: PubSubSubscriberTemplate,
        handlers: List<EventHandler>,
        inboxProcessor: InboxProcessor,
        observations: ObservationRegistry,
        meterRegistry: MeterRegistry,
        @Value("\${poc.chaos.consumer-fail-rate:0.0}") consumerFailRate: Double,
    ) = PubSubEventSubscriber(template, handlers, inboxProcessor, observations, meterRegistry, consumerFailRate)
}
