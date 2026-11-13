package dev.poc.outbox.lib.subscribe

import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage
import com.google.cloud.pubsub.v1.Subscriber
import dev.poc.outbox.lib.inbox.InboxProcessor
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationRegistry
import io.micrometer.observation.transport.ReceiverContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/** Subscribes every [EventHandler] once the app is ready. Acks after the inbox transaction commits. */
class PubSubEventSubscriber(
    private val template: PubSubSubscriberTemplate,
    private val handlers: List<EventHandler>,
    private val inboxProcessor: InboxProcessor,
    private val observations: ObservationRegistry,
    private val meters: MeterRegistry,
    consumerFailRate: Double,
) : DisposableBean {
    private val log = LoggerFactory.getLogger(javaClass)
    private val subscribers = mutableListOf<Subscriber>()

    /** Chaos: share of messages nacked before processing (`poc.chaos.consumer-fail-rate`). */
    @Volatile
    var consumerFailRate: Double = consumerFailRate

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        for (handler in handlers) {
            subscribers += template.subscribe(handler.subscription) { onMessage(handler, it) }
            log.info("Subscribed {} to {}", handler.javaClass.simpleName, handler.subscription)
        }
    }

    override fun destroy() {
        subscribers.forEach { it.stopAsync() }
        subscribers.forEach { runCatching { it.awaitTerminated(5, TimeUnit.SECONDS) } }
    }

    private fun onMessage(handler: EventHandler, message: BasicAcknowledgeablePubsubMessage) {
        val attributes = message.pubsubMessage.attributesMap
        val eventType = attributes["eventType"]
        val eventId = attributes["eventId"]?.let { runCatching { UUID.fromString(it) }.getOrNull() }
        when {
            Random.nextDouble() < consumerFailRate -> {
                log.warn("Chaos: nacking eventId={}", eventId)
                finish(handler, message, "chaos", ack = false)
            }
            eventType == null || eventId == null || !handler.handles(eventType) -> {
                log.info("Ignoring message eventType={} eventId={} on {}", eventType, attributes["eventId"], handler.subscription)
                finish(handler, message, "ignored", ack = true)
            }
            else -> process(handler, message, InboundEvent(eventId, eventType, attributes["aggregateId"].orEmpty(),
                message.pubsubMessage.data.toStringUtf8(), attributes))
        }
    }

    private fun process(handler: EventHandler, message: BasicAcknowledgeablePubsubMessage, event: InboundEvent) {
        val context = ReceiverContext<Map<String, String>> { carrier, key -> carrier[key] }
        context.setCarrier(event.attributes)
        try {
            val processed = Observation.createNotStarted("pubsub.consume", { context }, observations)
                .lowCardinalityKeyValue("subscription", handler.subscription)
                .lowCardinalityKeyValue("event.type", event.eventType)
                .observeChecked<Boolean, Exception> { inboxProcessor.process(event, handler) }!!
            if (!processed) log.info("Duplicate eventId={} on {}, skipped", event.eventId, handler.subscription)
            finish(handler, message, if (processed) "processed" else "duplicate", ack = true)
        } catch (e: Exception) {
            log.warn("Handling eventId={} type={} failed, nacking", event.eventId, event.eventType, e)
            finish(handler, message, "failed", ack = false)
        }
    }

    private fun finish(handler: EventHandler, message: BasicAcknowledgeablePubsubMessage, outcome: String, ack: Boolean) {
        meters.counter("inbox.messages", "subscription", handler.subscription, "outcome", outcome).increment()
        if (ack) message.ack() else message.nack()
    }
}
