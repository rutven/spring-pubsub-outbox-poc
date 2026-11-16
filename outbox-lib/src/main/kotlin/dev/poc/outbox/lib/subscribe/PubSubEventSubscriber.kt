package dev.poc.outbox.lib.subscribe

import com.google.api.core.ApiService
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate
import com.google.common.util.concurrent.MoreExecutors
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
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
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
    private val subscribers = ConcurrentHashMap<String, Subscriber>()
    private val restarts = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("pubsub-resubscribe").factory())

    @Volatile
    private var stopping = false

    /** Chaos: share of messages whose handler throws after running, inside the inbox transaction (`poc.chaos.consumer-fail-rate`). */
    @Volatile
    var consumerFailRate: Double = consumerFailRate

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        handlers.forEach(::subscribe)
    }

    override fun destroy() {
        stopping = true
        restarts.shutdownNow()
        subscribers.values.forEach { it.stopAsync() }
        subscribers.values.forEach { runCatching { it.awaitTerminated(5, TimeUnit.SECONDS) } }
    }

    /** A streaming subscriber stops for good on errors such as NOT_FOUND, so a failed one is replaced after a delay. */
    private fun subscribe(handler: EventHandler) {
        if (stopping) return
        val subscriber = try {
            template.subscribe(handler.subscription) { onMessage(handler, it) }
        } catch (e: Exception) {
            log.error("Subscribing to {} failed, retrying in {}", handler.subscription, RESUBSCRIBE_DELAY, e)
            scheduleResubscribe(handler)
            return
        }
        val restartOnce = AtomicBoolean() // listener and the state check below may both see the failure
        subscriber.addListener(
            object : ApiService.Listener() {
                override fun failed(from: ApiService.State, failure: Throwable) {
                    log.error("Subscriber for {} failed, resubscribing in {}", handler.subscription, RESUBSCRIBE_DELAY, failure)
                    if (restartOnce.compareAndSet(false, true)) scheduleResubscribe(handler)
                }
            },
            MoreExecutors.directExecutor(),
        )
        subscribers[handler.subscription] = subscriber
        // The subscriber starts inside subscribe(); a failure before addListener is not replayed to the listener.
        if (subscriber.state() == ApiService.State.FAILED) {
            log.error("Subscriber for {} failed on start, resubscribing in {}", handler.subscription, RESUBSCRIBE_DELAY, subscriber.failureCause())
            if (restartOnce.compareAndSet(false, true)) scheduleResubscribe(handler)
            return
        }
        log.info("Subscribed {} to {}", handler.javaClass.simpleName, handler.subscription)
    }

    private fun scheduleResubscribe(handler: EventHandler) {
        if (stopping) return
        runCatching { restarts.schedule({ subscribe(handler) }, RESUBSCRIBE_DELAY.toMillis(), TimeUnit.MILLISECONDS) }
    }

    private fun onMessage(handler: EventHandler, message: BasicAcknowledgeablePubsubMessage) {
        val attributes = message.pubsubMessage.attributesMap
        val eventType = attributes["eventType"]
        val eventId = attributes["eventId"]?.let { runCatching { UUID.fromString(it) }.getOrNull() }
        when {
            eventType == null || eventId == null -> {
                log.warn("Malformed message eventType={} eventId={} on {}, nacking", eventType, attributes["eventId"], handler.subscription)
                finish(handler, message, "malformed", ack = false)
            }
            !handler.handles(eventType) -> {
                log.info("Ignoring message eventType={} eventId={} on {}", eventType, eventId, handler.subscription)
                finish(handler, message, "ignored", ack = true)
            }
            else -> {
                val event = InboundEvent(eventId, eventType, attributes["aggregateId"].orEmpty(),
                    message.pubsubMessage.data.toStringUtf8(), attributes)
                process(if (Random.nextDouble() < consumerFailRate) ChaosHandler(handler) else handler, message, event)
            }
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
        } catch (e: ChaosException) {
            log.warn("Chaos: eventId={} rolled back, nacking", event.eventId)
            finish(handler, message, "chaos", ack = false)
        } catch (e: Exception) {
            log.warn("Handling eventId={} type={} failed, nacking", event.eventId, event.eventType, e)
            finish(handler, message, "failed", ack = false)
        }
    }

    private fun finish(handler: EventHandler, message: BasicAcknowledgeablePubsubMessage, outcome: String, ack: Boolean) {
        meters.counter("inbox.messages", "subscription", handler.subscription, "outcome", outcome).increment()
        if (ack) message.ack() else message.nack()
    }

    private companion object {
        val RESUBSCRIBE_DELAY: Duration = Duration.ofSeconds(5)
    }
}

private class ChaosException : RuntimeException("chaos")

/** Runs the handler, then throws, so the inbox, business and outbox writes all roll back. */
private class ChaosHandler(private val delegate: EventHandler) : EventHandler by delegate {
    override fun handle(event: InboundEvent) {
        delegate.handle(event)
        throw ChaosException()
    }
}
