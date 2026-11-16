package dev.poc.outbox.lib.subscribe

import com.google.cloud.spring.pubsub.core.PubSubTemplate
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import dev.poc.outbox.lib.LibIntegrationTest
import dev.poc.outbox.lib.test.Containers
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.tracing.Tracer
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RecordingHandler(private val tracer: Tracer, private val jsonMapper: JsonMapper) : EventHandler {
    val calls = CopyOnWriteArrayList<InboundEvent>()
    val traceIds = CopyOnWriteArrayList<String?>()

    override val subscription = "order-service.payment-events"

    override fun handles(eventType: String) = eventType == "Known"

    override fun handle(event: InboundEvent) {
        calls += event
        traceIds += tracer.currentSpan()?.context()?.traceId()
        try {
            jsonMapper.readTree(event.payload)
        } catch (e: Exception) {
            throw MalformedEventException("bad payload for ${event.eventId}", e)
        }
    }

    fun callsFor(eventId: UUID) = calls.count { it.eventId == eventId }
}

@TestConfiguration
class RecordingHandlerConfig {
    @Bean
    fun recordingHandler(tracer: Tracer, jsonMapper: JsonMapper) = RecordingHandler(tracer, jsonMapper)
}

@Import(RecordingHandlerConfig::class)
class PubSubEventSubscriberTest : LibIntegrationTest() {
    @Autowired lateinit var pubSub: PubSubTemplate
    @Autowired lateinit var handler: RecordingHandler
    @Autowired lateinit var subscriber: PubSubEventSubscriber
    @Autowired lateinit var meters: MeterRegistry

    private fun send(eventType: String, payload: String = "{}", extra: Map<String, String> = emptyMap()): UUID {
        val eventId = UUID.randomUUID()
        val message = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(payload))
            .setOrderingKey(eventId.toString())
            .putAllAttributes(mapOf("eventId" to eventId.toString(), "eventType" to eventType, "aggregateId" to "a-1") + extra)
            .build()
        pubSub.publish("payment-events", message).get(10, TimeUnit.SECONDS)
        return eventId
    }

    private fun outcome(name: String) =
        meters.find("inbox.messages").tags("subscription", handler.subscription, "outcome", name).counter()?.count() ?: 0.0

    /** Longer than the 15s ack deadline, so an unacked message would come back. */
    private fun waitPastAckDeadline() = Thread.sleep(Duration.ofSeconds(17))

    @Test
    fun `message is handled and acked`() {
        val duplicatesBefore = outcome("duplicate")

        val id = send("Known")

        await atMost Duration.ofSeconds(15) untilAsserted { assertEquals(1, handler.callsFor(id)) }
        waitPastAckDeadline()
        assertEquals(1, handler.callsFor(id))
        assertEquals(duplicatesBefore, outcome("duplicate"))
    }

    @Test
    fun `unknown event type is acked and ignored`() {
        val ignoredBefore = outcome("ignored")

        send("Unknown")

        await atMost Duration.ofSeconds(15) untilAsserted { assertEquals(ignoredBefore + 1, outcome("ignored")) }
        waitPastAckDeadline()
        assertEquals(ignoredBefore + 1, outcome("ignored"))
    }

    @Test
    fun `malformed payload is nacked`() {
        val id = send("Known", payload = "{not json")

        await atMost Duration.ofSeconds(40) untilAsserted { assertTrue(handler.callsFor(id) >= 2) }
    }

    @Test
    fun `consumer fail rate 1 rolls back and nacks everything`() {
        val chaosBefore = outcome("chaos")
        val processedBefore = outcome("processed")
        subscriber.consumerFailRate = 1.0
        try {
            val id = send("Known")

            await atMost Duration.ofSeconds(40) untilAsserted { assertTrue(outcome("chaos") >= chaosBefore + 2) }
            assertTrue(handler.callsFor(id) >= 2)
        } finally {
            subscriber.consumerFailRate = 0.0
        }
        // The inbox row was rolled back, so the redelivery is processed, not skipped as a duplicate.
        await atMost Duration.ofSeconds(40) untilAsserted { assertEquals(processedBefore + 1, outcome("processed")) }
    }

    @Test
    fun `message without eventId is nacked`() {
        val malformedBefore = outcome("malformed")
        val message = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("{}"))
            .putAllAttributes(mapOf("eventType" to "Known"))
            .build()
        pubSub.publish("payment-events", message).get(10, TimeUnit.SECONDS)

        await atMost Duration.ofSeconds(40) untilAsserted { assertTrue(outcome("malformed") >= malformedBefore + 2) }
    }

    @Test
    fun `trace continues from traceparent attribute`() {
        val traceId = "0af7651916cd43dd8448eb211c80319c"

        val id = send("Known", extra = mapOf("traceparent" to "00-$traceId-b7ad6b7169203331-01"))

        await atMost Duration.ofSeconds(15) untilAsserted { assertEquals(1, handler.callsFor(id)) }
        assertEquals(traceId, handler.traceIds[handler.calls.indexOfFirst { it.eventId == id }])
    }

    @Test
    fun `subscriber resubscribes after its subscription disappears`() {
        Containers.deleteSubscription(handler.subscription)
        Thread.sleep(Duration.ofSeconds(3)) // streaming pull notices NOT_FOUND and the subscriber fails
        Containers.ensureTopology()

        val id = send("Known")

        await atMost Duration.ofSeconds(45) untilAsserted { assertEquals(1, handler.callsFor(id)) }
    }
}
