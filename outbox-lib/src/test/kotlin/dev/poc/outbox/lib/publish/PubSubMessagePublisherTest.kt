package dev.poc.outbox.lib.publish

import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate
import dev.poc.outbox.lib.LibIntegrationTest
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class PubSubMessagePublisherTest : LibIntegrationTest() {
    @Autowired lateinit var publisher: MessagePublisher
    @Autowired lateinit var subscriber: PubSubSubscriberTemplate

    private val subscription = "payment-service.order-events"

    @Test
    fun `publishes with ordering key and attributes`() {
        while (subscriber.pullAndAck(subscription, 100, true).isNotEmpty()) Unit
        val key = "order-${System.nanoTime()}"

        (1..3).forEach { i ->
            publisher.publish(OutboundMessage("order-events", key, """{"n":$i}""", mapOf("eventId" to "e$i", "eventType" to "T")))
                .get(10, TimeUnit.SECONDS)
        }

        val received = mutableListOf<com.google.pubsub.v1.PubsubMessage>()
        await atMost Duration.ofSeconds(15) untilAsserted {
            received += subscriber.pullAndAck(subscription, 10, true)
            assertEquals(3, received.size)
        }
        assertEquals(listOf("e1", "e2", "e3"), received.map { it.getAttributesOrThrow("eventId") })
        assertEquals(listOf("""{"n":1}""", """{"n":2}""", """{"n":3}"""), received.map { it.data.toStringUtf8() })
        received.forEach { assertEquals(key, it.orderingKey) }
        received.forEach { assertEquals("T", it.getAttributesOrThrow("eventType")) }
    }
}
