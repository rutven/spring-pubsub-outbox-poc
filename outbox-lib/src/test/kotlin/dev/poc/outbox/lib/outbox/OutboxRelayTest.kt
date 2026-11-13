package dev.poc.outbox.lib.outbox

import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationRegistry
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OutboxRelayTest : RelayTestSupport() {
    @Autowired lateinit var observations: ObservationRegistry

    private fun instant(value: Any?) = (value as java.sql.Timestamp).toInstant()

    @Test
    fun `publishes pending rows in id order and marks them published`() {
        val ids = listOf(append("K1"), append("K2"), append("K1"))

        val result = relay.relayOnce()

        assertEquals(RelayResult(3, 0, 0), result)
        assertEquals(ids, publisher.eventIds())
        ids.forEach { assertNotNull(row(it)["published_at"]) }
    }

    @Test
    fun `failed publish increments attempts and sets backoff`() {
        val a = append("K1")
        publisher.failOn += a

        relay.relayOnce()

        val row = row(a)
        assertEquals(1, row["attempts"])
        assertNotNull(row["last_error"])
        val expected = Instant.now().plusSeconds(2)
        val actual = instant(row["next_attempt_at"])
        assertTrue(Duration.between(expected, actual).abs() <= Duration.ofSeconds(1), "next_attempt_at=$actual")
        assertNull(row["published_at"])
    }

    @Test
    fun `relay skips later rows of failed key and publishes other keys`() {
        val a = append("K1")
        val b = append("K1")
        val c = append("K2")
        publisher.failOn += a

        val result = relay.relayOnce()

        assertEquals(listOf(c), publisher.eventIds())
        assertEquals(RelayResult(1, 1, 1), result)
        assertNull(row(b)["published_at"])
        assertEquals(0, row(b)["attempts"])
    }

    @Test
    fun `row behind a backing-off head is not claimed`() {
        val a = append("K1")
        append("K1")
        publisher.failOn += a
        relay.relayOnce()
        publisher.failOn.clear()

        val result = relay.relayOnce()

        assertEquals(RelayResult(0, 0, 0), result)
        assertEquals(emptyList(), publisher.eventIds())
    }

    @Test
    fun `resume is called for previously failed key`() {
        val a = append("K1")
        publisher.failOn += a
        relay.relayOnce()

        relay.relayOnce()

        assertTrue(("order-events" to "K1") in publisher.resumed, "resumed=${publisher.resumed}")
    }

    @Test
    fun `crash after publish rolls back marking`() {
        val a = append("K1")
        val crashing = newRelay(crashAfterPublish = true, crashAction = { throw IllegalStateException("crash") })

        assertThrows<IllegalStateException> { crashing.relayOnce() }

        assertNull(row(a)["published_at"])
        assertEquals(listOf(a), publisher.eventIds())
    }

    @Test
    fun `attributes contain envelope and traceparent`() {
        val observation = Observation.start("test", observations)
        val a = observation.openScope().use { append("K1") }
        observation.stop()

        relay.relayOnce()

        val attributes = publisher.messages.single().attributes
        assertEquals(a, attributes["eventId"])
        assertEquals("OrderCreated", attributes["eventType"])
        assertEquals("K1", attributes["aggregateId"])
        assertNotNull(OffsetDateTime.parse(attributes["occurredAt"]))
        assertTrue(attributes["traceparent"]!!.startsWith("00-"), attributes.toString())
        assertEquals("K1", publisher.messages.single().orderingKey)
    }

    @Test
    fun `row exceeding max attempts is marked failed and blocks its key`() {
        val a = append("K1")
        val b = append("K1")
        val c = append("K2")
        publisher.failOn += a
        val limited = newRelay(properties = OutboxProperties(maxAttempts = 2))

        limited.relayOnce()
        backoffToPast()
        limited.relayOnce()
        backoffToPast()
        limited.relayOnce()

        assertEquals(2, row(a)["attempts"])
        assertNotNull(row(a)["failed_at"])
        assertEquals(listOf(c), publisher.eventIds())
        assertTrue(b !in publisher.eventIds())
    }

    @Test
    fun `requeued failed row is published`() {
        `row exceeding max attempts is marked failed and blocks its key`()
        val (a, b) = jdbc.queryForList("select event_id::text from outbox where aggregate_id = 'K1' order by id", String::class.java)
        publisher.failOn.clear()

        jdbc.update("update outbox set failed_at = null, attempts = 0, next_attempt_at = null where event_id = ?::uuid", a)
        relay.relayOnce()

        assertEquals(listOf(a, b), publisher.eventIds().filter { it == a || it == b })
    }
}
