package dev.poc.outbox.lib.outbox

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OutboxRelayConcurrencyTest : RelayTestSupport() {

    @Test
    fun `two relays publish every row once in per-key id order`() {
        repeat(10) { (1..20).forEach { k -> append("K$k") } }
        publisher.delay = Duration.ofMillis(2)
        val relays = listOf(newRelay(properties = OutboxProperties(batchSize = 15)), newRelay(properties = OutboxProperties(batchSize = 15)))

        val pool = Executors.newFixedThreadPool(2)
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
        relays.forEach { relay ->
            pool.submit { while (pending() > 0 && System.nanoTime() < deadline) relay.relayOnce() }
        }
        pool.shutdown()
        assertTrue(pool.awaitTermination(70, TimeUnit.SECONDS))

        assertEquals(0, pending())
        val ids = jdbc.queryForList("select event_id::text, id from outbox").associate { it["event_id"] as String to it["id"] as Long }
        val recorded = publisher.records
        assertEquals(200, recorded.size)
        assertEquals(200, recorded.map { it.eventId }.toSet().size)
        recorded.groupBy { it.orderingKey }.forEach { (key, records) ->
            val seq = records.map { ids.getValue(it.eventId) }
            assertEquals(seq.sorted(), seq, "key $key out of order")
        }
    }
}
