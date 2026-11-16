package dev.poc.outbox.lib.outbox

import dev.poc.outbox.lib.LibIntegrationTest
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.IllegalTransactionStateException
import org.springframework.transaction.support.TransactionTemplate
import java.math.BigDecimal
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OutboxWriterTest : LibIntegrationTest() {
    @Autowired lateinit var writer: OutboxWriter
    @Autowired lateinit var tx: TransactionTemplate
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var observations: ObservationRegistry

    data class Created(val orderId: String, val amount: BigDecimal)

    @BeforeEach
    fun clean() {
        jdbc.update("delete from outbox")
    }

    private fun append(id: String = "o-1") =
        writer.append("order-events", "Order", id, "OrderCreated", Created(id, BigDecimal("12.50")))

    @Test
    fun `append inside committed transaction persists row`() {
        tx.execute { append() }

        val row = jdbc.queryForMap("select * from outbox")
        assertNull(row["published_at"])
        assertEquals(0, row["attempts"])
        assertEquals("o-1", row["aggregate_id"])
        assertEquals("OrderCreated", row["event_type"])
        assertEquals("o-1", jdbc.queryForObject("select payload->>'orderId' from outbox", String::class.java))
        assertEquals("12.50", jdbc.queryForObject("select payload->>'amount' from outbox", String::class.java))
    }

    @Test
    fun `append inside rolled back transaction leaves no row`() {
        tx.execute { status ->
            append()
            status.setRollbackOnly()
        }

        assertEquals(0, jdbc.queryForObject("select count(*) from outbox", Int::class.java))
    }

    @Test
    fun `append without transaction throws`() {
        assertThrows<IllegalTransactionStateException> { append() }
    }

    @Test
    fun `append inside observation stores traceparent`() {
        Observation.start("test", observations).openScope().use {
            tx.execute { append() }
        }

        val traceparent = jdbc.queryForObject("select headers->>'traceparent' from outbox", String::class.java)
        assertTrue(Regex("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$").matches(traceparent!!), traceparent)
    }
}
