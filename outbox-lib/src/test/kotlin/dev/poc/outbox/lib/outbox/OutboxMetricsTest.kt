package dev.poc.outbox.lib.outbox

import dev.poc.outbox.lib.LibIntegrationTest
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OutboxMetricsTest : LibIntegrationTest() {
    @Autowired lateinit var jdbc: JdbcTemplate

    private val registry = SimpleMeterRegistry()

    @BeforeEach
    fun setUp() {
        jdbc.update("delete from outbox")
        OutboxMetrics(jdbc, registry, Duration.ZERO)
    }

    private fun gauge(name: String) = registry.get(name).gauge().value()

    private fun insert(key: String, ageSeconds: Int = 0, failed: Boolean = false, backoff: Boolean = false) = jdbc.update(
        """insert into outbox(event_id, topic, aggregate_type, aggregate_id, event_type, payload, created_at, failed_at, next_attempt_at)
           values (gen_random_uuid(), 't', 'Order', ?, 'E', '{}', now() - make_interval(secs => ?),
                   case when ? then now() end, case when ? then now() + interval '1 minute' end)""",
        key, ageSeconds, failed, backoff,
    )

    @Test
    fun `gauges report pending lag failed and blocked`() {
        assertEquals(0.0, gauge("outbox.lag"))
        insert("A", ageSeconds = 10)
        insert("B", ageSeconds = 2)

        assertEquals(2.0, gauge("outbox.pending"))
        assertTrue(gauge("outbox.lag") >= 10.0)
        assertEquals(0.0, gauge("outbox.blocked"))

        insert("K1", failed = true)
        insert("K1")
        insert("K1")
        insert("K2", backoff = true)
        insert("K2")

        assertEquals(1.0, gauge("outbox.failed"))
        assertEquals(3.0, gauge("outbox.blocked"))
    }
}
