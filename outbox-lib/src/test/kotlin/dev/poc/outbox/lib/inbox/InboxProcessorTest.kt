package dev.poc.outbox.lib.inbox

import dev.poc.outbox.lib.LibIntegrationTest
import dev.poc.outbox.lib.subscribe.EventHandler
import dev.poc.outbox.lib.subscribe.InboundEvent
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class InboxProcessorTest : LibIntegrationTest() {
    @Autowired lateinit var processor: InboxProcessor
    @Autowired lateinit var jdbc: JdbcTemplate

    private class CountingHandler(private val fail: Boolean = false) : EventHandler {
        var calls = 0
        override val subscription = "test"
        override fun handles(eventType: String) = true
        override fun handle(event: InboundEvent) {
            calls++
            if (fail) throw IllegalStateException("handler failed")
        }
    }

    private val event = InboundEvent(UUID.randomUUID(), "OrderCreated", "o-1", "{}", emptyMap())

    @BeforeEach
    fun clean() {
        jdbc.update("delete from inbox")
    }

    @Test
    fun `first delivery runs handler and returns true`() {
        val handler = CountingHandler()

        assertTrue(processor.process(event, handler))
        assertEquals(1, handler.calls)
    }

    @Test
    fun `duplicate delivery skips handler and returns false`() {
        val handler = CountingHandler()
        processor.process(event, handler)

        assertFalse(processor.process(event, handler))
        assertEquals(1, handler.calls)
    }

    @Test
    fun `handler exception rolls back inbox row`() {
        assertThrows<IllegalStateException> { processor.process(event, CountingHandler(fail = true)) }

        assertEquals(0, jdbc.queryForObject("select count(*) from inbox", Int::class.java))
        val retry = CountingHandler()
        assertTrue(processor.process(event, retry))
        assertEquals(1, retry.calls)
    }
}
