package dev.poc.outbox.payment

import dev.poc.outbox.lib.inbox.InboxProcessor
import dev.poc.outbox.lib.subscribe.InboundEvent
import dev.poc.outbox.lib.test.Containers
import dev.poc.outbox.payment.events.OrderEventsHandler
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

@SpringBootTest(properties = ["poc.outbox.relay-enabled=false"])
class OrderEventsHandlerTest {
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var handler: OrderEventsHandler
    @Autowired lateinit var inbox: InboxProcessor

    @BeforeEach
    fun clean() {
        jdbc.update("delete from outbox")
        jdbc.update("delete from inbox")
        jdbc.update("delete from payments")
    }

    private fun orderCreated(amount: String): UUID {
        val orderId = UUID.randomUUID()
        inbox.process(
            InboundEvent(UUID.randomUUID(), "OrderCreated", orderId.toString(), """{"orderId":"$orderId","amount":$amount}""", emptyMap()),
            handler,
        )
        return orderId
    }

    private fun payment(orderId: UUID) = jdbc.queryForMap("select * from payments where order_id = ?", orderId)

    private fun outbox(orderId: UUID) = jdbc.queryForMap(
        "select topic, aggregate_id, event_type, payload::text as payload, payload->>'reason' as reason from outbox where aggregate_id = ?",
        orderId.toString(),
    )

    @Test
    fun `order up to 1000 is approved and PaymentCompleted written`() {
        val orderId = orderCreated("1000.00")

        assertEquals("APPROVED", payment(orderId)["status"])
        assertNull(payment(orderId)["reason"])
        val event = outbox(orderId)
        assertEquals("PaymentCompleted", event["event_type"])
        assertEquals("payment-events", event["topic"])
    }

    @Test
    fun `order above 1000 is declined and PaymentFailed written`() {
        val orderId = orderCreated("1000.01")

        assertEquals("DECLINED", payment(orderId)["status"])
        assertEquals("amount exceeds limit", payment(orderId)["reason"])
        val event = outbox(orderId)
        assertEquals("PaymentFailed", event["event_type"])
        assertEquals("amount exceeds limit", event["reason"])
    }

    @Test
    fun `OrderCancelled is ignored`() {
        assertFalse(handler.handles("OrderCancelled"))
        assertEquals("payment-service.order-events", handler.subscription)
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = Containers.register(registry, "payments", "payment_svc")
    }
}
