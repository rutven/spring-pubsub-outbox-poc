package dev.poc.outbox.order

import dev.poc.outbox.lib.inbox.InboxProcessor
import dev.poc.outbox.lib.subscribe.InboundEvent
import dev.poc.outbox.order.events.PaymentEventsHandler
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull

class PaymentEventsHandlerTest : OrderIntegrationTest() {
    @Autowired lateinit var orders: OrderService
    @Autowired lateinit var handler: PaymentEventsHandler
    @Autowired lateinit var inbox: InboxProcessor

    private fun deliver(type: String, orderId: UUID, payload: String) =
        inbox.process(InboundEvent(UUID.randomUUID(), type, orderId.toString(), payload, emptyMap()), handler)

    private fun completed(orderId: UUID) = deliver(
        "PaymentCompleted", orderId, """{"paymentId":"${UUID.randomUUID()}","orderId":"$orderId","amount":10.00}""",
    )

    @Test
    fun `payment completed marks order PAID`() {
        val order = orders.create(BigDecimal("10.00"))

        completed(order.id)

        val after = orders.get(order.id)!!
        assertEquals(OrderStatus.PAID, after.status)
        assertNull(after.cancelReason)
    }

    @Test
    fun `payment failed cancels order with reason`() {
        val order = orders.create(BigDecimal("5000"))

        deliver("PaymentFailed", order.id, """{"paymentId":"${UUID.randomUUID()}","orderId":"${order.id}","reason":"amount exceeds limit"}""")

        val after = orders.get(order.id)!!
        assertEquals(OrderStatus.CANCELLED, after.status)
        assertEquals("amount exceeds limit", after.cancelReason)
    }

    @Test
    fun `payment completed for cancelled order keeps cancelled`() {
        val order = orders.create(BigDecimal("10.00"))
        orders.cancel(order.id)

        completed(order.id)

        assertEquals(OrderStatus.CANCELLED, orders.get(order.id)!!.status)
    }

    @Test
    fun `handles only payment events`() {
        assertEquals(listOf(true, true, false), listOf("PaymentCompleted", "PaymentFailed", "OrderCreated").map(handler::handles))
        assertEquals("order-service.payment-events", handler.subscription)
    }
}
