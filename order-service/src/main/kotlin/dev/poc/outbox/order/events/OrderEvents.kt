package dev.poc.outbox.order.events

import java.math.BigDecimal
import java.util.UUID

const val ORDER_EVENTS_TOPIC = "order-events"

/** Published. Event type = class simple name. */
data class OrderCreated(val orderId: UUID, val amount: BigDecimal)

data class OrderCancelled(val orderId: UUID)

/** Consumed from payment-service. */
data class PaymentCompleted(val paymentId: UUID, val orderId: UUID, val amount: BigDecimal)

data class PaymentFailed(val paymentId: UUID, val orderId: UUID, val reason: String)
