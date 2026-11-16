package dev.poc.outbox.payment.events

import java.math.BigDecimal
import java.util.UUID

const val PAYMENT_EVENTS_TOPIC = "payment-events"

/** Published. Event type = class simple name; aggregateId = orderId (the ordering key). */
data class PaymentCompleted(val paymentId: UUID, val orderId: UUID, val amount: BigDecimal)

data class PaymentFailed(val paymentId: UUID, val orderId: UUID, val reason: String)

/** Consumed from order-service. */
data class OrderCreated(val orderId: UUID, val amount: BigDecimal)
