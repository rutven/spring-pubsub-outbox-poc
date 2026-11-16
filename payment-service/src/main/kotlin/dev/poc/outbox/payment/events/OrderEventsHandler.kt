package dev.poc.outbox.payment.events

import dev.poc.outbox.lib.subscribe.EventHandler
import dev.poc.outbox.lib.subscribe.InboundEvent
import dev.poc.outbox.lib.subscribe.MalformedEventException
import dev.poc.outbox.payment.PaymentService
import org.springframework.stereotype.Component
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper

/** Handles OrderCreated only; OrderCancelled and anything else is acked and ignored by the subscriber. */
@Component
class OrderEventsHandler(private val payments: PaymentService, private val json: JsonMapper) : EventHandler {
    override val subscription = "payment-service.order-events"

    override fun handles(eventType: String) = eventType == "OrderCreated"

    override fun handle(event: InboundEvent) {
        val created = try {
            json.readValue(event.payload, OrderCreated::class.java)
        } catch (e: JacksonException) {
            throw MalformedEventException("Bad OrderCreated payload for ${event.eventId}", e)
        }
        payments.charge(created.orderId, created.amount)
    }
}
