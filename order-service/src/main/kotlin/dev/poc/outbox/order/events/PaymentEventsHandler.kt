package dev.poc.outbox.order.events

import dev.poc.outbox.lib.subscribe.EventHandler
import dev.poc.outbox.lib.subscribe.InboundEvent
import dev.poc.outbox.lib.subscribe.MalformedEventException
import dev.poc.outbox.order.OrderService
import org.springframework.stereotype.Component
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper

@Component
class PaymentEventsHandler(private val orders: OrderService, private val json: JsonMapper) : EventHandler {
    override val subscription = "order-service.payment-events"

    override fun handles(eventType: String) = eventType in TYPES

    override fun handle(event: InboundEvent) {
        when (event.eventType) {
            "PaymentCompleted" -> orders.markPaid(parse<PaymentCompleted>(event).orderId)
            "PaymentFailed" -> parse<PaymentFailed>(event).let { orders.markPaymentFailed(it.orderId, it.reason) }
        }
    }

    private inline fun <reified T> parse(event: InboundEvent): T =
        try {
            json.readValue(event.payload, T::class.java)
        } catch (e: JacksonException) {
            throw MalformedEventException("Bad ${event.eventType} payload for ${event.eventId}", e)
        }

    private companion object {
        val TYPES = setOf("PaymentCompleted", "PaymentFailed")
    }
}
