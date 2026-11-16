package dev.poc.outbox.lib.subscribe

/** Consumes events from one subscription. Runs inside the inbox transaction. */
interface EventHandler {
    val subscription: String

    fun handles(eventType: String): Boolean

    fun handle(event: InboundEvent)
}
