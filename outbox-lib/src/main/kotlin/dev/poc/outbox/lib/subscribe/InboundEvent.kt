package dev.poc.outbox.lib.subscribe

import java.util.UUID

data class InboundEvent(
    val eventId: UUID,
    val eventType: String,
    val aggregateId: String,
    val payload: String,
    val attributes: Map<String, String>,
)

/** Thrown by handlers when the payload cannot be parsed. The message is nacked and ends in the DLQ. */
class MalformedEventException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
