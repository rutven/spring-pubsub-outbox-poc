package dev.poc.outbox.lib.publish

import java.util.concurrent.CompletableFuture

data class OutboundMessage(
    val topic: String,
    val orderingKey: String,
    val data: String,
    val attributes: Map<String, String>,
)

/** Broker port used by the relay. */
interface MessagePublisher {
    /** Completes with the broker message id, or exceptionally on failure. */
    fun publish(message: OutboundMessage): CompletableFuture<String>

    /** Re-enables publishing for an ordering key paused by an earlier failure. */
    fun resume(topic: String, orderingKey: String)
}
