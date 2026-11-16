package dev.poc.outbox.lib.test

import dev.poc.outbox.lib.publish.MessagePublisher
import dev.poc.outbox.lib.publish.OutboundMessage
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * In-memory [MessagePublisher]. Records successful publishes in arrival order. Like the Pub/Sub client,
 * a failed publish pauses its ordering key: later publishes for that key fail until [resume].
 */
class RecordingPublisher : MessagePublisher {
    data class Record(val topic: String, val orderingKey: String, val eventId: String)

    val records = CopyOnWriteArrayList<Record>()
    val messages = CopyOnWriteArrayList<OutboundMessage>()
    val resumed = CopyOnWriteArrayList<Pair<String, String>>()
    val failOn: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /** Event ids whose publish throws before reaching the client, so the key is not paused. */
    val throwOn: MutableSet<String> = ConcurrentHashMap.newKeySet()
    private val paused: MutableSet<Pair<String, String>> = ConcurrentHashMap.newKeySet()

    @Volatile
    var delay: Duration = Duration.ZERO

    override fun publish(message: OutboundMessage): CompletableFuture<String> {
        val eventId = message.attributes.getValue("eventId")
        val key = message.topic to message.orderingKey
        if (eventId in throwOn) throw IllegalArgumentException("rejected $eventId")
        if (key in paused) return CompletableFuture.failedFuture(IllegalStateException("ordering key paused"))
        if (eventId in failOn) {
            paused += key
            return CompletableFuture.failedFuture(IllegalStateException("publish failed for $eventId"))
        }
        if (!delay.isZero) Thread.sleep(delay)
        records += Record(message.topic, message.orderingKey, eventId)
        messages += message
        return CompletableFuture.completedFuture("msg-$eventId")
    }

    override fun resume(topic: String, orderingKey: String) {
        resumed += topic to orderingKey
        paused -= topic to orderingKey
    }

    fun eventIds() = records.map { it.eventId }

    fun reset() {
        records.clear()
        messages.clear()
        resumed.clear()
        failOn.clear()
        throwOn.clear()
        paused.clear()
        delay = Duration.ZERO
    }
}
