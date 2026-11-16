package dev.poc.outbox.lib.outbox

import dev.poc.outbox.lib.publish.MessagePublisher
import dev.poc.outbox.lib.publish.OutboundMessage
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

data class RelayResult(val published: Int, val failed: Int, val skipped: Int)

/** Publishes claimed outbox rows in id order per aggregate and marks the outcome, all in one transaction. */
class OutboxRelay(
    private val repository: OutboxRepository,
    private val publisher: MessagePublisher,
    private val properties: OutboxProperties,
    private val tx: TransactionTemplate,
    meterRegistry: MeterRegistry,
    private val crashAfterPublish: Boolean,
    private val crashAction: CrashAction,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val publishedCounter = meterRegistry.counter("outbox.published")
    private val failureCounter = meterRegistry.counter("outbox.publish.failures")

    /** (topic, key) pairs paused in the publisher by a failure in an earlier cycle. */
    private val toResume: MutableSet<Pair<String, String>> = ConcurrentHashMap.newKeySet()

    fun relayOnce(): RelayResult {
        val result = tx.execute { relayInTransaction() }!!
        publishedCounter.increment(result.published.toDouble())
        failureCounter.increment(result.failed.toDouble())
        return result
    }

    private fun relayInTransaction(): RelayResult {
        resumePausedKeys()
        val keys = repository.lockClaimableKeys(properties.batchSize)
        if (keys.isEmpty()) return RelayResult(0, 0, 0)
        val rows = repository.claim(keys, properties.batchSize)

        // A publish that fails at once never reaches the client, so the client does not pause its key:
        // later rows of that aggregate must not be sent at all.
        val failedFast = mutableSetOf<String>()
        val sent = rows.map { row ->
            val future = if (row.aggregateId in failedFast) null else publish(row)
            if (future?.isCompletedExceptionally == true) failedFast += row.aggregateId
            row to future
        }
        var published = 0
        var failed = 0
        var skipped = 0
        // Keyed by aggregate like the claim SQL, so a failure blocks the aggregate's rows on every topic.
        val failedAggregates = mutableSetOf<String>()
        val now = Instant.now()
        for ((row, future) in sent) {
            if (row.aggregateId in failedAggregates || future == null) {
                skipped++ // the client paused this key after the earlier failure, or the row was never sent
                continue
            }
            val error = await(future)
            if (error == null) {
                row.publishedAt = now
                published++
            } else {
                markFailed(row, error, now)
                failedAggregates += row.aggregateId
                toResume += row.topic to row.aggregateId
                failed++
            }
        }

        if (crashAfterPublish) crashAction.crash()
        return RelayResult(published, failed, skipped)
    }

    private fun resumePausedKeys() {
        val keys = toResume.toList()
        keys.forEach { (topic, key) -> publisher.resume(topic, key) }
        toResume -= keys.toSet()
    }

    private fun publish(row: OutboxEvent): CompletableFuture<String> =
        try {
            publisher.publish(
                OutboundMessage(
                    topic = row.topic,
                    orderingKey = row.aggregateId,
                    data = row.payload,
                    attributes = row.headers + mapOf(
                        "eventId" to row.eventId.toString(),
                        "eventType" to row.eventType,
                        "aggregateId" to row.aggregateId,
                        "occurredAt" to row.createdAt.toString(),
                    ),
                ),
            )
        } catch (e: Exception) {
            CompletableFuture.failedFuture(e)
        }

    private fun await(future: CompletableFuture<String>): Throwable? =
        try {
            future.get(PUBLISH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            null
        } catch (e: Exception) {
            (e.cause ?: e)
        }

    private fun markFailed(row: OutboxEvent, error: Throwable, now: Instant) {
        row.attempts++
        row.lastError = (error.message ?: error.javaClass.name).take(1000)
        if (row.attempts >= properties.maxAttempts) {
            row.failedAt = now
            row.nextAttemptAt = null
            log.error(
                "Outbox event failed permanently, its key is blocked until requeued: eventId={} aggregateId={} topic={} lastError={}",
                row.eventId, row.aggregateId, row.topic, row.lastError,
            )
        } else {
            val backoff = Duration.ofSeconds(1L shl row.attempts.coerceAtMost(30)).coerceAtMost(properties.maxBackoff)
            row.nextAttemptAt = now.plus(backoff)
            log.warn("Outbox publish failed (attempt {}): eventId={} error={}", row.attempts, row.eventId, row.lastError)
        }
    }

    private companion object {
        val PUBLISH_TIMEOUT: Duration = Duration.ofSeconds(30)
    }
}
