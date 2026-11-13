package dev.poc.outbox.lib.outbox

import dev.poc.outbox.lib.LibIntegrationTest
import dev.poc.outbox.lib.test.RecordingPublisher
import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.support.TransactionTemplate

@TestConfiguration
class RecordingPublisherConfig {
    @Bean
    fun recordingPublisher() = RecordingPublisher()
}

@Import(RecordingPublisherConfig::class)
abstract class RelayTestSupport : LibIntegrationTest() {
    @Autowired lateinit var writer: OutboxWriter
    @Autowired lateinit var tx: TransactionTemplate
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var publisher: RecordingPublisher
    @Autowired lateinit var relay: OutboxRelay
    @Autowired lateinit var repository: OutboxRepository
    @Autowired lateinit var meterRegistry: MeterRegistry

    /** A relay with its own settings, sharing the context's DB and publisher. */
    fun newRelay(
        properties: OutboxProperties = OutboxProperties(),
        crashAfterPublish: Boolean = false,
        crashAction: CrashAction = CrashAction { },
    ) = OutboxRelay(repository, publisher, properties, tx, meterRegistry, crashAfterPublish, crashAction)

    @BeforeEach
    fun cleanOutbox() {
        jdbc.update("delete from outbox")
        publisher.reset()
    }

    /** Appends a row for [key] and returns its eventId. */
    fun append(key: String, topic: String = "order-events"): String =
        tx.execute { writer.append(topic, "Order", key, "OrderCreated", mapOf("orderId" to key)) }!!.eventId.toString()

    fun row(eventId: String): Map<String, Any?> = jdbc.queryForMap("select * from outbox where event_id = ?::uuid", eventId)

    fun pending(): Int = jdbc.queryForObject("select count(*) from outbox where published_at is null", Int::class.java)!!

    fun backoffToPast() = jdbc.update("update outbox set next_attempt_at = now() - interval '1 second' where next_attempt_at is not null")
}
