package dev.poc.outbox.lib.outbox

import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import java.sql.Timestamp
import java.time.Instant

/** Deletes published outbox rows and inbox rows past their retention. */
class OutboxPurgeJob(private val jdbc: JdbcTemplate, private val properties: OutboxProperties) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** Returns (deleted outbox rows, deleted inbox rows). */
    @Scheduled(cron = "0 0 * * * *")
    fun purge(): Pair<Int, Int> {
        val now = Instant.now()
        val outbox = jdbc.update("delete from outbox where published_at < ?", Timestamp.from(now - properties.outboxRetention))
        val inbox = jdbc.update("delete from inbox where received_at < ?", Timestamp.from(now - properties.inboxRetention))
        log.info("Purged {} outbox and {} inbox rows", outbox, inbox)
        return outbox to inbox
    }
}
