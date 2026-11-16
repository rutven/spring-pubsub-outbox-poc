package dev.poc.outbox.lib.outbox

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("poc.outbox")
data class OutboxProperties(
    val batchSize: Int = 100,
    val pollDelay: Duration = Duration.ofMillis(500),
    val maxBackoff: Duration = Duration.ofSeconds(60),
    val maxAttempts: Int = 10,
    val outboxRetention: Duration = Duration.ofDays(7),
    val inboxRetention: Duration = Duration.ofDays(14),
    val relayEnabled: Boolean = true,
)
