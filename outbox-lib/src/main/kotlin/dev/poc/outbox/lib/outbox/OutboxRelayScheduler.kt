package dev.poc.outbox.lib.outbox

import org.springframework.scheduling.annotation.Scheduled

class OutboxRelayScheduler(private val relay: OutboxRelay) {
    @Scheduled(fixedDelayString = "\${poc.outbox.poll-delay:500ms}")
    fun relay() {
        do {
            val result = relay.relayOnce()
        } while (result.published + result.failed + result.skipped > 0 && result.failed == 0)
    }
}
