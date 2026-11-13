package dev.poc.outbox.lib.inbox

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "inbox")
class InboxEntry(
    @Id
    val eventId: UUID,
    val receivedAt: Instant = Instant.now(),
)
