package dev.poc.outbox.lib.inbox

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import java.util.UUID

interface InboxRepository : JpaRepository<InboxEntry, UUID> {
    /** Returns 1 when the event is new, 0 when it was already received. */
    @Modifying
    @Query(nativeQuery = true, value = "insert into inbox(event_id, received_at) values (:eventId, now()) on conflict do nothing")
    fun insertIfAbsent(eventId: UUID): Int
}
