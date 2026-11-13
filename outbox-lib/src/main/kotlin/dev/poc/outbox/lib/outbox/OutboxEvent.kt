package dev.poc.outbox.lib.outbox

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "outbox")
class OutboxEvent(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    val eventId: UUID,
    val topic: String,
    val aggregateType: String,
    val aggregateId: String,
    val eventType: String,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    val payload: String,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    val headers: Map<String, String> = emptyMap(),
    val createdAt: Instant = Instant.now(),
    var publishedAt: Instant? = null,
    var attempts: Int = 0,
    var nextAttemptAt: Instant? = null,
    var lastError: String? = null,
    var failedAt: Instant? = null,
)
