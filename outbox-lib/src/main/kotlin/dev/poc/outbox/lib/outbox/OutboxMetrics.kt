package dev.poc.outbox.lib.outbox

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration

/** Outbox gauges from one aggregate query, cached for [cacheTtl]. The relay owns the counters. */
class OutboxMetrics(private val jdbc: JdbcTemplate, registry: MeterRegistry, private val cacheTtl: Duration = Duration.ofSeconds(5)) {
    private data class Snapshot(val pending: Double, val lagSeconds: Double, val failed: Double, val blocked: Double)

    @Volatile private var snapshot: Snapshot? = null
    @Volatile private var takenAt = 0L

    init {
        gauge(registry, "outbox.pending", "Unpublished outbox rows") { it.pending }
        gauge(registry, "outbox.lag", "Seconds since the oldest unpublished row was written", "seconds") { it.lagSeconds }
        gauge(registry, "outbox.failed", "Rows that exhausted their attempts and wait for a manual requeue") { it.failed }
        gauge(registry, "outbox.blocked", "Unpublished rows held behind a failed or backing-off row of the same key") { it.blocked }
    }

    private fun gauge(registry: MeterRegistry, name: String, description: String, unit: String? = null, value: (Snapshot) -> Double) {
        Gauge.builder(name, this) { value(it.current()) }.description(description).baseUnit(unit).register(registry)
    }

    @Synchronized
    private fun current(): Snapshot {
        val now = System.nanoTime()
        snapshot?.takeIf { now - takenAt < cacheTtl.toNanos() }?.let { return it }
        return query().also {
            snapshot = it
            takenAt = now
        }
    }

    private fun query(): Snapshot = jdbc.queryForObject(
        """
        select count(*) as pending,
               coalesce(extract(epoch from now() - min(o.created_at)), 0) as lag,
               count(*) filter (where o.failed_at is not null) as failed,
               count(*) filter (where exists (select 1 from outbox p
                                              where p.aggregate_id = o.aggregate_id and p.published_at is null and p.id < o.id
                                                and (p.failed_at is not null or p.next_attempt_at > now()))) as blocked
        from outbox o
        where o.published_at is null
        """,
    ) { rs, _ -> Snapshot(rs.getDouble("pending"), rs.getDouble("lag"), rs.getDouble("failed"), rs.getDouble("blocked")) }!!
}
