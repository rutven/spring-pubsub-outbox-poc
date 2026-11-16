package dev.poc.outbox.lib.outbox

import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import org.springframework.beans.factory.ObjectProvider
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.json.JsonMapper
import java.util.UUID

/** Appends an event to the outbox in the caller's transaction. */
open class OutboxWriter(
    private val repository: OutboxRepository,
    private val jsonMapper: JsonMapper,
    private val tracer: ObjectProvider<Tracer>,
    private val propagator: ObjectProvider<Propagator>,
) {
    @Transactional(propagation = Propagation.MANDATORY)
    open fun append(topic: String, aggregateType: String, aggregateId: String, eventType: String, payload: Any): OutboxEvent =
        repository.save(
            OutboxEvent(
                eventId = UUID.randomUUID(),
                topic = topic,
                aggregateType = aggregateType,
                aggregateId = aggregateId,
                eventType = eventType,
                payload = jsonMapper.writeValueAsString(payload),
                headers = traceHeaders(),
            ),
        )

    private fun traceHeaders(): Map<String, String> {
        val context = tracer.ifAvailable?.currentTraceContext()?.context() ?: return emptyMap()
        val propagator = propagator.ifAvailable ?: return emptyMap()
        val headers = mutableMapOf<String, String>()
        propagator.inject(context, headers) { carrier, key, value -> carrier!![key] = value }
        return headers
    }
}
