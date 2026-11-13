package dev.poc.outbox.lib

import dev.poc.outbox.lib.outbox.OutboxRepository
import dev.poc.outbox.lib.outbox.OutboxWriter
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.context.annotation.Bean
import tools.jackson.databind.json.JsonMapper

/** Registers the lib's entities/repositories (via the auto-configuration package) and beans. */
@AutoConfiguration
@AutoConfigurationPackage
class OutboxAutoConfiguration {
    @Bean
    fun outboxWriter(
        repository: OutboxRepository,
        jsonMapper: JsonMapper,
        tracer: ObjectProvider<Tracer>,
        propagator: ObjectProvider<Propagator>,
    ) = OutboxWriter(repository, jsonMapper, tracer, propagator)
}
