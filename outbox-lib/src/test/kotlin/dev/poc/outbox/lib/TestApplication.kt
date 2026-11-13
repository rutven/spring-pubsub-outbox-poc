package dev.poc.outbox.lib

import dev.poc.outbox.lib.test.Containers
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

@SpringBootApplication
class TestApplication

/** Base for lib integration tests: schema `orders`, relay scheduler off. */
@SpringBootTest(
    classes = [TestApplication::class],
    properties = ["poc.outbox.relay-enabled=false", "spring.liquibase.change-log=classpath:db/changelog/test-changelog.yaml"],
)
abstract class LibIntegrationTest {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            Containers.register(registry, "orders", "order_svc")
            registry.add("spring.liquibase.default-schema") { "orders" }
            registry.add("spring.jpa.properties.hibernate.default_schema") { "orders" }
        }
    }
}
