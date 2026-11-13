package dev.poc.outbox.order

import dev.poc.outbox.lib.test.Containers
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

@SpringBootTest(properties = ["poc.outbox.relay-enabled=false"])
abstract class OrderIntegrationTest {
    @Autowired lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun cleanTables() {
        jdbc.update("delete from outbox")
        jdbc.update("delete from inbox")
        jdbc.update("delete from orders")
    }

    fun outboxTypes(orderId: Any): List<String?> =
        jdbc.queryForList("select event_type from outbox where aggregate_id = ? order by id", String::class.java, orderId.toString())

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = Containers.register(registry, "orders", "order_svc")
    }
}
