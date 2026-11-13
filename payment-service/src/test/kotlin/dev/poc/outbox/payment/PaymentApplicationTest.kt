package dev.poc.outbox.payment

import com.google.cloud.spring.pubsub.core.PubSubTemplate
import dev.poc.outbox.lib.test.Containers
import jakarta.persistence.EntityManagerFactory
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

@SpringBootTest
class PaymentApplicationTest(@Autowired private val context: ApplicationContext) {

    @Test
    fun contextLoads() {
        context.getBean(PubSubTemplate::class.java)
        context.getBean(EntityManagerFactory::class.java)
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) = Containers.register(registry, "payments", "payment_svc")
    }
}
