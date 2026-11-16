package dev.poc.outbox.e2e

import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class OrderPaymentFlowTest {
    private val services = TwoServices.start()

    @AfterEach
    fun stop() = services.close()

    @Test
    fun `small order becomes PAID`() {
        val id = services.order.create("100")

        await atMost E2E_TIMEOUT untilAsserted { assertEquals("PAID", services.order.get(id)["status"]) }
    }

    @Test
    fun `large order becomes CANCELLED with reason`() {
        val id = services.order.create("5000")

        await atMost E2E_TIMEOUT untilAsserted {
            val order = services.order.get(id)
            assertEquals("CANCELLED", order["status"])
            assertEquals("amount exceeds limit", order["cancelReason"])
        }
    }
}
