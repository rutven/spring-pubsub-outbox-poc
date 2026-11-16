package dev.poc.outbox.e2e

import dev.poc.outbox.lib.outbox.CrashAction
import dev.poc.outbox.lib.subscribe.PubSubEventSubscriber
import io.micrometer.core.instrument.MeterRegistry
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FailureDemoTest {
    private var services: TwoServices? = null

    @AfterEach
    fun stop() {
        services?.close()
    }

    @Test
    fun `crash after publish leads to redelivery deduplicated by inbox`() {
        val crashes = AtomicInteger()
        val crashOnce = CrashAction { if (crashes.getAndIncrement() == 0) throw IllegalStateException("simulated crash") }
        val s = TwoServices.start(orderArgs = mapOf("poc.chaos.crash-after-publish" to "true"), orderCrashAction = crashOnce)
        services = s

        val id = s.order.create("100")

        await atMost E2E_TIMEOUT untilAsserted {
            assertEquals("PAID", s.order.get(id)["status"])
            // payment may consume the crashed attempt's message before the retry commits published_at
            assertEquals(1, s.publishedOrderCreatedRows(id))
        }
        assertEquals(1, s.paymentRows(id))
        assertEquals(true, crashes.get() >= 1)
    }

    @Test
    fun `consumer failures are retried until success`() {
        val s = TwoServices.start(paymentArgs = mapOf("poc.chaos.consumer-fail-rate" to "1.0"))
        services = s
        val chaos = { s.paymentBean(MeterRegistry::class).find("inbox.messages").tag("outcome", "chaos").counter()?.count() ?: 0.0 }

        val ids = (1..5).map { s.order.create("10") }

        // Once nacks happened, stop failing: redelivery backoff doubles from 10s, so endless chaos would only test luck.
        await atMost E2E_TIMEOUT untilAsserted { assertTrue(chaos() >= ids.size) }
        s.paymentBean(PubSubEventSubscriber::class).consumerFailRate = 0.0
        await atMost E2E_TIMEOUT untilAsserted { ids.forEach { assertEquals("PAID", s.order.get(it)["status"], "order $it") } }
    }

    @Test
    fun `two order instances relay without duplicates`() {
        val s = TwoServices.start(extraOrderInstances = 1)
        services = s

        val ids = (1..50).map { i -> s.orderInstances[i % 2].create("10") }

        await atMost E2E_TIMEOUT untilAsserted {
            ids.forEach { assertEquals("PAID", s.order.get(it)["status"], "order $it") }
        }
        ids.forEach { assertEquals(1, s.paymentRows(it)) }
        ids.forEach { assertEquals(1, s.publishedOrderCreatedRows(it)) }
    }
}
