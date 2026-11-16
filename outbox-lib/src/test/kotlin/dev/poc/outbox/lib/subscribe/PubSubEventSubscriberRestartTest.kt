package dev.poc.outbox.lib.subscribe

import com.google.api.gax.core.NoCredentialsProvider
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Subscriber
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage
import com.google.cloud.spring.pubsub.support.SubscriberFactory
import com.google.pubsub.v1.ProjectSubscriptionName
import dev.poc.outbox.lib.inbox.InboxProcessor
import dev.poc.outbox.lib.inbox.InboxRepository
import dev.poc.outbox.lib.test.Containers
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.observation.ObservationRegistry
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
import kotlin.test.assertTrue

class PubSubEventSubscriberRestartTest {
    /** Returns subscribers that already failed before the caller can attach a listener. */
    private class FailedSubscriberTemplate : PubSubSubscriberTemplate(mock(SubscriberFactory::class.java)) {
        val calls = AtomicInteger()

        override fun subscribe(subscription: String, consumer: Consumer<BasicAcknowledgeablePubsubMessage>): Subscriber {
            calls.incrementAndGet()
            return Containers.withChannel { transport ->
                Subscriber.newBuilder(ProjectSubscriptionName.of(Containers.PROJECT, "missing-subscription"), MessageReceiver { _, reply -> reply.ack() })
                    .setChannelProvider(transport)
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build()
                    .also { it.startAsync() }
                    .also { runCatching { it.awaitTerminated(10, TimeUnit.SECONDS) } }
            }
        }
    }

    private val handler = object : EventHandler {
        override val subscription = "missing-subscription"
        override fun handles(eventType: String) = true
        override fun handle(event: InboundEvent) = Unit
    }

    @Test
    fun `subscriber that failed before the listener was attached is resubscribed`() {
        val template = FailedSubscriberTemplate()
        val subscriber = PubSubEventSubscriber(
            template, listOf(handler), InboxProcessor(mock(InboxRepository::class.java)),
            ObservationRegistry.NOOP, SimpleMeterRegistry(), 0.0,
        )

        subscriber.start()
        try {
            await atMost Duration.ofSeconds(20) untilAsserted { assertTrue(template.calls.get() >= 2, "calls=${template.calls}") }
        } finally {
            subscriber.destroy()
        }
    }
}
