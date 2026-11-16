package dev.poc.outbox.lib.publish

import com.google.cloud.spring.pubsub.core.publisher.PubSubPublisherTemplate
import com.google.cloud.spring.pubsub.support.PublisherFactory
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import java.util.concurrent.CompletableFuture

class PubSubMessagePublisher(
    private val template: PubSubPublisherTemplate,
    private val publisherFactory: PublisherFactory,
) : MessagePublisher {
    override fun publish(message: OutboundMessage): CompletableFuture<String> =
        template.publish(
            message.topic,
            PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(message.data))
                .setOrderingKey(message.orderingKey)
                .putAllAttributes(message.attributes)
                .build(),
        )

    override fun resume(topic: String, orderingKey: String) {
        publisherFactory.createPublisher(topic).resumePublish(orderingKey)
    }
}
