package dev.poc.outbox.order

import dev.poc.outbox.lib.outbox.OutboxWriter
import dev.poc.outbox.order.events.ORDER_EVENTS_TOPIC
import dev.poc.outbox.order.events.OrderCancelled
import dev.poc.outbox.order.events.OrderCreated
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.util.UUID

class OrderNotFoundException(id: UUID) : RuntimeException("Order $id not found")

class OrderStateException(message: String) : RuntimeException(message)

@Service
@Transactional
class OrderService(private val orders: OrderRepository, private val outbox: OutboxWriter) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun create(amount: BigDecimal): Order {
        val order = orders.save(Order(amount = amount))
        publish(order, OrderCreated(order.id, order.amount))
        return order
    }

    @Transactional(readOnly = true)
    fun get(id: UUID): Order? = orders.findByIdOrNull(id)

    fun cancel(id: UUID): Order {
        val order = orders.findByIdOrNull(id) ?: throw OrderNotFoundException(id)
        if (order.status != OrderStatus.NEW) throw OrderStateException("Order $id is ${order.status}")
        order.status = OrderStatus.CANCELLED
        order.cancelReason = "cancelled by customer"
        publish(order, OrderCancelled(order.id))
        return order
    }

    fun markPaid(id: UUID) = transition(id, OrderStatus.PAID, reason = null)

    fun markPaymentFailed(id: UUID, reason: String) = transition(id, OrderStatus.CANCELLED, reason)

    /** Payment outcomes apply only to NEW orders; anything else is logged and left as is. */
    private fun transition(id: UUID, target: OrderStatus, reason: String?) {
        val order = orders.findByIdOrNull(id)
        if (order == null) {
            log.warn("Payment event for unknown order {}, ignored", id)
            return
        }
        if (order.status != OrderStatus.NEW) {
            log.info("Order {} is {}, ignoring transition to {}", id, order.status, target)
            return
        }
        order.status = target
        order.cancelReason = reason
    }

    private fun publish(order: Order, event: Any) =
        outbox.append(ORDER_EVENTS_TOPIC, "Order", order.id.toString(), event.javaClass.simpleName, event)
}
