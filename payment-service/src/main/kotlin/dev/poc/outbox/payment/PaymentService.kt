package dev.poc.outbox.payment

import dev.poc.outbox.lib.outbox.OutboxWriter
import dev.poc.outbox.payment.events.PAYMENT_EVENTS_TOPIC
import dev.poc.outbox.payment.events.PaymentCompleted
import dev.poc.outbox.payment.events.PaymentFailed
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.util.UUID

@Service
class PaymentService(private val payments: PaymentRepository, private val outbox: OutboxWriter) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** Approves amounts up to [LIMIT], declines the rest. Runs in the inbox transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    fun charge(orderId: UUID, amount: BigDecimal): Payment {
        payments.findByOrderId(orderId)?.let {
            log.info("Order {} already has payment {}, skipping", orderId, it.id)
            return it
        }
        val payment = if (amount <= LIMIT) {
            Payment(orderId = orderId, amount = amount, status = PaymentStatus.APPROVED)
        } else {
            Payment(orderId = orderId, amount = amount, status = PaymentStatus.DECLINED, reason = "amount exceeds limit")
        }
        payments.save(payment)
        val event = when (payment.status) {
            PaymentStatus.APPROVED -> PaymentCompleted(payment.id, orderId, amount)
            PaymentStatus.DECLINED -> PaymentFailed(payment.id, orderId, payment.reason!!)
        }
        outbox.append(PAYMENT_EVENTS_TOPIC, "Payment", orderId.toString(), event.javaClass.simpleName, event)
        return payment
    }

    private companion object {
        val LIMIT = BigDecimal("1000")
    }
}
