package dev.poc.outbox.payment

import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

enum class PaymentStatus { APPROVED, DECLINED }

@Entity
@Table(name = "payments")
class Payment(
    @Id
    val id: UUID = UUID.randomUUID(),
    val orderId: UUID,
    val amount: BigDecimal,
    @Enumerated(EnumType.STRING)
    val status: PaymentStatus,
    val reason: String? = null,
    val createdAt: Instant = Instant.now(),
)
