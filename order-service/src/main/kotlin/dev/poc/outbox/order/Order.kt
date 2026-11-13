package dev.poc.outbox.order

import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.Version
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

enum class OrderStatus { NEW, PAID, CANCELLED }

@Entity
@Table(name = "orders")
class Order(
    @Id
    val id: UUID = UUID.randomUUID(),
    val amount: BigDecimal,
    @Enumerated(EnumType.STRING)
    var status: OrderStatus = OrderStatus.NEW,
    var cancelReason: String? = null,
    val createdAt: Instant = Instant.now(),
    @Version
    var version: Long = 0,
)
