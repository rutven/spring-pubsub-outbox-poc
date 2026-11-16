package dev.poc.outbox.order

import jakarta.validation.Valid
import jakarta.validation.constraints.DecimalMin
import org.springframework.http.HttpStatus
import org.springframework.orm.ObjectOptimisticLockingFailureException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import java.math.BigDecimal
import java.util.UUID

data class CreateOrderRequest(
    @field:DecimalMin(value = "0", inclusive = false)
    val amount: BigDecimal,
)

data class OrderDto(val id: UUID, val amount: BigDecimal, val status: OrderStatus, val cancelReason: String?) {
    constructor(order: Order) : this(order.id, order.amount, order.status, order.cancelReason)
}

@RestController
@RequestMapping("/orders")
class OrderController(private val orders: OrderService) {
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun create(@Valid @RequestBody request: CreateOrderRequest) = OrderDto(orders.create(request.amount))

    @GetMapping("/{id}")
    fun get(@PathVariable id: UUID) = OrderDto(orders.get(id) ?: throw OrderNotFoundException(id))

    @PostMapping("/{id}/cancel")
    fun cancel(@PathVariable id: UUID) = OrderDto(orders.cancel(id))

    @ExceptionHandler(OrderNotFoundException::class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    fun notFound() = Unit

    @ExceptionHandler(OrderStateException::class, ObjectOptimisticLockingFailureException::class)
    @ResponseStatus(HttpStatus.CONFLICT)
    fun conflict() = Unit
}
