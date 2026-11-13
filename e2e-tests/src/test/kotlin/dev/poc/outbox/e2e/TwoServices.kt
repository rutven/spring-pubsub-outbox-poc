package dev.poc.outbox.e2e

import dev.poc.outbox.lib.outbox.CrashAction
import dev.poc.outbox.lib.test.Containers
import dev.poc.outbox.order.OrderApplication
import dev.poc.outbox.payment.PaymentApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import kotlin.reflect.KClass

val E2E_TIMEOUT: Duration = Duration.ofSeconds(60)

/** HTTP client for one order-service instance. */
class OrderClient(port: Int) {
    private val base = "http://localhost:$port/orders"
    private val http = HttpClient.newHttpClient()
    private val json = JsonMapper.builder().build()

    fun create(amount: String): UUID {
        val response = send(HttpRequest.newBuilder(URI.create(base)).header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("""{"amount": $amount}""")))
        check(response.statusCode() == 201) { "create returned ${response.statusCode()}: ${response.body()}" }
        return UUID.fromString(json.readTree(response.body())["id"].asString())
    }

    fun get(id: UUID): Map<String, Any?> {
        val response = send(HttpRequest.newBuilder(URI.create("$base/$id")).GET())
        check(response.statusCode() == 200) { "get returned ${response.statusCode()}" }
        @Suppress("UNCHECKED_CAST")
        return json.readValue(response.body(), Map::class.java) as Map<String, Any?>
    }

    private fun send(request: HttpRequest.Builder) = http.send(request.build(), HttpResponse.BodyHandlers.ofString())
}

/**
 * Order and payment apps in one JVM against the shared [Containers]. Both apps' resources share one classpath, so
 * every service-specific property is passed as a command-line argument, which outranks the (shadowed) application.yaml.
 */
class TwoServices private constructor(
    private val orderContexts: List<ConfigurableApplicationContext>,
    private val paymentContext: ConfigurableApplicationContext,
) : AutoCloseable {
    val orderInstances = orderContexts.map { OrderClient(it.environment.getProperty("local.server.port")!!.toInt()) }
    val order = orderInstances.first()

    private val orderJdbc = orderContexts.first().getBean(JdbcTemplate::class.java)
    private val paymentJdbc = paymentContext.getBean(JdbcTemplate::class.java)

    fun paymentRows(orderId: UUID): Int =
        paymentJdbc.queryForObject("select count(*) from payments where order_id = ?", Int::class.java, orderId)!!

    fun publishedOrderCreatedRows(orderId: UUID): Int = orderJdbc.queryForObject(
        "select count(*) from outbox where aggregate_id = ? and event_type = 'OrderCreated' and published_at is not null",
        Int::class.java, orderId.toString(),
    )!!

    override fun close() {
        orderContexts.forEach { it.close() }
        paymentContext.close()
    }

    companion object {
        fun start(
            orderArgs: Map<String, String> = emptyMap(),
            paymentArgs: Map<String, String> = emptyMap(),
            orderCrashAction: CrashAction? = null,
            extraOrderInstances: Int = 0,
        ): TwoServices {
            val payment = run(PaymentApplication::class, "payment", "payments", paymentArgs, null)
            val orders = (0..extraOrderInstances).map { run(OrderApplication::class, "order", "orders", orderArgs, orderCrashAction) }
            return TwoServices(orders, payment)
        }

        private fun run(app: KClass<*>, name: String, schema: String, overrides: Map<String, String>, crashAction: CrashAction?): ConfigurableApplicationContext {
            val args = Containers.properties(schema, "${name}_svc") + mapOf(
                "spring.application.name" to "$name-service",
                "server.port" to "0",
                "spring.liquibase.change-log" to "classpath:db/changelog/$name-changelog.yaml",
                "spring.liquibase.default-schema" to schema,
                "spring.jpa.properties.hibernate.default_schema" to schema,
            ) + overrides
            return SpringApplicationBuilder(app.java)
                .initializers({ context -> crashAction?.let { context.beanFactory.registerSingleton("crashAction", it) } })
                .run(*args.map { (key, value) -> "--$key=$value" }.toTypedArray())
        }
    }
}
