package dev.poc.outbox.order

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import tools.jackson.databind.json.JsonMapper
import java.util.UUID
import kotlin.test.assertEquals

@AutoConfigureMockMvc
class OrderControllerTest : OrderIntegrationTest() {
    @Autowired lateinit var mvc: MockMvc
    @Autowired lateinit var json: JsonMapper

    private fun create(amount: String): UUID {
        val body = mvc.post("/orders") {
            contentType = MediaType.APPLICATION_JSON
            content = """{"amount": $amount}"""
        }.andExpect { status { isCreated() } }.andReturn().response.contentAsString
        return UUID.fromString(json.readTree(body)["id"].asString())
    }

    @Test
    fun `create order persists NEW and writes OrderCreated to outbox`() {
        val id = create("100.00")

        mvc.get("/orders/$id").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("NEW") }
            jsonPath("$.amount") { value(100.0) }
        }
        assertEquals(listOf("OrderCreated"), outboxTypes(id))
        assertEquals(
            "100.00",
            jdbc.queryForObject("select payload->>'amount' from outbox where aggregate_id = ?", String::class.java, id.toString()),
        )
        assertEquals("order-events", jdbc.queryForObject("select topic from outbox where aggregate_id = ?", String::class.java, id.toString()))
    }

    @Test
    fun `get unknown order returns 404`() {
        mvc.get("/orders/${UUID.randomUUID()}").andExpect { status { isNotFound() } }
    }

    @Test
    fun `cancel NEW order writes OrderCancelled`() {
        val id = create("10")

        mvc.post("/orders/$id/cancel").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("CANCELLED") }
        }
        assertEquals(listOf("OrderCreated", "OrderCancelled"), outboxTypes(id))
    }

    @Test
    fun `cancel PAID order returns 409 and writes nothing`() {
        val id = create("10")
        jdbc.update("update orders set status = 'PAID' where id = ?", id)

        mvc.post("/orders/$id/cancel").andExpect { status { isConflict() } }
        assertEquals(listOf("OrderCreated"), outboxTypes(id))
    }

    @Test
    fun `cancel unknown order returns 404`() {
        mvc.post("/orders/${UUID.randomUUID()}/cancel").andExpect { status { isNotFound() } }
    }

    @Test
    fun `amount must be positive`() {
        mvc.post("/orders") {
            contentType = MediaType.APPLICATION_JSON
            content = """{"amount": 0}"""
        }.andExpect { status { isBadRequest() } }
    }
}
