package dev.poc.outbox.lib.outbox

import dev.poc.outbox.lib.LibIntegrationTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import kotlin.test.assertEquals

class OutboxPurgeJobTest : LibIntegrationTest() {
    @Autowired lateinit var job: OutboxPurgeJob
    @Autowired lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun clean() {
        jdbc.update("delete from outbox")
        jdbc.update("delete from inbox")
    }

    private fun outbox(key: String, publishedDaysAgo: Int?, createdDaysAgo: Int) = jdbc.update(
        """insert into outbox(event_id, topic, aggregate_type, aggregate_id, event_type, payload, created_at, published_at)
           values (gen_random_uuid(), 't', 'Order', ?, 'E', '{}', now() - make_interval(days => ?),
                   case when ?::int is null then null else now() - make_interval(days => ?::int) end)""",
        key, createdDaysAgo, publishedDaysAgo, publishedDaysAgo,
    )

    @Test
    fun `purges old published outbox rows and old inbox rows`() {
        outbox("old-published", publishedDaysAgo = 8, createdDaysAgo = 8)
        outbox("recent-published", publishedDaysAgo = 6, createdDaysAgo = 6)
        outbox("old-unpublished", publishedDaysAgo = null, createdDaysAgo = 30)
        jdbc.update("insert into inbox(event_id, received_at) values (gen_random_uuid(), now() - interval '15 days')")
        jdbc.update("insert into inbox(event_id, received_at) values (gen_random_uuid(), now() - interval '13 days')")

        assertEquals(1 to 1, job.purge())

        assertEquals(
            listOf("old-unpublished", "recent-published"),
            jdbc.queryForList("select aggregate_id from outbox order by aggregate_id", String::class.java),
        )
        assertEquals(1, jdbc.queryForObject("select count(*) from inbox", Int::class.java))
    }
}
