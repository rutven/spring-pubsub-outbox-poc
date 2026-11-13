package dev.poc.outbox.lib.outbox

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

interface OutboxRepository : JpaRepository<OutboxEvent, Long> {
    /**
     * Step 1 of the claim: takes a transaction-scoped advisory lock on up to [limit] keys that have a claimable row,
     * oldest first. A key is unclaimable while an earlier unpublished row of it is failed or in backoff.
     * Keys locked by another relay are skipped.
     */
    @Query(
        nativeQuery = true,
        value = """
            select k from (
                select o.aggregate_id as k, min(o.id) as m from outbox o
                where o.published_at is null and o.failed_at is null
                  and (o.next_attempt_at is null or o.next_attempt_at <= now())
                  and not exists (select 1 from outbox p
                                  where p.aggregate_id = o.aggregate_id and p.published_at is null and p.id < o.id
                                    and (p.failed_at is not null or p.next_attempt_at > now()))
                group by o.aggregate_id
                order by m
                limit :limit
            ) s
            where pg_try_advisory_xact_lock(hashtext(k))
        """,
    )
    fun lockClaimableKeys(limit: Int): List<String>

    /**
     * Step 2 of the claim, a separate statement so its snapshot sees every commit made before the locks were taken.
     */
    @Query(
        nativeQuery = true,
        value = """
            select o.* from outbox o
            where o.aggregate_id in (:keys)
              and o.published_at is null and o.failed_at is null
              and (o.next_attempt_at is null or o.next_attempt_at <= now())
              and not exists (select 1 from outbox p
                              where p.aggregate_id = o.aggregate_id and p.published_at is null and p.id < o.id
                                and (p.failed_at is not null or p.next_attempt_at > now()))
            order by o.id
            limit :limit
            for update skip locked
        """,
    )
    fun claim(keys: Collection<String>, limit: Int): List<OutboxEvent>
}
