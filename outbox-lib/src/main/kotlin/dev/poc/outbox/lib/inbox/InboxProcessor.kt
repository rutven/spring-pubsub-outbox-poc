package dev.poc.outbox.lib.inbox

import dev.poc.outbox.lib.subscribe.EventHandler
import dev.poc.outbox.lib.subscribe.InboundEvent
import org.springframework.transaction.annotation.Transactional

/** Runs a handler at most once per eventId: the inbox row, business change and outbox rows commit together. */
open class InboxProcessor(private val inbox: InboxRepository) {
    /** Returns false for a duplicate delivery, which is skipped. */
    @Transactional
    open fun process(event: InboundEvent, handler: EventHandler): Boolean {
        if (inbox.insertIfAbsent(event.eventId) == 0) return false
        handler.handle(event)
        return true
    }
}
