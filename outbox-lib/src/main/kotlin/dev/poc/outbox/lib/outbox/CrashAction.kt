package dev.poc.outbox.lib.outbox

/** Chaos hook run after publish and before commit when `poc.chaos.crash-after-publish=true`. */
fun interface CrashAction {
    fun crash()
}
