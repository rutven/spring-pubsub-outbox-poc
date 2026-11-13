plugins {
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.spring) apply false
    alias(libs.plugins.kotlin.jpa) apply false
}

subprojects {
    group = "dev.poc.outbox"
    version = "0.0.1-SNAPSHOT"

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        systemProperty("poc.rootDir", rootDir.absolutePath)
    }
}
