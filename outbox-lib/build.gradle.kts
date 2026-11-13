import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    `java-library`
    `java-test-fixtures`
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.kotlin.jpa)
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

dependencies {
    api(platform(SpringBootPlugin.BOM_COORDINATES))
    api(platform(libs.spring.cloud.gcp.bom))
    api("org.springframework.boot:spring-boot-starter-data-jpa")
    api("org.springframework.boot:spring-boot-starter-liquibase")
    api("org.springframework.boot:spring-boot-starter-actuator")
    api("org.springframework.boot:spring-boot-starter-opentelemetry")
    api("org.springframework.boot:spring-boot-starter-jackson")
    api("com.google.cloud:spring-cloud-gcp-starter-pubsub")
    api("org.jetbrains.kotlin:kotlin-reflect")
    api("tools.jackson.module:jackson-module-kotlin")
    runtimeOnly("org.postgresql:postgresql")

    testFixturesApi(platform(SpringBootPlugin.BOM_COORDINATES))
    testFixturesApi(platform(libs.spring.cloud.gcp.bom))
    testFixturesApi("org.springframework.boot:spring-boot-starter-test")
    testFixturesApi("org.springframework.boot:spring-boot-testcontainers")
    testFixturesApi("org.testcontainers:testcontainers-postgresql")
    testFixturesApi("org.testcontainers:testcontainers-gcloud")
    testFixturesApi("com.google.cloud:google-cloud-pubsub")
    testFixturesApi("org.jetbrains.kotlin:kotlin-test-junit5")
    testFixturesApi("org.awaitility:awaitility-kotlin")

    testImplementation("io.micrometer:micrometer-observation-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
