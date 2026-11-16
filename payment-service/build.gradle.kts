plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.kotlin.jpa)
    alias(libs.plugins.jib)
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

dependencies {
    implementation(project(":outbox-lib"))
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    runtimeOnly(libs.cloud.sql.postgres.socket.factory)

    testImplementation(testFixtures(project(":outbox-lib")))
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

jib {
    from.image = "eclipse-temurin:25-jre"
    to.image = "${System.getenv("IMAGE_REPO") ?: "local"}/payment-service"
    container.ports = listOf("8082")
}
