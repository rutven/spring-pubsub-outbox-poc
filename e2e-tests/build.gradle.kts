plugins {
    alias(libs.plugins.kotlin.jvm)
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

dependencies {
    testImplementation(project(":order-service"))
    testImplementation(project(":payment-service"))
    testImplementation(testFixtures(project(":outbox-lib")))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
