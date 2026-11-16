rootProject.name = "spring-pubsub-outbox-poc"

include("outbox-lib", "order-service", "payment-service", "e2e-tests")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
