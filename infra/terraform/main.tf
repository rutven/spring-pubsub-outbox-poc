data "google_project" "this" {}

locals {
  apis = ["run", "sqladmin", "pubsub", "artifactregistry", "iam", "cloudtrace", "monitoring", "logging", "secretmanager"]

  services = {
    order = {
      name         = "order-service"
      image        = var.order_image
      port         = 8081
      ingress      = "INGRESS_TRAFFIC_ALL"
      topic        = "order-events"
      subscription = "order-service.payment-events"
    }
    payment = {
      name         = "payment-service"
      image        = var.payment_image
      port         = 8082
      ingress      = "INGRESS_TRAFFIC_INTERNAL_ONLY"
      topic        = "payment-events"
      subscription = "payment-service.order-events"
    }
  }

  collector_image = "us-docker.pkg.dev/cloud-ops-agents-artifacts/google-cloud-opentelemetry-collector/otelcol-google:0.164.0"
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = "${each.key}.googleapis.com"
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "poc" {
  repository_id = "poc"
  location      = var.region
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}
