resource "google_service_account" "svc" {
  for_each     = local.services
  account_id   = "${each.key}-svc"
  display_name = each.value.name
  depends_on   = [google_project_service.apis]
}

locals {
  project_roles = [
    "roles/cloudsql.client",
    "roles/cloudsql.instanceUser",
    "roles/cloudtrace.agent",
    "roles/monitoring.metricWriter",
    "roles/logging.logWriter",
  ]
  svc_roles = { for pair in setproduct(keys(local.services), local.project_roles) : "${pair[0]}/${pair[1]}" => pair }
}

resource "google_project_iam_member" "svc" {
  for_each = local.svc_roles
  project  = var.project_id
  role     = each.value[1]
  member   = google_service_account.svc[each.value[0]].member
}

resource "google_pubsub_topic_iam_member" "publisher" {
  for_each = local.services
  topic    = google_pubsub_topic.events[each.value.topic].id
  role     = "roles/pubsub.publisher"
  member   = google_service_account.svc[each.key].member
}

resource "google_pubsub_subscription_iam_member" "subscriber" {
  for_each     = local.services
  subscription = google_pubsub_subscription.events[each.value.subscription].id
  role         = "roles/pubsub.subscriber"
  member       = google_service_account.svc[each.key].member
}

resource "google_secret_manager_secret_iam_member" "collector_config" {
  for_each  = local.services
  secret_id = google_secret_manager_secret.collector_config.id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.svc[each.key].member
}
