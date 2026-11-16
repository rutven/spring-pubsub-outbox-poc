locals {
  # subscription -> source topic
  subscriptions = { for s in local.services : s.subscription => s.topic == "order-events" ? "payment-events" : "order-events" }
  pubsub_agent  = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_topic" "events" {
  for_each   = toset(["order-events", "payment-events"])
  name       = each.key
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic" "dlq" {
  for_each = google_pubsub_topic.events
  name     = "${each.key}-dlq"
}

# Messages on a topic without subscriptions are dropped, so each DLQ gets one to inspect dead letters.
resource "google_pubsub_subscription" "dlq_inspect" {
  for_each                   = google_pubsub_topic.dlq
  name                       = "${each.value.name}.inspect"
  topic                      = each.value.id
  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "events" {
  for_each                = local.subscriptions
  name                    = each.key
  topic                   = google_pubsub_topic.events[each.value].id
  ack_deadline_seconds    = 15
  enable_message_ordering = true

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq[each.value].id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# The Pub/Sub service agent forwards dead letters: publish to the DLQ, ack on the source subscription.
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  for_each = google_pubsub_topic.dlq
  topic    = each.value.id
  role     = "roles/pubsub.publisher"
  member   = local.pubsub_agent
}

resource "google_pubsub_subscription_iam_member" "dlq_subscriber" {
  for_each     = google_pubsub_subscription.events
  subscription = each.value.id
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}
