resource "google_monitoring_notification_channel" "email" {
  display_name = "Outbox alerts"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
  depends_on = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "outbox_failed" {
  display_name = "Outbox failed events"
  combiner     = "OR"

  conditions {
    display_name = "outbox_failed > 0"
    condition_prometheus_query_language {
      query               = "max(outbox_failed) > 0"
      duration            = "60s"
      evaluation_interval = "60s"
    }
  }

  documentation {
    content = "An outbox row exhausted its publish attempts and blocks its aggregate. See the README poison-event runbook."
  }

  notification_channels = [google_monitoring_notification_channel.email.id]
}
