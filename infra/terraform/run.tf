resource "google_secret_manager_secret" "collector_config" {
  secret_id = "otel-collector-config"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "collector_config" {
  secret      = google_secret_manager_secret.collector_config.id
  secret_data = file("${path.module}/../otel/collector.yaml")
}

resource "google_cloud_run_v2_service" "svc" {
  for_each            = local.services
  name                = each.value.name
  location            = var.region
  ingress             = each.value.ingress
  deletion_protection = false

  template {
    service_account = google_service_account.svc[each.key].email

    # Relay and subscriber run in the background, so keep one instance with CPU always allocated.
    scaling {
      min_instance_count = 1
      max_instance_count = 2
    }

    containers {
      name       = "app"
      image      = each.value.image
      depends_on = ["collector"]

      ports {
        container_port = each.value.port
      }

      resources {
        cpu_idle          = false
        startup_cpu_boost = true
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
      }

      env {
        name  = "SPRING_PROFILES_ACTIVE"
        value = "gcp"
      }
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "CLOUD_SQL_INSTANCE"
        value = google_sql_database_instance.poc.connection_name
      }
      env {
        name  = "DB_IAM_USER"
        value = google_sql_user.svc[each.key].name
      }

      startup_probe {
        initial_delay_seconds = 10
        period_seconds        = 5
        failure_threshold     = 24
        http_get {
          path = "/actuator/health"
        }
      }
    }

    containers {
      name  = "collector"
      image = local.collector_image
      args  = ["--config=/etc/otelcol-google/config.yaml"]

      resources {
        limits = {
          cpu    = "0.5"
          memory = "256Mi"
        }
      }

      volume_mounts {
        name       = "collector-config"
        mount_path = "/etc/otelcol-google"
      }

      startup_probe {
        http_get {
          path = "/"
          port = 13133
        }
      }
    }

    volumes {
      name = "collector-config"
      secret {
        secret = google_secret_manager_secret.collector_config.secret_id
        items {
          version = google_secret_manager_secret_version.collector_config.version
          path    = "config.yaml"
        }
      }
    }
  }

  depends_on = [google_project_iam_member.svc, google_secret_manager_secret_iam_member.collector_config]
}
