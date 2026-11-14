resource "google_sql_database_instance" "poc" {
  name                = "poc-pg"
  database_version    = "POSTGRES_18"
  region              = var.region
  deletion_protection = false

  settings {
    edition = "ENTERPRISE" # db-f1-micro is not available on ENTERPRISE_PLUS
    tier    = "db-f1-micro"

    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_sql_database" "poc" {
  name     = "poc"
  instance = google_sql_database_instance.poc.name
}

resource "random_password" "postgres" {
  length  = 24
  special = false
}

# Built-in admin used once to run infra/sql/bootstrap.sql in Cloud SQL Studio.
resource "google_sql_user" "postgres" {
  name     = "postgres"
  instance = google_sql_database_instance.poc.name
  password = random_password.postgres.result
}

resource "google_sql_user" "svc" {
  for_each = local.services
  name     = trimsuffix(google_service_account.svc[each.key].email, ".gserviceaccount.com")
  instance = google_sql_database_instance.poc.name
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}
