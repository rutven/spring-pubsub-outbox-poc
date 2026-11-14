output "order_url" {
  value = google_cloud_run_v2_service.svc["order"].uri
}

output "image_repo" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.poc.repository_id}"
}

output "sql_connection_name" {
  value = google_sql_database_instance.poc.connection_name
}

output "db_iam_users" {
  value = { for k, u in google_sql_user.svc : k => u.name }
}

output "postgres_password" {
  value     = random_password.postgres.result
  sensitive = true
}
