resource "google_secret_manager_secret" "scheduler_zen_config" {
  secret_id = "scheduler-zen-config"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "config" {
  secret = google_secret_manager_secret.scheduler_zen_config.id
  secret_data = file("${path.module}/${var.environment}-config.env")
}