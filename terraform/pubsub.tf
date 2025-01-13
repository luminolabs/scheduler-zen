resource "google_pubsub_subscription" "job_heartbeats" {
  name    = "pipeline-zen-jobs-heartbeats-scheduler"
  topic   = "pipeline-zen-jobs-heartbeats"
  project = var.project_id

  message_retention_duration = "14400s"  # 4h
  retain_acked_messages = false
  ack_deadline_seconds = 60
  expiration_policy {
    ttl = ""  # Never expire
  }
  retry_policy {
    minimum_backoff = "0s"
    maximum_backoff = "60s"
  }
}

resource "google_pubsub_subscription" "job_heartbeats_local" {
  count  = var.environment == "dev" ? 1 : 0

  name    = "pipeline-zen-jobs-heartbeats-local-scheduler"
  topic   = "pipeline-zen-jobs-heartbeats-local"
  project = var.project_id

  message_retention_duration = "14400s" # 4h
  retain_acked_messages = false
  ack_deadline_seconds = 60
  expiration_policy {
    ttl = ""  # Never expire
  }
  retry_policy {
    minimum_backoff = "0s"
    maximum_backoff = "60s"
  }
}