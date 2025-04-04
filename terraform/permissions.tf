resource "google_service_account" "scheduler_zen" {
  account_id   = "scheduler-zen-sa"
  display_name = "Scheduler Zen Service Account"
  description  = "Service account for Scheduler Zen"
}

resource "google_project_iam_custom_role" "scheduler_zen_mig_manager" {
  role_id     = "scheduler_zen_mig_manager"
  title       = "Scheduler Zen MIG Manager"
  description = "Role for Scheduler Zen MIG operations"
  permissions = [
    "compute.instanceGroupManagers.get",
    "compute.instanceGroupManagers.list",
    "compute.instanceGroupManagers.update",
    "compute.instances.list"
  ]
}

resource "google_project_iam_member" "scheduler_zen_project" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/secretmanager.secretAccessor",
    "roles/secretmanager.viewer",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/cloudsql.client"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.scheduler_zen.email}"
}

resource "google_project_iam_member" "scheduler_zen_custom_roles" {
  for_each = toset([
    google_project_iam_custom_role.scheduler_zen_mig_manager.role_id
  ])

  project = var.project_id
  role    = "projects/${var.project_id}/roles/${each.key}"
  member  = "serviceAccount:${google_service_account.scheduler_zen.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_zen_jobs_results" {
  for_each = toset([
    "lum-${var.environment}-pipeline-zen-jobs-us",
    "lum-${var.environment}-pipeline-zen-jobs-asia",
    "lum-${var.environment}-pipeline-zen-jobs-europe",
    "lum-${var.environment}-pipeline-zen-jobs-me-west1",
    "lum-local-pipeline-zen-jobs-us",
  ])

  bucket = each.value
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.scheduler_zen.email}"
}

resource "google_artifact_registry_repository_iam_member" "scheduler_zen" {
  member     = "serviceAccount:${google_service_account.scheduler_zen.email}"
  repository = "lum-docker-images"
  project    = var.resources_project_id
  location   = var.region
  role       = "roles/artifactregistry.reader"
}