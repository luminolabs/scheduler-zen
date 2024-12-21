resource "google_project_service" "service_networking" {
  project = var.project_id
  service = "servicenetworking.googleapis.com"
}

resource "google_compute_global_address" "private_vpc_range" {
  name         = "private-vpc-range"
  project      = var.project_id
  purpose      = "VPC_PEERING"
  address_type = "INTERNAL"
  prefix_length = 16
  network = "projects/${var.project_id}/global/networks/default"

  depends_on = [
    google_project_service.service_networking
  ]
}

resource "google_service_networking_connection" "private_service_connection" {
  network = "projects/${var.project_id}/global/networks/default"
  service = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [
    google_compute_global_address.private_vpc_range.name
  ]

  depends_on = [
    google_compute_global_address.private_vpc_range
  ]
}

resource "google_sql_database_instance" "postgres" {
  name                = "zen-db"
  database_version    = "POSTGRES_17"
  region              = var.region
  deletion_protection = true

  settings {
    edition           = "ENTERPRISE"
    tier              = "db-custom-1-3840"  # 1 vCPU, 3.75 GB RAM
    availability_type = "ZONAL"
    disk_size         = 100
    disk_type         = "PD_HDD"
    disk_autoresize   = true

    backup_configuration {
      enabled                        = true
      start_time                     = "01:00"
      point_in_time_recovery_enabled = true
      backup_retention_settings {
        retained_backups = 7
      }
    }

    maintenance_window {
      day          = 6
      hour         = 1
      update_track = "stable"
    }

    ip_configuration {
      ipv4_enabled                                  = false  # Disable public IP
      private_network                               = "projects/${var.project_id}/global/networks/default"
      enable_private_path_for_google_cloud_services = true
    }
  }

  depends_on = [
    google_service_networking_connection.private_service_connection
  ]
}

resource "google_sql_database" "default" {
  name     = "scheduler_zen"
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "default" {
  name     = "scheduler_zen"
  instance = google_sql_database_instance.postgres.name
  password = local.zen_db_password
}