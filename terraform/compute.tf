resource "google_compute_instance" "scheduler-zen" {

  name = "scheduler-zen-vm"
  machine_type = "e2-standard-2"
  project = var.project_id
  zone = var.zone

  boot_disk {
    auto_delete = true
    device_name = "scheduler-zen-disk"

    initialize_params {
      image = "projects/${var.resources_project_id}/global/images/scheduler-zen-image"
      size  = 250  # TODO: Legacy, from current scheduler image. Should be resized
      type  = "pd-balanced"
    }

    mode = "READ_WRITE"
  }

  can_ip_forward      = false
  deletion_protection = true
  enable_display      = false

  metadata = {
    enable-osconfig = "TRUE"
    SZ_ENV = var.environment
  }

  network_interface {
    access_config {
      network_tier = "PREMIUM"
    }

    queue_count = 0
    stack_type  = "IPV4_ONLY"
    subnetwork  = "projects/${var.project_id}/regions/${var.region}/subnetworks/default"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  service_account {
    email = google_service_account.scheduler_zen.email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  shielded_instance_config {
    enable_integrity_monitoring = false
    enable_secure_boot          = false
    enable_vtpm                 = false
  }
}