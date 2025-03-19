terraform {
  required_providers {
    google = {
      source  = "hashicorp/google-beta"
    }
  }
}

provider "google" {
  project = var.project_id
}

terraform {
  backend "gcs" {
    bucket = "lum-terraform-state"
    prefix = "scheduler-zen"
  }
}
