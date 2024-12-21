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

locals {
    zen_db_password = trimsuffix(file("${path.module}/../.secrets/zen-db.password"), "\n")
}