variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "environment" {
  description = "The environment (e.g., dev, prod)"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "resources_project_id" {
  description = "The GCP project ID for resources"
  type        = string
  default     = "neat-airport-407301"
}

variable "zen_db_password" {
  description = "The password for the Cloud SQL database"
  type        = string
}