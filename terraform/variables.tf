variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "eng-ai-dev"
}

variable "environment" {
  description = "The environment (e.g., dev, prod)"
  type        = string
  default     = "dev"
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